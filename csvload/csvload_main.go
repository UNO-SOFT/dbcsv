// Copyright 2021, 2023 Tamás Gulácsi.
//
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/base32"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"hash/fnv"
	"io"
	"os"
	"reflect"
	"runtime"
	"runtime/pprof"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode"

	"github.com/peterbourgon/ff/v3/ffcli"
	"golang.org/x/sync/errgroup"

	"github.com/godror/godror"

	"github.com/UNO-SOFT/dbcsv"

	"github.com/UNO-SOFT/zlog/v2"
)

var (
	verbose zlog.VerboseVar
	logger  = zlog.NewLogger(zlog.MaybeConsoleHandler(&verbose, os.Stderr)).SLog()
)

func main() {
	if err := Main(); err != nil {
		logger.Error("Main", "error", err)
		os.Exit(1)
	}
}

var (
	dateFormat = "2006-01-02T15:04:05"
	xlsEpoch   = time.Date(1899, 12, 30, 0, 0, 0, 0, time.Local)

	ErrTooManyFields = errors.New("too many fields")
)

const defaultChunkSize = 1024

type config struct {
	WriteHeapProf    func()
	Tablespace, Copy string
	*dbcsv.Config
	Concurrency, ChunkSize           int
	ForceString, JustPrint, Truncate bool
	LobSource                        bool
}

func Main() error {
	encName := os.Getenv("LANG")
	if i := strings.IndexByte(encName, '.'); i >= 0 {
		encName = encName[i+1:]
	} else if encName == "" {
		encName = "UTF-8"
	}

	cfg := config{Config: new(dbcsv.Config)}
	fs := flag.NewFlagSet("load", flag.ContinueOnError)
	flagConnect := fs.String("connect", os.Getenv("DB_ID"), "database to connect to")
	fs.BoolVar(&cfg.Truncate, "truncate", false, "truncate table")
	fs.StringVar(&cfg.Tablespace, "tablespace", "DATA", "tablespace to create table in")
	flagFields := fs.String("fields", "", "target fields, comma separated names")
	fs.BoolVar(&cfg.ForceString, "force-string", false, "force all columns to be VARCHAR2")
	fs.BoolVar(&cfg.JustPrint, "just-print", false, "just print the INSERTs")
	fs.StringVar(&cfg.Copy, "copy", "", "copy this table's structure")
	fs.IntVar(&cfg.ChunkSize, "chunk-size", defaultChunkSize, "chunk size - number of rows inserted at once")
	fs.Var(&verbose, "v", "verbose logging")
	fs.BoolVar(&cfg.LobSource, "lob", false, "source is not a filename but a query that returns a LOB")
	if *flagConnect == "" {
		if *flagConnect = os.Getenv("BRUNO_OWNER_ID"); *flagConnect == "" {
			*flagConnect = os.Getenv("BRUNO_ID")
		}
	}
	loadCmd := ffcli.Command{Name: "load", FlagSet: fs,
		Exec: func(ctx context.Context, args []string) error {
			if len(args) != 2 {
				return errors.New("need two args: the table and the source")
			}
			P, err := godror.ParseConnString(*flagConnect)
			if err != nil {
				return fmt.Errorf("%q: %w", *flagConnect, err)
			}
			P.StandaloneConnection = false
			P.SetSessionParamOnInit("NLS_NUMERIC_CHARACTERS", ". ")
			connector := godror.NewConnector(P)
			db := sql.OpenDB(connector)
			defer db.Close()

			db.SetMaxIdleConns(0)
			fields := strings.FieldsFunc(*flagFields, func(r rune) bool { return r == ',' || r == ';' || r == ' ' })

			return cfg.load(ctx, db, args[0], args[1], fields)
		},
	}

	sheetCmd := ffcli.Command{Name: "sheet",
		Exec: func(ctx context.Context, args []string) error {
			if err := cfg.Config.Open(args[0]); err != nil {
				return err
			}
			defer cfg.Close()
			m, err := cfg.Config.ReadSheets(ctx)
			if err != nil {
				return err
			}
			keys := make([]int, 0, len(m))
			for k := range m {
				keys = append(keys, k)
			}
			sort.Ints(keys)
			for _, k := range keys {
				fmt.Printf("%d\t%s\n", k, m[k])
			}
			return nil
		},
	}

	fs = flag.NewFlagSet("csvload", flag.ContinueOnError)
	fs.StringVar(&cfg.Charset, "charset", encName, "input charset")
	fs.StringVar(&cfg.Delim, "delim", "", "CSV separator")
	fs.IntVar(&cfg.Concurrency, "concurrency", 4, "concurrency")
	fs.StringVar(&dateFormat, "date", dateFormat, "date format, in Go notation")
	fs.IntVar(&cfg.Skip, "skip", 0, "skip rows")
	fs.IntVar(&cfg.Sheet, "sheet", 0, "sheet of spreadsheet")
	fs.StringVar(&cfg.ColumnsString, "columns", "", "columns, comma separated indexes")
	flagMemProf := fs.String("memprofile", "", "file to output memory profile to")
	flagCPUProf := fs.String("cpuprofile", "", "file to output CPU profile to")
	app := ffcli.Command{Name: "csvload", FlagSet: fs, ShortUsage: "load from csv/xls/ods into database table",
		Exec:        func(ctx context.Context, args []string) error { return loadCmd.Exec(ctx, args) },
		Subcommands: []*ffcli.Command{&loadCmd, &sheetCmd},
	}

	args := os.Args[1:]
	if err := app.Parse(args); err != nil {
		if len(args) == 0 {
			return err
		}
		if err = app.Parse(append(append(make([]string, 0, 1+len(args)), "load"), args...)); err != nil {
			return err
		}
	}

	if *flagCPUProf != "" {
		f, err := os.Create(*flagCPUProf)
		if err != nil {
			return fmt.Errorf("create CPU profile: %w", err)
		}
		if err := pprof.StartCPUProfile(f); err != nil {
			return fmt.Errorf("start CPU profile: %w", err)
		}
		defer pprof.StopCPUProfile()
	}

	if *flagMemProf != "" {
		f, err := os.Create(*flagMemProf)
		if err != nil {
			return fmt.Errorf("create memory profile: %w", err)
		}
		defer f.Close()
		cfg.WriteHeapProf = func() {
			logger.Debug("writeHeapProf")
			_, _ = f.Seek(0, 0)
			runtime.GC() // get up-to-date statistics
			if err := pprof.WriteHeapProfile(f); err != nil {
				logger.Error("write memory profile", "error", err)
			}
		}
	}

	ctx, cancel := dbcsv.Wrap(context.Background())
	defer cancel()
	return app.Run(ctx)
}

func (cfg config) load(ctx context.Context, db *sql.DB, tbl, src string, fields []string) error {
	if tbl == "" {
		panic("empty table")
	}
	tbl = strings.ToUpper(tbl)
	tblFullInsert := strings.HasPrefix(tbl, "INSERT /*+ APPEND */ INTO ")

	if err := cfg.Open(ctx, db, src); err != nil {
		return err
	}
	defer cfg.Close()

	rows := make(chan dbcsv.Row)
	var firstRow dbcsv.Row
	firstRowErr := make(chan error, 2)

	defCtx, defCancel := context.WithCancel(ctx)
	defer defCancel()
	grp, grpCtx := errgroup.WithContext(defCtx)
	grp.Go(func() error {
		defer close(rows)
		err := cfg.Config.ReadRows(grpCtx,
			func(ctx context.Context, _ string, row dbcsv.Row) error {
				if firstRow.Columns == nil {
					firstRow = row
					firstRowErr <- nil
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case rows <- row:
				}
				return nil
			},
		)
		firstRowErr <- err
		return err
	})
	select {
	case err := <-firstRowErr:
		if err != nil {
			logger.Error("first", "row", firstRow, "error", err)
			return err
		}
	case <-grpCtx.Done():
		return grpCtx.Err()
	}
	if len(fields) == 0 {
		fields = firstRow.Columns
	}
	logger.Debug("fields", "fields", fields)

	if cfg.JustPrint {
		fmt.Println("INSERT ALL")
		cols, err := getColumns(defCtx, db, tbl)
		if err != nil {
			return err
		}
		var buf strings.Builder
		var pattern string
		if tblFullInsert {
			i := strings.Index(tbl, "VALUES")
			j := strings.LastIndexByte(tbl[:i], ')')
			pattern = strings.TrimSpace(tbl[i:])
			for i := range cols {
				pattern = strings.Replace(pattern, fmt.Sprintf(":%d", i+1), "%s", 1)
			}
			pattern = strings.TrimSpace(strings.TrimPrefix(tbl[:j+1], "INSERT")) + pattern + "\n"
		} else {
			cols = filterCols(cols, fields)
			if len(cols) == 0 {
				for _, nm := range firstRow.Columns {
					cols = append(cols, Column{Name: nm})
				}
			} else {
				colMap := make(map[string]Column, len(cols))
				for _, col := range cols {
					colMap[col.Name] = col
				}
				cols = cols[:0]
				for _, nm := range firstRow.Columns {
					cols = append(cols, colMap[strings.ToUpper(nm)])
				}
			}
			for i, col := range cols {
				if i != 0 {
					buf.Write([]byte{',', ' '})
				}
				buf.WriteString(col.Name)
			}
			pattern = "  INTO " + tbl + " (" + buf.String() + ") VALUES ("

			buf.Reset()
			for j := range cols {
				if j != 0 {
					buf.WriteString(", ")
				}
				buf.WriteString("%s")
			}
			pattern += buf.String() + ")\n"
		}

		dRepl := strings.NewReplacer(".", "", "-", "")
		for row := range rows {
			allEmpty := true
			for i, s := range row.Values {
				row.Values[i] = s
				allEmpty = allEmpty && row.Values[i] == ""
			}
			if allEmpty {
				continue
			}

			vals := make([]interface{}, len(cols))
			for j, s := range row.Values {
				buf.Reset()
				col := cols[j]
				if col.Type != Date {
					if err = quote(&buf, s); err != nil {
						return err
					}
				} else {
					buf.WriteString("TO_DATE('")
					d := dRepl.Replace(s)
					if len(d) == 6 {
						d = "20" + d
					} else if len(d) < 8 {
						if i, err := strconv.Atoi(d); err == nil {
							d = xlsEpoch.AddDate(0, 0, i).Format("20060102")
						}
					}
					buf.WriteString(d)
					buf.WriteString("','YYYYMMDD')")
				}
				vals[j] = buf.String()
			}
			fmt.Printf(pattern, vals...)
		}
		fmt.Println("SELECT 1 FROM DUAL;")
		return nil
	}

	var columns []Column
	var qry string
	if tblFullInsert {
		qry = tbl
		s := qry[strings.Index(qry, "VALUES")+6:]
		s = s[strings.IndexByte(s, '(')+1 : strings.LastIndexByte(s, ')')]
		logger.Debug("tblFullInsert", "qry", s)
		for x, i := strings.Count(s, ":"), 0; i < x; i++ {
			columns = append(columns, Column{Name: fmt.Sprintf("%d", i+1)})
		}
	} else {
		var err error
		ctRows := make(chan dbcsv.Row, 1)
		ctRows <- firstRow
		go func() {
			defer close(ctRows)
			for row := range rows {
				ctRows <- row
			}
		}()
		columns, err = CreateTable(defCtx, db, tbl, ctRows, cfg.Truncate, cfg.Tablespace, cfg.Copy, cfg.ForceString)
		if err != nil {
			logger.Error("create", "table", tbl, "error", err)
			return err
		}
		columns = filterCols(columns, fields)
		var buf strings.Builder
		fmt.Fprintf(&buf, `INSERT /*+ APPEND */ INTO %s (`, tbl)
		for i, c := range columns {
			if i != 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(c.Name)
		}
		buf.WriteString(") VALUES (")
		for i := range columns {
			if i != 0 {
				buf.WriteString(", ")
			}
			fmt.Fprintf(&buf, ":%d", i+1)
		}
		buf.WriteString(")")
		qry = buf.String()
	}
	defCancel()
	if err := grp.Wait(); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	logger.Info("synthetized", "qry", qry)

	var hasLOB bool
	chunkSize := cfg.ChunkSize
	if chunkSize <= 0 {
		chunkSize = defaultChunkSize
	}
	for _, c := range columns {
		if hasLOB = c.DataType == tCLOB || c.DataType == tBLOB; hasLOB {
			chunkSize = 1
			break
		}
	}

	start := time.Now()

	type rowsType struct {
		Rows  [][]string
		Start int64
	}
	rowsCh := make(chan rowsType, cfg.Concurrency)
	chunkPool := sync.Pool{New: func() interface{} { z := make([][]string, 0, chunkSize); return &z }}

	grp, grpCtx = errgroup.WithContext(ctx)

	var inserted int64
	for i := 0; i < cfg.Concurrency; i++ {
		grp.Go(func() error {
			tx, txErr := db.BeginTx(grpCtx, nil)
			if txErr != nil {
				return fmt.Errorf("BEGIN: %w", txErr)
			}
			defer tx.Rollback()
			stmt, prepErr := tx.PrepareContext(grpCtx, qry)
			if prepErr != nil {
				return fmt.Errorf("%s: %w", qry, prepErr)
			}
			nCols := len(columns)
			cols := make([][]string, nCols)
			rowsI := make([]interface{}, nCols)

			for rs := range rowsCh {
				chunk := rs.Rows
				var err error
				if err = grpCtx.Err(); err != nil {
					logger.Error("GrpRows", "error", err)
					return nil
				}
				if len(chunk) == 0 {
					continue
				}
				nRows := len(chunk)
				for j := range cols {
					if cap(cols[j]) < nRows {
						cols[j] = make([]string, nRows)
					} else {
						cols[j] = cols[j][:nRows]
						for i := range cols[j] {
							cols[j][i] = ""
						}
					}
				}
				for k, row := range chunk {
					if len(row) > len(cols) {
						if row[len(row)-1] != "" {
							return fmt.Errorf("%d. more elements in the row (%d) then columns (%d): %w", rs.Start+int64(k), len(row), len(cols), ErrTooManyFields)
						}
						row = row[:len(cols)]
					}
					for j, v := range row {
						cols[j][k] = v
					}
				}

				for i, col := range cols {
					if rowsI[i], err = columns[i].FromString(col); err != nil {
						logger.Error("FromString", "col", i, "error", err)
						for k, row := range chunk {
							if _, err = columns[i].FromString(col[k : k+1]); err != nil {
								logger.Error("FromString", "start", rs.Start+int64(k), "column", columns[i].Name, "value", col[k:k+1], "row", row, "error", err)
								break
							}
						}

						if err != nil {
							return fmt.Errorf("%s: %w", columns[i].Name, err)
						}
						return nil
					}
				}

				_, err = stmt.Exec(rowsI...)
				{
					z := chunk[:0]
					chunkPool.Put(&z)
				}
				if err == nil {
					atomic.AddInt64(&inserted, int64(len(chunk)))
					continue
				}
				if chunkSize == 1 {
					logger.Error("exec", "qry", qry, "rows", rowsI, "error", err)
					return fmt.Errorf("%s [%v]: %w", qry, rowsI, err)
				}
				logger.Error("exec", "qry", qry, "error", err)
				err = fmt.Errorf("%s: %w", qry, err)

				rowsR := make([]reflect.Value, len(rowsI))
				rowsI2 := make([]interface{}, len(rowsI))
				for j, I := range rowsI {
					rowsR[j] = reflect.ValueOf(I)
					rowsI2[j] = ""
				}
				R2 := reflect.ValueOf(rowsI2)
				for j := range cols[0] { // rows
					for i, r := range rowsR { // cols
						if r.Len() <= j {
							logger.Info("debug", "row", j, "column", columns[i].Name, "len", r.Len())
							rowsI2[i] = ""
							continue
						}
						R2.Index(i).Set(r.Index(j))
					}
					if _, err = stmt.Exec(rowsI2...); err != nil {
						logger.Error("exec", "rows", rowsI2, "error", err)
						return fmt.Errorf("%s, %q: %w", qry, rowsI2, err)
					}
				}

				return err
			}
			if err := tx.Commit(); err != nil {
				return fmt.Errorf("COMMIT: %w", err)
			}
			return nil
		})
	}

	var n int64

	if err := grpCtx.Err(); err != nil {
		panic(err)
	}

	var headerSeen bool
	chunk := (*(chunkPool.Get().(*[][]string)))[:0]
	if err := cfg.Config.ReadRows(grpCtx,
		func(ctx context.Context, fn string, row dbcsv.Row) error {
			var err error
			if err = ctx.Err(); err != nil {
				logger.Error("GrpRead", "error", err)
				return nil
			}

			if !headerSeen {
				headerSeen = true
				return nil
			} else if cfg.WriteHeapProf != nil && n%10000 == 0 {
				cfg.WriteHeapProf()
			}
			allEmpty := true
			for i, s := range row.Values {
				row.Values[i] = s
				allEmpty = allEmpty && row.Values[i] == ""
			}
			if allEmpty {
				return nil
			}
			// Reader may reuse the Values slice
			chunk = append(chunk, append(make([]string, 0, len(row.Values)), row.Values...))
			if len(chunk) < chunkSize {
				return nil
			}

			select {
			case rowsCh <- rowsType{Rows: chunk, Start: n}:
				n += int64(len(chunk))
			case <-ctx.Done():
				logger.Error("CTX", "error", ctx.Err())
				return nil
			}

			chunk = (*chunkPool.Get().(*[][]string))[:0]
			return nil
		},
	); err != nil {
		logger.Error("ReadRows", "error", err)
		return err
	}

	if len(chunk) != 0 {
		rowsCh <- rowsType{Rows: chunk, Start: n}
		n += int64(len(chunk))
	}
	close(rowsCh)

	err := grp.Wait()
	if err != nil {
		logger.Error("ERROR", "error", err)
	}
	dur := time.Since(start)
	logger.Info("timing", "read", n, "inserted", inserted, "src", src, "tbl", tbl, "dur", dur.String())
	return err
}

func typeOf(s string, forceString bool) Type {
	if forceString {
		return String
	}

	if s == "" {
		return Unknown
	}
	var hasNonDigit bool
	var dotCount int
	var length int
	_ = strings.Map(func(r rune) rune {
		length++
		if r == '.' {
			dotCount++
		} else if !hasNonDigit {
			hasNonDigit = !('0' <= r && r <= '9')
		}
		return -1
	},
		s)

	if !hasNonDigit && s[0] != '0' {
		if dotCount == 1 {
			return Float
		}
		if dotCount == 0 {
			return Int
		}
	}
	if 10 <= len(s) && len(s) <= len(dateFormat) {
		if _, err := time.Parse(dateFormat[:len(s)], s); err == nil {
			return Date
		}
	}
	return String
}
func tableSplitOwner(tbl string) (string, string) {
	if tbl == "" {
		panic("empty tabl name")
	}
	logger.Debug("tblSplitOwner", "tbl", tbl)
	if i := strings.IndexByte(tbl, '.'); i >= 0 {
		return tbl[:i], tbl[i+1:]
	}
	return "", tbl
}

func CreateTable(ctx context.Context, db *sql.DB, tbl string, rows <-chan dbcsv.Row, truncate bool, tablespace, copyTable string, forceString bool) ([]Column, error) {
	owner, tbl := tableSplitOwner(strings.ToUpper(tbl))
	var ownerDot string
	if owner != "" {
		ownerDot = owner + "."
	}
	qry := "SELECT COUNT(0) FROM all_tables WHERE UPPER(table_name) = :1 AND owner = NVL(:2, SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA'))"
	var n int64
	var cols []Column
	if err := db.QueryRowContext(ctx, qry, tbl, owner).Scan(&n); err != nil {
		return cols, fmt.Errorf("%s: %w", qry, err)
	}
	if n > 0 && truncate {
		// nosemgrep: go.lang.security.audit.database.string-formatted-query.string-formatted-query
		qry = `TRUNCATE TABLE ` + ownerDot + tbl
		if _, err := db.ExecContext(ctx, qry); err != nil {
			// nosemgrep: go.lang.security.audit.database.string-formatted-query.string-formatted-query
			if _, delErr := db.ExecContext(ctx, "DELETE FROM "+ownerDot+tbl); delErr != nil {
				return cols, fmt.Errorf("%s: %w", qry, err)
			}
		}
	}

	if n == 0 && copyTable != "" {
		var tblsp string
		if tablespace != "" {
			tblsp = "TABLESPACE " + tablespace
		}
		qry := fmt.Sprintf("CREATE TABLE %s%s %s AS SELECT * FROM %s WHERE 1=0", ownerDot, tbl, tblsp, copyTable)
		if _, err := db.ExecContext(ctx, qry); err != nil {
			return cols, fmt.Errorf("%s: %w", qry, err)
		}
	} else if n == 0 && copyTable == "" {
		row := <-rows
	Loop:
		for len(row.Columns) == 0 {
			var ok bool
			select {
			case row, ok = <-rows:
				if !ok {
					break Loop
				}
			case <-ctx.Done():
				return cols, ctx.Err()
			}
		}
		if len(row.Columns) == 0 {
			panic(fmt.Sprintf("empty row: %#v", row))
		}
		cols = make([]Column, len(row.Columns))
		for i, v := range row.Columns {
			cols[i].Name = mkColName(v)
		}
		if forceString {
			for i := range cols {
				cols[i].Type = String
			}
		}
		for row := range rows {
			for i, v := range row.Values {
				if len(v) > cols[i].Length {
					cols[i].Length = len(v)
				}
				if cols[i].Type == String {
					continue
				}
				typ := typeOf(v, forceString)
				if cols[i].Type == Unknown {
					cols[i].Type = typ
				} else if typ != cols[i].Type {
					cols[i].Type = String
				}
			}
		}
		var buf bytes.Buffer
		buf.WriteString(`CREATE TABLE "` + ownerDot + tbl + `" (`)
		for i, c := range cols {
			if i != 0 {
				buf.WriteString(",\n")
			}
			if c.Type == Date {
				fmt.Fprintf(&buf, "  %s DATE", c.Name)
				continue
			}
			length := c.Length * 2
			if length == 0 {
				length = 1
			}
			fmt.Fprintf(&buf, "  %s %s(%d)", c.Name, c.Type.String(), length)
		}
		buf.WriteString("\n)")
		if tablespace != "" {
			buf.WriteString(" TABLESPACE ")
			buf.WriteString(tablespace)
		}
		qry = buf.String()
		logger.Debug("exec", "qry", qry)
		if _, err := db.Exec(qry); err != nil {
			return cols, fmt.Errorf("%s: %w", qry, err)
		}
		cols = cols[:0]
	}

	qry = `SELECT column_name, data_type, NVL(data_length, 0), NVL(data_precision, 0), NVL(data_scale, 0), nullable
  FROM all_tab_cols WHERE table_name = :1 AND owner = NVL(:2, SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA'))
  ORDER BY nullable, column_id`
	tRows, err := db.QueryContext(ctx, qry, tbl, owner)
	if err != nil {
		return cols, fmt.Errorf("%s: %w", qry, err)
	}
	defer tRows.Close()
	for tRows.Next() {
		var c Column
		var nullable string
		if err = tRows.Scan(&c.Name, &c.DataType, &c.Length, &c.Precision, &c.Scale, &nullable); err != nil {
			return cols, err
		}
		c.Nullable = nullable != "N"
		cols = append(cols, c)
	}
	return cols, nil
}

type Column struct {
	Name             string
	DataType         string
	Length           int
	Precision, Scale int
	Type             Type
	Nullable         bool
}
type Type uint8

const (
	Unknown = Type(0)
	String  = Type(1)
	Int     = Type(2)
	Float   = Type(3)
	Date    = Type(4)

	tBLOB     = "BLOB"
	tCLOB     = "CLOB"
	tDATE     = "DATE"
	tVARCHAR2 = "VARCHAR2"
	tNUMBER   = "NUMBER"
)

func (t Type) String() string {
	switch t {
	case Int, Float:
		return tNUMBER
	case Date:
		return tDATE
	default:
		return tVARCHAR2
	}
}

func (c Column) FromString(ss []string) (interface{}, error) {
	if c.DataType == "DATE" || strings.HasPrefix(c.DataType, "TIMESTAMP") || c.Type == Date {
		res := make([]sql.NullTime, len(ss))
		for i, s := range ss {
			if s == "" {
				continue
			}
			if len(s) < 8 {
				if j, err := strconv.Atoi(s); err == nil {
					res[i] = sql.NullTime{Valid: true, Time: xlsEpoch.AddDate(0, 0, j)}
					continue
				}
			}
			df := dateFormat
			if len(s) < len(df) {
				df = df[:len(s)]
			}
			t, err := time.ParseInLocation(df, s, time.Local)
			if err != nil {
				return res, fmt.Errorf("%d. %q: %w", i, s, err)
			}
			res[i] = sql.NullTime{Valid: true, Time: t}
		}
		return res, nil
	}

	if strings.HasPrefix(c.DataType, "VARCHAR2") {
		for i, s := range ss {
			if len(s) > c.Length*4 { // AL32UTF8 or not?
				ss[i] = s[:c.Length]
				return ss, fmt.Errorf("%d. %q is longer (%d) then allowed (%d) for column %v", i, s, len(s), c.Length, c)
			}
		}
		return ss, nil
	}
	if c.Type == Int {
		for i, s := range ss {
			e := strings.Map(func(r rune) rune {
				if !('0' <= r && r <= '9' || r == '-') {
					return r
				}
				return -1
			}, s)
			if e != "" {
				ss[i] = ""
				return ss, fmt.Errorf("%d. %q is not integer (%q)", i, s, e)
			}
		}
		return ss, nil
	}
	if c.Type == Float {
		for i, s := range ss {
			e := strings.Map(func(r rune) rune {
				if !('0' <= r && r <= '9' || r == '-' || r == '.') {
					return r
				}
				return -1
			}, s)
			if e != "" {
				ss[i] = ""
				return ss, fmt.Errorf("%d. %q is not float (%q)", i, s, e)
			}
		}
		return ss, nil
	}

	if c.DataType == tCLOB || c.DataType == tBLOB {
		isClob := c.DataType == tCLOB
		res := make([]godror.Lob, len(ss))
		for i, s := range ss {
			if !isClob {
				if b, err := hex.DecodeString(s); err == nil {
					res[i] = godror.Lob{IsClob: false, Reader: bytes.NewReader(b)}
					continue
				}
			}
			res[i] = godror.Lob{IsClob: isClob, Reader: strings.NewReader(s)}
		}
		return res, nil
	}

	return ss, nil
}

func getColumns(ctx context.Context, db *sql.DB, tbl string) ([]Column, error) {
	owner, tbl := tableSplitOwner(strings.ToUpper(tbl))
	// TODO(tgulacsi): this is Oracle-specific!
	const qry = `SELECT column_name, data_type, data_length, data_precision, data_scale, nullable 
		FROM all_tab_cols 
		WHERE table_name = UPPER(:1) AND owner = NVL(:2, SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA')) 
		ORDER BY nullable, column_id`
	rows, err := db.QueryContext(ctx, qry, tbl, owner)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", qry, err)
	}
	defer rows.Close()
	var cols []Column
	for rows.Next() {
		var c Column
		var prec, scale sql.NullInt64
		var nullable string
		if err = rows.Scan(&c.Name, &c.DataType, &c.Length, &prec, &scale, &nullable); err != nil {
			return nil, err
		}
		c.Nullable = nullable == "Y"
		switch x, _ := strings.CutPrefix(c.DataType, "("); x {
		case "DATE", "TIMESTAMP":
			c.Type = Date
			c.Length = 8
		case "NUMBER":
			c.Precision, c.Scale = int(prec.Int64), int(scale.Int64)
			if c.Scale > 0 {
				c.Type = Float
				c.Length = c.Precision + 1
			} else {
				c.Type = Int
				c.Length = c.Precision
			}
		default:
			c.Type = String
		}
		cols = append(cols, c)
	}
	return cols, rows.Close()
}

var qRepl = strings.NewReplacer(
	"'", "''",
	"&", "'||CHR(38)||'",
)

func quote(w io.Writer, s string) error {
	if _, err := w.Write([]byte{'\''}); err != nil {
		return err
	}
	if _, err := io.WriteString(w, qRepl.Replace(s)); err != nil {
		return err
	}
	_, err := w.Write([]byte{'\''})
	return err
}

func filterCols(cols []Column, fields []string) []Column {
	if len(fields) == 0 || len(cols) == 0 {
		return cols
	}
	m := make(map[string]int, len(cols))
	for i, c := range cols {
		m[c.Name] = i
		// Try alternate name, except it would overwrite
		if strings.HasPrefix(c.Name, "F_") {
			if _, ok := m[c.Name[2:]]; !ok {
				m[c.Name[2:]] = i
			}
		}
	}
	columns := make([]Column, 0, len(fields))
	for _, f := range fields {
		if i, ok := m[strings.ToUpper(f)]; ok {
			columns = append(columns, cols[i])
		} else if i, ok = m[mkColName(f)]; ok {
			columns = append(columns, cols[i])
		} else {
			logger.Info("filter out", "field", f, "col", mkColName(f))
		}
	}
	return columns
}

func mkColName(v string) string {
	v = strings.Map(func(r rune) rune {
		r = unicode.ToUpper(r)
		switch r {
		case 'Á':
			return 'A'
		case 'É':
			return 'E'
		case 'Í':
			return 'I'
		case 'Ö', 'Ő', 'Ó':
			return 'O'
		case 'Ü', 'Ű', 'Ú':
			return 'U'
		case '_':
			return '_'
		default:
			if 'A' <= r && r <= 'Z' || '0' <= r && r <= '9' {
				return r
			}
			return '_'
		}
	},
		v)
	if v[0] == '_' {
		v = "X" + v
	}
	if len(v) <= 30 {
		return v
	}
	hsh := fnv.New32()
	hsh.Write([]byte(v))
	var a [4]byte
	return v[:30-7] + base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(hsh.Sum(a[:0]))
}

func (cfg config) Open(ctx context.Context, db *sql.DB, fn string) (err error) {
	if cfg.LobSource {
		fh, tempErr := os.CreateTemp("", "csvload-*.csv")
		if tempErr != nil {
			return err
		}
		os.Remove(fh.Name())
		defer func() {
			if err != nil {
				fh.Close()
			}
		}()
		qry := strings.TrimSpace(fn)
		var lob godror.Lob
		if len(qry) > len("SELECT") && (strings.EqualFold(qry[:len("SELECT")], "SELECT") || strings.EqualFold(qry[:len("WITH")], "WITH")) {
			rows, err := db.QueryContext(ctx, qry, godror.LobAsReader())
			if err != nil {
				return fmt.Errorf("query %s: %w", qry, err)
			}
			defer rows.Close()
			if !rows.Next() {
				return io.EOF
			}
			var lobI interface{}
			if err = rows.Scan(&lobI); err != nil {
				return fmt.Errorf("scan %s: %w", qry, err)
			}
			lob = *(lobI.(*godror.Lob))
		} else {
			if _, err = db.ExecContext(ctx, qry, sql.Out{Dest: &lob}); err != nil {
				return fmt.Errorf("exec %s: %w", qry, err)
			}
		}
		if _, err = io.Copy(fh, lob); err != nil {
			return err
		}
		if _, err = fh.Seek(0, 0); err != nil {
			return err
		}
		os.Stdin.Close()
		fn, os.Stdin = "", fh
	}
	return cfg.Config.Open(fn)
}

// vim: set fileencoding=utf-8 noet:
