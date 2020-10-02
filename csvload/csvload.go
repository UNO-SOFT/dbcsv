// Copyright 2020 Tamás Gulácsi.
//
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
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

	"github.com/UNO-SOFT/dbcsv"

	"github.com/peterbourgon/ff/v3/ffcli"
	"golang.org/x/sync/errgroup"

	"github.com/godror/godror"
)

func main() {
	if err := Main(); err != nil {
		log.Fatal(err)
	}
}

var dateFormat = "2006-01-02 15:04:05"
var xlsEpoch = time.Date(1899, 12, 30, 0, 0, 0, 0, time.Local)

const chunkSize = 1024

type config struct {
	dbcsv.Config
	ForceString, JustPrint, Truncate bool
	Tablespace, Copy                 string
	Concurrency                      int
	WriteHeapProf                    func()
}

func Main() error {
	encName := os.Getenv("LANG")
	if i := strings.IndexByte(encName, '.'); i >= 0 {
		encName = encName[i+1:]
	} else if encName == "" {
		encName = "UTF-8"
	}

	var cfg config
	fs := flag.NewFlagSet("load", flag.ContinueOnError)
	flagConnect := fs.String("connect", os.Getenv("DB_ID"), "database to connect to")
	fs.BoolVar(&cfg.Truncate, "truncate", false, "truncate table")
	fs.StringVar(&cfg.Tablespace, "tablespace", "DATA", "tablespace to create table in")
	flagFields := fs.String("fields", "", "target fields, comma separated names")
	fs.BoolVar(&cfg.ForceString, "force-string", false, "force all columns to be VARCHAR2")
	fs.BoolVar(&cfg.JustPrint, "just-print", false, "just print the INSERTs")
	fs.StringVar(&cfg.Copy, "copy", "", "copy this table's structure")
	if *flagConnect == "" {
		*flagConnect = os.Getenv("BRUNO_ID")
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
			if err := cfg.Open(args[0]); err != nil {
				return err
			}
			defer cfg.Close()
			m, err := cfg.ReadSheets(ctx)
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
	fs.StringVar(&cfg.Delim, "delim", ";", "CSV separator")
	fs.IntVar(&cfg.Concurrency, "concurrency", 4, "concurrency")
	fs.StringVar(&dateFormat, "date", dateFormat, "date format, in Go notation")
	fs.IntVar(&cfg.Skip, "skip", 0, "skip rows")
	fs.IntVar(&cfg.Sheet, "sheet", 0, "sheet of spreadsheet")
	fs.StringVar(&cfg.ColumnsString, "columns", "", "columns, comma separated indexes")
	flagMemProf := fs.String("memprofile", "", "file to output memory profile to")
	flagCPUProf := fs.String("cpuprofile", "", "file to output CPU profile to")
	app := ffcli.Command{Name: "csvload", FlagSet: fs, ShortUsage: "load from csv/xls/ods into database table",
		Exec:        loadCmd.Exec,
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
			log.Fatal("could not create CPU profile: ", err)
		}
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatal("could not start CPU profile: ", err)
		}
		defer pprof.StopCPUProfile()
	}

	if *flagMemProf != "" {
		f, err := os.Create(*flagMemProf)
		if err != nil {
			log.Fatal("could not create memory profile: ", err)
		}
		defer f.Close()
		cfg.WriteHeapProf = func() {
			log.Println("writeHeapProf")
			f.Seek(0, 0)
			runtime.GC() // get up-to-date statistics
			if err := pprof.WriteHeapProfile(f); err != nil {
				log.Fatal("could not write memory profile: ", err)
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
	tblFullInsert := strings.HasPrefix(tbl, "INSERT INTO ")

	var err error
	if cfg.ForceString {
		err = cfg.OpenVolatile(src)
	} else {
		err = cfg.Open(src)
	}
	if err != nil {
		return err
	}
	defer cfg.Close()

	rows := make(chan dbcsv.Row)

	defCtx, defCancel := context.WithCancel(ctx)
	defer defCancel()
	grp, grpCtx := errgroup.WithContext(defCtx)
	grp.Go(func() error {
		defer close(rows)
		return cfg.ReadRows(grpCtx,
			func(_ string, row dbcsv.Row) error {
				select {
				case <-grpCtx.Done():
					return grpCtx.Err()
				case rows <- row:
				}
				return nil
			},
		)
	})

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
			for i, col := range cols {
				if i != 0 {
					buf.Write([]byte{',', ' '})
				}
				buf.WriteString(col.Name)
			}
			pattern = "  INTO " + tbl + " (" + buf.String() + ") VALUES ("
			colMap := make(map[string]Column, len(cols))
			for _, col := range cols {
				colMap[col.Name] = col
			}
			cols = cols[:0]
			for _, nm := range (<-rows).Values {
				cols = append(cols, colMap[strings.ToUpper(nm)])
			}

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
		log.Println(s)
		for x, i := strings.Count(s, ":"), 0; i < x; i++ {
			columns = append(columns, Column{Name: fmt.Sprintf("%d", i+1)})
		}
	} else {
		columns, err = CreateTable(defCtx, db, tbl, rows, cfg.Truncate, cfg.Tablespace, cfg.Copy, cfg.ForceString)
		if err != nil {
			return err
		}
		columns = filterCols(columns, fields)
		var buf strings.Builder
		fmt.Fprintf(&buf, `INSERT INTO %s (`, tbl)
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
	log.Println(qry)
	defCancel()
	if err = grp.Wait(); err != context.Canceled {
		return err
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
				return txErr
			}
			defer tx.Rollback()
			stmt, prepErr := tx.Prepare(qry)
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
					return err
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
							log.Printf("%d. more elements in the row (%d) then columns (%d)!", rs.Start+int64(k), len(row), len(cols))
						}
						row = row[:len(cols)]
					}
					for j, v := range row {
						cols[j][k] = v
					}
				}

				for i, col := range cols {
					if rowsI[i], err = columns[i].FromString(col); err != nil {
						log.Printf("%d. col: %+v", i, err)
						for k, row := range chunk {
							if _, err = columns[i].FromString(col[k : k+1]); err != nil {
								log.Printf("%d.%q %q: %q", rs.Start+int64(k), columns[i].Name, col[k:k+1], row)
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
				err = fmt.Errorf("%s: %w", qry, err)
				log.Println(err)

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
							log.Printf("%d[%q]=%d", j, columns[i].Name, r.Len())
							rowsI2[i] = ""
							continue
						}
						R2.Index(i).Set(r.Index(j))
					}
					if _, err = stmt.Exec(rowsI2...); err != nil {
						err = fmt.Errorf("%s, %q: %w", qry, rowsI2, err)
						log.Println(err)
						return err
					}
				}

				return err
			}
			return tx.Commit()
		})
	}

	var n int64

	var headerSeen bool
	chunk := (*(chunkPool.Get().(*[][]string)))[:0]
	if err = cfg.ReadRows(grpCtx,
		func(fn string, row dbcsv.Row) error {
			var err error
			if err = grpCtx.Err(); err != nil {
				log.Printf("Grp: %+v", err)
				chunk = chunk[:0]
				return err
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
			chunk = append(chunk, row.Values)
			if len(chunk) < chunkSize {
				return nil
			}

			select {
			case rowsCh <- rowsType{Rows: chunk, Start: n}:
				n += int64(len(chunk))
			case <-grpCtx.Done():
				log.Println("CTX:", grpCtx.Err())
				return grpCtx.Err()
			}

			chunk = (*chunkPool.Get().(*[][]string))[:0]
			return nil
		},
	); err != nil {
		return err
	}

	if len(chunk) != 0 {
		rowsCh <- rowsType{Rows: chunk, Start: n}
		n += int64(len(chunk))
	}
	close(rowsCh)

	if err = grp.Wait(); err != nil {
		log.Printf("ERROR: %+v", err)
	}
	dur := time.Since(start)
	log.Printf("Read %d, inserted %d rows from %q to %q in %s.", n, inserted, src, tbl, dur)
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
	log.Printf("tableSplitOwner(%q)", tbl)
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
		qry = `TRUNCATE TABLE ` + ownerDot + tbl
		if _, err := db.ExecContext(ctx, qry); err != nil {
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
		log.Printf("row: %v", row.Values)
		cols = make([]Column, len(row.Values))
		for i, v := range row.Values {
			v = strings.Map(func(r rune) rune {
				r = unicode.ToLower(r)
				switch r {
				case 'á':
					return 'a'
				case 'é':
					return 'e'
				case 'í':
					return 'i'
				case 'ö', 'ő', 'ó':
					return 'o'
				case 'ü', 'ű', 'ú':
					return 'u'
				case '_':
					return '_'
				default:
					if 'a' <= r && r <= 'z' || '0' <= r && r <= '9' {
						return r
					}
					return '_'
				}
			},
				v)
			if len(v) > 30 {
				v = fmt.Sprintf("%s_%02d", v[:27], i)
			}
			cols[i].Name = v
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
			length := c.Length
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
		log.Println(qry)
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
	Length           int
	Name             string
	Type             Type
	DataType         string
	Precision, Scale int
	Nullable         bool
}
type Type uint8

const (
	Unknown = Type(0)
	String  = Type(1)
	Int     = Type(2)
	Float   = Type(3)
	Date    = Type(4)
)

func (t Type) String() string {
	switch t {
	case Int, Float:
		return "NUMBER"
	case Date:
		return "DATE"
	default:
		return "VARCHAR2"
	}
}

func (c Column) FromString(ss []string) (interface{}, error) {
	if c.DataType == "DATE" || c.Type == Date {
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
			t, err := time.ParseInLocation(dateFormat[:len(s)], s, time.Local)
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
		switch c.DataType {
		case "DATE":
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
	if len(fields) == 0 {
		return cols
	}
	m := make(map[string]int, len(cols))
	for i, c := range cols {
		m[c.Name] = i
	}
	columns := make([]Column, 0, len(fields))
	for _, f := range fields {
		if i, ok := m[strings.ToUpper(f)]; ok {
			columns = append(columns, cols[i])
		}
	}
	return columns
}

// vim: set fileencoding=utf-8 noet:
