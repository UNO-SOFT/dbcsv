// Copyright 2020, 2023 Tamás Gulácsi.
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

// Package main in csvdump represents a cursor->csv dumper
package main

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"flag"
	"fmt"
	"io"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"golang.org/x/sync/errgroup"
	"golang.org/x/text/encoding"

	"github.com/google/renameio/v2"
	"github.com/klauspost/compress/gzip"
	"github.com/klauspost/compress/zstd"

	"github.com/godror/godror"

	"github.com/UNO-SOFT/dbcsv"
	"github.com/UNO-SOFT/spreadsheet"
	"github.com/UNO-SOFT/spreadsheet/ods"
	"github.com/UNO-SOFT/spreadsheet/xlsx"

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

func Main() error {
	flagConnect := flag.String("connect", os.Getenv("DB_ID"), "user/passw@sid to connect to")
	flagDateFormat := flag.String("date", "2006-01-02T15:04:05", "date format, in Go notation")
	flagSep := flag.String("sep", ",", "separator")
	flagHeader := flag.Bool("header", true, "print header")
	flagEnc := flag.String("encoding", dbcsv.DefaultEncoding.Name, "encoding to use for output")
	flagOut := flag.String("o", "-", "output (defaults to stdout)")
	flagRaw := flag.Bool("raw", false, "not real csv, just dump the raw data")
	flagSort := flag.Bool("sort", false, "sort data")
	flagSheets := dbcsv.FlagStrings()
	flag.Var(flagSheets, "sheet", "each -sheet=name:SELECT will become a separate sheet on the output ods")
	flagParams := dbcsv.FlagStrings()
	flag.Var(flagParams, "param", "each -param=asdf will becoma separate parameter (:1, :2, ...)")
	flag.Var(&verbose, "v", "verbose logging")
	flagCompress := flag.String("compress", "", "compress output with gz/gzip or zst/zstd/zstandard")
	flagCall := flag.Bool("call", false, "the first argument is not the WHERE, but the PL/SQL block to be called, the followings are not the columns but the arguments")
	flagRemote := flag.Bool("remote", false, `the rows are XLSX commands in JSON {"c":"command_name", "a":[{"f":"float_value","s":"string_value", "i":"int_value"}]} format`)
	flagAQ := flag.Bool("aq", false, "get the remote commands from AQ/objectTypeName/correlation")
	flagTimeout := flag.Duration("timeout", 0, "timeout")

	flag.Usage = func() {
		fmt.Fprintln(flag.CommandLine.Output(), strings.Replace(`Usage of {{.prog}}:
	{{.prog}} [options] 'T_able' 'F_ield=1'

will execute a "SELECT * FROM T_able WHERE F_ield=1" and dump all the columns;

	{{.prog}} -call [options] 'DB_lista.csv' 'p_a=1' 'p_b=c'

will execute "BEGIN :1 := DB_lista.csv(p_a=>:2, p_b=>3); END" with p_a=1, p_b=c
and dump all the columns of the cursor returned by the function.

`, "{{.prog}}", os.Args[0], -1))
		flag.PrintDefaults()
	}
	if *flagConnect == "" {
		if *flagConnect = os.Getenv("BRUNO_OWNER_ID"); *flagConnect == "" {
			*flagConnect = os.Getenv("BRUNO_ID")
		}
	}
	flag.Parse()

	enc, err := dbcsv.EncFromName(*flagEnc)
	if err != nil {
		return err
	}
	dec := enc.Encoding.NewDecoder()
	args := flag.Args()
	if dec != nil {
		for i, a := range args {
			if args[i], err = dec.String(a); err != nil {
				return fmt.Errorf("%q: %w", a, err)
			}
		}
		for i, q := range flagSheets.Strings {
			if flagSheets.Strings[i], err = dec.String(q); err != nil {
				return fmt.Errorf("%q: %w", q, err)
			}
		}
	}

	dbcsv.DateFormat = *flagDateFormat
	dbcsv.DateEnd = `"` + strings.NewReplacer(
		"2006", "9999",
		"01", "12",
		"02", "31",
		"15", "23",
		"04", "59",
		"05", "59",
	).Replace(dbcsv.DateFormat) + `"`

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	if *flagTimeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, *flagTimeout)
		defer cancel()
	}
	ctx = zlog.NewSContext(ctx, logger)

	var queries []Query
	var params []interface{}
	db, err := sql.Open("godror", *flagConnect)
	if err != nil {
		return fmt.Errorf("%s: %w", *flagConnect, err)
	}
	defer db.Close()
	db.SetMaxOpenConns(2)
	db.SetMaxIdleConns(1)

	logger.Debug("flags", "sheets", flagSheets.Strings, "call", *flagCall, "args", args)
	if len(flagSheets.Strings) != 0 {
		queries = make([]Query, len(flagSheets.Strings))
		for i, q := range flagSheets.Strings {
			if j := strings.IndexByte(q, ':'); j > 0 {
				queries[i] = Query{Name: q[:j], Query: q[j+1:]}
			} else {
				queries[i] = Query{Query: q}
			}
		}
		if *flagAQ {
			Q := queries[0]
			Q.ParseQueue()
			queries[0] = Q
			for i, q := range queries[1:] {
				q.ParseQueue()
				if q.QueueName == "" {
					q.QueueName = Q.QueueName
				}
				if q.TypeName == "" && q.QueueName == Q.QueueName {
					q.TypeName = Q.TypeName
				}
				queries[i+1] = q
			}
		} else if *flagCall {
			Q := queries[0]
			Q.Query, params = splitParamArgs(Q.Query, args)
			queries[0] = Q
			for i, Q := range queries[1:] {
				queries[i+1].Query, _ = splitParamArgs(Q.Query, args)
			}
			logger.Info("call", "queries", queries, "params", params)
		}
	} else if *flagCall {
		var qry string
		qry, params = splitParamArgs(args[0], args[1:])
		logger.Debug("call", "qry", qry, "params", params)
		queries = append(queries, Query{Query: qry})
	} else if *flagAQ {
		Q := Query{Query: args[0]}
		Q.ParseQueue()
		queries = append(queries, Q)
	} else {
		params = make([]interface{}, len(flagParams.Strings))
		for i, p := range flagParams.Strings {
			params[i] = p
		}
		var (
			qry, where string
			columns    []string
		)
		if len(args) > 0 {
			qry = args[0]
		}
		if len(args) > 1 {
			where = args[1]
			if len(args) > 2 {
				columns = args[2:]
			}
		}
		qry = getQuery(qry, where, columns, dbcsv.DefaultEncoding)
		queries = append(queries, Query{Query: qry})
	}

	fh := interface {
		io.WriteCloser
		Name() string
	}(os.Stdout)
	defer fh.Close()
	var origFn string
	if !(*flagOut == "" || *flagOut == "-") {
		// nosemgrep: go.lang.correctness.permissions.file_permission.incorrect-default-permission
		_ = os.MkdirAll(filepath.Dir(*flagOut), 0750)
		pfh, err := renameio.NewPendingFile(*flagOut, renameio.WithPermissions(0640))
		if err != nil {
			return fmt.Errorf("%s: %w", *flagOut, err)
		}
		defer pfh.Cleanup()
		fh = pfh
		origFn = *flagOut
	}
	wfh := io.WriteCloser(fh)
	if *flagCompress != "" {
		switch (strings.TrimSpace(strings.ToLower(*flagCompress)) + "  ")[:2] {
		case "gz":
			wfh = gzip.NewWriter(fh)
		case "zs":
			var err error
			if wfh, err = zstd.NewWriter(fh); err != nil {
				return err
			}
		}
	}

	logger.Debug("writing", "file", fh.Name(), "encoding", enc)
	tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		log.Printf("[WARN] Read-Only transaction: %v", err)
		if tx, err = db.BeginTx(ctx, nil); err != nil {
			return fmt.Errorf("%s: %w", "beginTx", err)
		}
	}
	defer tx.Rollback()
	if logger.Enabled(ctx, slog.LevelDebug) {
		godror.SetLogger(logger.With("lib", "godror"))
		defer godror.SetLogger(zlog.Discard().SLog())
	}

	if len(flagSheets.Strings) == 0 &&
		!strings.HasSuffix(origFn, ".ods") &&
		!strings.HasSuffix(origFn, ".xlsx") {
		w := encoding.ReplaceUnsupported(enc.NewEncoder()).Writer(wfh)
		logger.Debug("encoding", "env", dbcsv.DefaultEncoding.Name)

		if queries[0].QueueName != "" {
			Q, err := queries[0].OpenQueue(ctx, db)
			if err != nil {
				return err
			}
			defer Q.Close()
			err = dumpRemoteCSVQueue(ctx, w, Q, *flagSep)
		} else {
			rows, columns, qErr := doQuery(ctx, tx, queries[0].Query, params, *flagCall, *flagSort)
			if qErr != nil {
				err = qErr
			} else {
				defer rows.Close()
				if *flagRemote {
					if len(columns) != 1 {
						return fmt.Errorf("-remote wants the queries to have only one column, this has %d", len(columns))
					}
					err = dumpRemoteCSV(ctx, w, rows, *flagSep)
				} else {
					err = dbcsv.DumpCSV(ctx, w, rows, columns, *flagHeader, *flagSep, *flagRaw)
				}
			}
		}
	} else {
		var w spreadsheet.Writer
		if strings.HasSuffix(origFn, ".xlsx") {
			if !*flagRemote {
				w = xlsx.NewWriter(wfh)
				defer w.Close()
			}
		} else {
			w, err = ods.NewWriter(wfh)
			if err != nil {
				return err
			}
			defer w.Close()
		}

		grp, grpCtx := errgroup.WithContext(ctx)
		for sheetNo := range queries {
			qry, name := queries[sheetNo].Query, queries[sheetNo].Name
			if name == "" {
				name = strconv.Itoa(sheetNo + 1)
			}
			if *flagAQ {
				Q, err := queries[sheetNo].OpenQueue(ctx, db)
				if err != nil {
					return err
				}
				defer Q.Close()

				err = executeCommands(ctx, wfh, queueNext(grpCtx, Q))
				Q.Close()
				if err != nil {
					break
				}
				continue
			}

			rows, columns, qErr := doQuery(grpCtx, tx, qry, params, *flagCall, *flagSort)
			if qErr != nil {
				err = qErr
				break
			}
			if *flagRemote {
				if len(columns) != 1 {
					return fmt.Errorf("-remote wants the queries to have only one column, %q has %d", name, len(columns))
				}
				if err = executeCommands(ctx, wfh, func() ([]byte, error) {
					if !rows.Next() {
						return nil, io.EOF
					}
					var s string
					err := rows.Scan(&s)
					return []byte(s), err
				}); err != nil {
					break
				}
				continue
			}
			header := make([]spreadsheet.Column, len(columns))
			if *flagHeader {
				for i, c := range columns {
					header[i].Name = c.Name
				}
			}
			sheet, sErr := w.NewSheet(name, header)
			if sErr != nil {
				rows.Close()
				err = sErr
				break
			}
			grp.Go(func() error {
				logger.Debug("DumpSheet", "name", name, "qry", qry)
				err := dbcsv.DumpSheet(grpCtx, sheet, rows, columns)
				rows.Close()
				if closeErr := sheet.Close(); closeErr != nil && err == nil {
					return closeErr
				}
				return err
			})
		}
		if err != nil {
			return err
		}
		err = grp.Wait()
		if w != nil {
			if closeErr := w.Close(); closeErr != nil && err == nil {
				err = closeErr
			}
		}
	}
	cancel()
	if err != nil {
		return err
	}
	if wfh != fh {
		if err = wfh.Close(); err != nil {
			return err
		}
	}
	if pfh, ok := fh.(interface{ CloseAtomicallyReplace() error }); ok {
		return pfh.CloseAtomicallyReplace()
	}
	return fh.Close()
}

func getQuery(table, where string, columns []string, enc encoding.Encoding) string {
	if (table == "" || table == "-") && where == "" && len(columns) == 0 {
		if enc == nil {
			enc = encoding.Nop
		}
		b, err := io.ReadAll(enc.NewDecoder().Reader(os.Stdin))
		if err != nil {
			panic(err)
		}
		return string(b)
	}
	table = strings.TrimSpace(table)
	if len(table) > 6 && strings.HasPrefix(strings.ToUpper(table), "SELECT ") {
		return table
	}
	cols := "*"
	if len(columns) > 0 {
		cols = strings.Join(columns, ", ")
	}
	if where == "" {
		return "SELECT " + cols + " FROM " + table //nolint:gas
	}
	return "SELECT " + cols + " FROM " + table + " WHERE " + where //nolint:gas
}

type queryer interface {
	QueryContext(context.Context, string, ...interface{}) (*sql.Rows, error)
}

type execer interface {
	ExecContext(context.Context, string, ...interface{}) (sql.Result, error)
}
type queryExecer interface {
	queryer
	execer
}

func doQuery(ctx context.Context, db queryExecer, qry string, params []interface{}, isCall, doSort bool) (*sql.Rows, []dbcsv.Column, error) {
	var rows *sql.Rows
	var err error
	const defaultBatchSize = 1024
	batchSize := defaultBatchSize
	if isCall {
		var dRows driver.Rows
		params = append(append(make([]interface{}, 0, 2+len(params)),
			sql.Out{Dest: &dRows}, godror.FetchRowCount(batchSize), godror.PrefetchCount(batchSize+1)),
			params...)
		if _, err = db.ExecContext(ctx, qry, params...); err == nil {
			rows, err = godror.WrapRows(ctx, db, dRows)
		} else {
			logger.Error("call", "qry", qry, "params", fmt.Sprintf("%#v", params), "error", err)
		}
	} else {
		origQry := qry
		if doSort && strings.HasPrefix(qry, "SELECT * FROM") {
			rows, err := db.QueryContext(ctx, qry+" FETCH FIRST ROW ONLY", params...)
			if err != nil {
				if rows, err = db.QueryContext(ctx, qry, params...); err != nil {
					return nil, nil, fmt.Errorf("%s: %w", qry, err)
				}
			}
			cols, err := rows.ColumnTypes()
			rows.Close()
			if err != nil {
				return nil, nil, fmt.Errorf("%s: %w", qry, err)
			}
			var bld strings.Builder
			for i, c := range cols {
				if strings.HasSuffix(c.DatabaseTypeName(), "LOB") {
					continue
				}
				if i == 0 {
					bld.WriteString(qry)
					bld.WriteString(" ORDER BY ")
				} else {
					bld.WriteByte(',')
				}
				fmt.Fprintf(&bld, "%d", i+1)
			}
			if bld.Len() != 0 {
				qry = bld.String()
			}
		}
		{
			var lastIsSpace bool
			qry := strings.Map(func(r rune) rune {
				if r == ' ' || r == '\n' || r == '\r' || r == '\t' || r == '\v' {
					if lastIsSpace {
						return -1
					}
					lastIsSpace = true
					return ' '
				}
				lastIsSpace = false
				if 'a' <= r && r <= 'z' {
					return r - 'a' + 'A'
				}
				return r
			},
				qry)
			//log.Println(qry)
			if i := strings.Index(qry, " FETCH FIRST "); i >= 0 {
				qry = strings.TrimSpace(qry[i+len(" FETCH FIRST "):])
				i = strings.Index(qry, " ROWS ONLY")
				if i < 0 {
					i = strings.Index(qry, " ROW ONLY")
				}
				if i >= 0 {
					if n, err := strconv.ParseUint(qry[:i], 10, 32); err == nil && n != 0 {
						batchSize = int(n)
					}
				}
			}
		}
		qry = strings.TrimSuffix(strings.TrimSpace(qry), ";")
		//log.Println("QRY:", qry, "batchSize:", batchSize)
		params = append(params, godror.FetchRowCount(batchSize), godror.PrefetchCount(batchSize+1))
		if rows, err = db.QueryContext(ctx, qry, params...); err != nil {
			qry = origQry
			rows, err = db.QueryContext(ctx, qry, params...)
		}
	}
	if err != nil {
		return nil, nil, fmt.Errorf("%q: %w", qry, err)
	}
	columns, err := dbcsv.GetColumns(ctx, rows)
	logger.Info("GetColumns", "columns", columns)
	if err != nil {
		rows.Close()
		return nil, nil, err
	}
	return rows, columns, nil
}

func splitParamArgs(fun string, args []string) (plsql string, params []interface{}) {
	haveParens := strings.Contains(fun, "(") && strings.Contains(fun, ")")
	params = make([]interface{}, len(args))
	var buf strings.Builder
	buf.WriteString("BEGIN :1 := ")
	buf.WriteString(fun)
	if !haveParens {
		buf.WriteByte('(')
	}
	for i, x := range args {
		var key string
		key, params[i], _ = strings.Cut(x, "=")
		if i != 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(key)
		buf.WriteString("=>:")
		buf.WriteString(strconv.Itoa(i + 2))
	}
	if !haveParens {
		buf.WriteByte(')')
	}
	buf.WriteString("; END;")
	logger.Info("splitParamArgs", "fun", fun, "args", args, "qry", buf.String(), "params", params)
	return buf.String(), params
}

type Query struct {
	Query, Name                      string
	QueueName, TypeName, Correlation string
}

func (Q *Query) ParseQueue() {
	cut := func(s string) (prefix, suffix string, found bool) {
		const sepChars = "/"
		i := strings.IndexAny(s, sepChars)
		if i < 0 {
			return s, "", false
		}
		return s[:i], strings.TrimLeftFunc(s[i+1:], func(r rune) bool { return strings.ContainsRune(sepChars, r) }), true
	}
	var found bool
	s := Q.Query
	Q.QueueName, s, found = cut(s)
	if found {
		if Q.TypeName, Q.Correlation, found = cut(s); !found {
			Q.TypeName, Q.Correlation = Q.Correlation, Q.TypeName
		}
	}
	if Q.TypeName == "" && Q.QueueName != "" {
		// Q_WSC_REQ -> TYP_Q_WSC_REQ
		Q.TypeName = "TYP_" + Q.QueueName
	}
	logger.Debug("NewQueue", "aqName", Q.QueueName, "typName", Q.TypeName, "correlation", Q.Correlation)
}

func (Q *Query) OpenQueue(ctx context.Context, db *sql.DB) (*godror.Queue, error) {
	return godror.NewQueue(ctx, db, Q.QueueName, Q.TypeName, godror.WithDeqOptions(godror.DeqOptions{
		Mode:        godror.DeqRemove,
		Navigation:  godror.NavFirst,
		Visibility:  godror.VisibleImmediate,
		Correlation: Q.Correlation,
		Wait:        time.Second,
	}))
}

// vim: se noet fileencoding=utf-8:
