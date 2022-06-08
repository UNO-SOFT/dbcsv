// Copyright 2020, 2022 Tamás Gulácsi.
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
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"golang.org/x/sync/errgroup"
	"golang.org/x/text/encoding"

	"github.com/UNO-SOFT/dbcsv"
	"github.com/UNO-SOFT/spreadsheet"
	"github.com/UNO-SOFT/spreadsheet/ods"
	"github.com/UNO-SOFT/spreadsheet/xlsx"
	"github.com/godror/godror"
	"github.com/klauspost/compress/gzip"
	"github.com/klauspost/compress/zstd"
)

func main() {
	if err := Main(); err != nil {
		log.Fatalf("%+v", err)
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
	flagVerbose := flag.Bool("v", false, "verbose logging")
	flagCompress := flag.String("compress", "", "compress output with gz/gzip or zst/zstd/zstandard")
	flagCall := flag.Bool("call", false, "the first argument is not the WHERE, but the PL/SQL block to be called, the followings are not the columns but the arguments")
	flagTimeout := flag.Duration("timeout", 15*time.Minute, "timeout")

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
		*flagConnect = os.Getenv("BRUNO_ID")
	}
	flag.Parse()

	Log := func(...interface{}) error { return nil }
	if *flagVerbose {
		Log = func(keyvals ...interface{}) error {
			if len(keyvals)%2 != 0 {
				keyvals = append(keyvals, "")
			}
			vv := make([]interface{}, len(keyvals)/2)
			for i := range vv {
				v := fmt.Sprintf("%+v", keyvals[(i<<1)+1])
				if strings.Contains(v, " ") {
					v = `"` + v + `"`
				}
				vv[i] = fmt.Sprintf("%s=%s", keyvals[(i<<1)], v)
			}
			log.Println(vv...)
			return nil
		}
	}

	enc, err := dbcsv.EncFromName(*flagEnc)
	if err != nil {
		return err
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

	var queries []string
	var params []interface{}
	if len(flagSheets.Strings) != 0 {
		queries = flagSheets.Strings
	} else if *flagCall {
		var buf strings.Builder
		fmt.Fprintf(&buf, `BEGIN :1 := %s(`, flag.Arg(0))
		params = make([]interface{}, flag.NArg()-1)
		for i, x := range flag.Args()[1:] {
			arg := strings.SplitN(x, "=", 2)
			params[i] = ""
			if len(arg) > 1 {
				params[i] = arg[1]
			}
			if i != 0 {
				buf.WriteString(", ")
			}
			fmt.Fprintf(&buf, "%s=>:%d", arg[0], i+2)
		}
		buf.WriteString("); END;")
		qry := buf.String()
		if Log != nil {
			_ = Log("call", qry, "params", params)
		}
		queries = append(queries, qry)
	} else {
		params = make([]interface{}, len(flagParams.Strings))
		for i, p := range flagParams.Strings {
			params[i] = p
		}
		var (
			where   string
			columns []string
		)
		if flag.NArg() > 1 {
			where = flag.Arg(1)
			if flag.NArg() > 2 {
				columns = flag.Args()[2:]
			}
		}
		qry := getQuery(flag.Arg(0), where, columns, dbcsv.DefaultEncoding)
		queries = append(queries, qry)
	}
	db, err := sql.Open("godror", *flagConnect)
	if err != nil {
		return fmt.Errorf("%s: %w", *flagConnect, err)
	}
	defer db.Close()
	db.SetMaxOpenConns(2)
	db.SetMaxIdleConns(1)
	ctx, cancel := context.WithTimeout(context.Background(), *flagTimeout)
	defer cancel()
	ctx, cancel = dbcsv.Wrap(ctx)
	defer cancel()

	fh := os.Stdout
	if !(*flagOut == "" || *flagOut == "-") {
		_ = os.MkdirAll(filepath.Dir(*flagOut), 0775)
		if fh, err = os.Create(*flagOut); err != nil {
			return fmt.Errorf("%s: %w", *flagOut, err)
		}
	}
	defer fh.Close()
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

	if Log != nil {
		_ = Log("msg", "writing", "file", fh.Name(), "encoding", enc)
	}
	tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		log.Printf("[WARN] Read-Only transaction: %v", err)
		if tx, err = db.BeginTx(ctx, nil); err != nil {
			return fmt.Errorf("%s: %w", "beginTx", err)
		}
	}
	defer tx.Rollback()

	if len(flagSheets.Strings) == 0 {
		w := encoding.ReplaceUnsupported(enc.NewEncoder()).Writer(wfh)
		if Log != nil {
			_ = Log("env_encoding", dbcsv.DefaultEncoding.Name)
		}

		rows, columns, qErr := doQuery(ctx, tx, queries[0], params, *flagCall, *flagSort)
		if qErr != nil {
			err = qErr
		} else {
			defer rows.Close()
			err = dbcsv.DumpCSV(ctx, w, rows, columns, *flagHeader, *flagSep, *flagRaw, Log)
		}
	} else {
		var w spreadsheet.Writer
		if strings.HasSuffix(fh.Name(), ".xlsx") {
			w = xlsx.NewWriter(wfh)
		} else {
			w, err = ods.NewWriter(wfh)
			if err != nil {
				return err
			}
		}
		defer w.Close()
		dec := enc.Encoding.NewDecoder()
		grp, grpCtx := errgroup.WithContext(ctx)
		for sheetNo := range queries {
			qry := queries[sheetNo]
			if qry, err = dec.String(qry); err != nil {
				return fmt.Errorf("%q: %w", queries[sheetNo], err)
			}
			var name string
			i := strings.IndexByte(qry, ':')
			if i >= 0 {
				name, qry = qry[:i], qry[i+1:]
			}
			if name == "" {
				name = strconv.Itoa(sheetNo + 1)
			}
			rows, columns, qErr := doQuery(grpCtx, tx, qry, nil, false, *flagSort)
			if qErr != nil {
				err = qErr
				break
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
				_ = Log(name, qry)
				err := dbcsv.DumpSheet(grpCtx, sheet, rows, columns, Log)
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
		if closeErr := w.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}
	cancel()
	if wfh != fh {
		if closeErr := wfh.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}
	if closeErr := fh.Close(); closeErr != nil && err == nil {
		err = closeErr
	}
	return err
}

func getQuery(table, where string, columns []string, enc encoding.Encoding) string {
	if table == "" && where == "" && len(columns) == 0 {
		if enc == nil {
			enc = encoding.Nop
		}
		b, err := ioutil.ReadAll(enc.NewDecoder().Reader(os.Stdin))
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
	columns, err := dbcsv.GetColumns(rows)
	if err != nil {
		rows.Close()
		return nil, nil, err
	}
	return rows, columns, nil
}

// vim: se noet fileencoding=utf-8:
