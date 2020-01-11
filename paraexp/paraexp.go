// Copyright 2020 Tamás Gulácsi.
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

// Package main in paraexp represents a parallel query-to-JSON dumper
package main

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"

	"golang.org/x/sync/errgroup"

	"github.com/UNO-SOFT/dbcsv"
	"github.com/godror/godror"

	errors "golang.org/x/xerrors"
)

const DefaultFetchRowCount = 8

func main() {
	if err := Main(); err != nil {
		log.Fatalf("%+v", err)
	}
}

func Main() error {
	flagConnect := flag.String("connect", os.Getenv("DB_ID"), "user/passw@sid to connect to")
	flagConcurrency := flag.Int("concurrency", runtime.GOMAXPROCS(-1), "concurrency to run the queries")
	flagFetchRowCount := flag.Int("fetch-row-count", DefaultFetchRowCount, "fetch row count")
	flagEnc := flag.String("encoding", dbcsv.DefaultEncoding.Name, "encoding to use for input")
	flagOut := flag.String("o", "-", "output (defaults to stdout)")
	flagValues := dbcsv.FlagStrings()
	flag.Var(flagValues, "value", "each -value=name:value will be bond on each query")
	flagVerbose := flag.Bool("v", false, "verbose logging")

	flag.Usage = func() {
		fmt.Fprintln(flag.CommandLine.Output(), strings.Replace(`Usage of {{.prog}}:
{{.prog}} [options] -value v_alue1=1 -value v_value2=3.14 'name1:SELECT * FROM T_able1 WHERE F_ield=:v_alue1' 'name2:SELECT * FROM T_able2 WHERE F_ield=:v_alue2' ...

will execute a "SELECT * FROM T_able1 WHERE F_ield=1" and "SELECT * FROM T_able2 WHERE F_ield=3.14"
parallel and dump all the results in one JSON object, named as "name1" and "name2":

  {"name1":[{"rownum":1,"F_IELD":1,...}],"name2":[{"rownum":2,"F_IELD":3.14,...}]}

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

	envEnc, err := dbcsv.EncFromName(*flagEnc)
	if err != nil {
		return err
	}

	queries := flag.Args()
	for i, q := range queries {
		if queries[i], err = envEnc.NewDecoder().String(q); err != nil {
			return errors.Errorf("%q: %w", q, err)
		}
	}

	params := make([]sql.NamedArg, 0, len(flagValues.Strings))
	for _, s := range flagValues.Strings {
		if i := strings.IndexAny(s, "-:= \t"); i < 0 {
			return errors.Errorf("%q does not contain a separator", s)
		} else {
			params = append(params, sql.Named(strings.ToLower(s[:i]), s[i+1:]))
		}
	}
	db, err := sql.Open("godror", *flagConnect)
	if err != nil {
		return errors.Errorf("%s: %w", *flagConnect, err)
	}
	defer db.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fh := os.Stdout
	if !(*flagOut == "" || *flagOut == "-") {
		os.MkdirAll(filepath.Dir(*flagOut), 0775)
		if fh, err = os.Create(*flagOut); err != nil {
			return errors.Errorf("%s: %w", *flagOut, err)
		}
	}
	defer fh.Close()
	bw := bufio.NewWriter(fh)
	defer bw.Flush()

	if Log != nil {
		Log("msg", "writing", "file", fh.Name())
	}

	bw.WriteString("[\n")
	concLimit := make(chan struct{}, *flagConcurrency)
	enc := json.NewEncoder(bw)
	var bwMu sync.Mutex
	grp, grpCtx := errgroup.WithContext(ctx)
	for _, qry := range queries {
		tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
		if err != nil {
			return err
		}
		grp.Go(func() error {
			defer tx.Rollback()
			concLimit <- struct{}{}
			defer func() { <-concLimit }()
			i := strings.IndexByte(qry, ':')
			name, qry := qry[:i], qry[i+1:]
			rows, err := doQuery(grpCtx, tx, qry, *flagFetchRowCount, params)
			bwMu.Lock()
			if encErr := enc.Encode(Table{Name: name, Error: err, Rows: rows}); encErr != nil && err == nil {
				err = encErr
			}
			bwMu.Unlock()
			return err
		})
	}
	if err = grp.Wait(); err != nil {
		return err
	}
	bw.WriteString("\n]\n")
	return fh.Close()
}

type Table struct {
	Name  string
	Error error
	Rows  []map[string]interface{}
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

func doQuery(ctx context.Context, db queryExecer, qry string, fetchRowCount int, params []sql.NamedArg) ([]map[string]interface{}, error) {
	if fetchRowCount <= 0 {
		fetchRowCount = DefaultFetchRowCount
	}
	rows, err := db.QueryContext(ctx, qry, godror.FetchRowCount(fetchRowCount), params)
	if err != nil {
		return nil, errors.Errorf("%q: %w", qry, err)
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	vals := make([]map[string]interface{}, fetchRowCount)
	dest := make([]interface{}, len(columns))
	for rows.Next() {
		if err := rows.Scan(dest...); err != nil {
			return vals, errors.Errorf("scan into %#v: %w", dest, err)
		}
		m := make(map[string]interface{}, len(dest))
		for i := range dest {
			m[columns[i]] = reflect.ValueOf(dest[i]).Elem().Interface()
			vals = append(vals, m)
		}
	}
	return vals, rows.Err()
}

// vim: se noet fileencoding=utf-8:
