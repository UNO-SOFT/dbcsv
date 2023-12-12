// Copyright 2020, 2022 Tamás Gulácsi.
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
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"

	"golang.org/x/sync/errgroup"

	"github.com/UNO-SOFT/dbcsv"
	"github.com/UNO-SOFT/zlog/v2"
	"github.com/godror/godror"
)

const DefaultFetchRowCount = 8

var (
	verbose zlog.VerboseVar
	logger  = zlog.NewLogger(zlog.MaybeConsoleHandler(&verbose, os.Stderr))
)

func main() {
	if err := Main(); err != nil {
		logger.Error(err, "Main")
		os.Exit(1)
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
	flag.Var(&verbose, "v", "verbose logging")

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
		if *flagConnect = os.Getenv("BRUNO_OWNER_ID"); *flagConnect == "" {
			*flagConnect = os.Getenv("BRUNO_ID")
		}
	}
	flag.Parse()

	envEnc, err := dbcsv.EncFromName(*flagEnc)
	if err != nil {
		return err
	}

	queries := flag.Args()
	if len(queries) == 0 || len(queries) == 1 && (queries[0] == "-" || queries[0] == "") {
		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			queries = append(queries, scanner.Text())
		}
	}
	for i, q := range queries {
		if queries[i], err = envEnc.NewDecoder().String(q); err != nil {
			return fmt.Errorf("%q: %w", q, err)
		}
	}

	params := make([]interface{}, 0, len(flagValues.Strings))
	for _, s := range flagValues.Strings {
		if i := strings.IndexAny(s, "-:= \t"); i < 0 {
			return fmt.Errorf("%q does not contain a separator", s)
		} else {
			params = append(params, sql.Named(strings.ToLower(s[:i]), s[i+1:]))
		}
	}
	db, err := sql.Open("godror", *flagConnect)
	if err != nil {
		return fmt.Errorf("%s: %w", *flagConnect, err)
	}
	defer db.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fh := os.Stdout
	if !(*flagOut == "" || *flagOut == "-") {
		// nosemgrep: go.lang.correctness.permissions.file_permission.incorrect-default-permission
		_ = os.MkdirAll(filepath.Dir(*flagOut), 0750)
		if fh, err = os.Create(*flagOut); err != nil {
			return fmt.Errorf("%s: %w", *flagOut, err)
		}
	}
	defer fh.Close()
	bw := bufio.NewWriter(fh)
	defer bw.Flush()

	logger.Info("writing", "file", fh.Name())

	if _, err := bw.WriteString("[\n"); err != nil {
		return err
	}
	first := true
	concLimit := make(chan struct{}, *flagConcurrency)
	enc := json.NewEncoder(bw)
	var bwMu sync.Mutex
	grp, grpCtx := errgroup.WithContext(ctx)
	for _, qry := range queries {
		qry := qry
		grp.Go(func() error {
			concLimit <- struct{}{}
			defer func() { <-concLimit }()

			tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
			if err != nil {
				return err
			}
			defer tx.Rollback()

			i := strings.IndexByte(qry, ':')
			name, qry := qry[:i], qry[i+1:]
			rows, err := doQuery(grpCtx, tx, qry, *flagFetchRowCount, params)
			if err == nil && len(rows) == 0 {
				return nil
			}
			var errS string
			if err != nil {
				if errors.Is(err, context.Canceled) {
					return nil
				}
				errS = err.Error()
			}
			bwMu.Lock()
			if first {
				first = false
			} else {
				if err = bw.WriteByte(','); err != nil {
					return err
				}
			}
			if encErr := enc.Encode(Table{Name: name, Error: errS, Rows: rows}); encErr != nil && err == nil {
				err = encErr
			}
			bwMu.Unlock()
			return err
		})
	}
	if err = grp.Wait(); err != nil {
		return err
	}
	_, _ = bw.WriteString("]\n")
	if err = bw.Flush(); err != nil {
		return err
	}
	return fh.Close()
}

type Table struct {
	Name  string                   `json:"name"`
	Error string                   `json:"error,omitempty"`
	Rows  []map[string]interface{} `json:"rows"`
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

func doQuery(ctx context.Context, db queryExecer, qry string, fetchRowCount int, params []interface{}) ([]map[string]interface{}, error) {
	if fetchRowCount <= 0 {
		fetchRowCount = DefaultFetchRowCount
	}
	params = append(params, godror.FetchRowCount(fetchRowCount))
	rows, err := db.QueryContext(ctx, qry, params...)
	if err != nil {
		return nil, fmt.Errorf("%q: %w", qry, err)
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	vals := make([]interface{}, len(columns))
	dest := make([]interface{}, len(columns))
	for i := range vals {
		dest[i] = &vals[i]
	}
	values := make([]map[string]interface{}, 0, fetchRowCount)
	for rows.Next() {
		if err := rows.Scan(dest...); err != nil {
			return values, fmt.Errorf("scan into %#v: %w", dest, err)
		}
		m := make(map[string]interface{}, len(vals))
		for i := range vals {
			if vals[i] == nil || reflect.ValueOf(vals[i]).IsZero() {
				continue
			}
			m[columns[i]] = vals[i]
		}
		values = append(values, m)
	}
	return values, rows.Close()
}

// vim: se noet fileencoding=utf-8:
