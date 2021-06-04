// Copyright 2021 Tamás Gulácsi. All rights reserved.

// SPDX-License-Identifier: Apache-2.0

// Package main in tablecopy is a table copier between databases.
package main

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"flag"
	"fmt"
	"log"
	"os"
	"reflect"
	"strings"
	"time"

	godror "github.com/godror/godror"

	"golang.org/x/sync/errgroup"
)

func main() {
	if err := Main(); err != nil {
		log.Fatalf("%+v", err)
	}
}

const DefaultBatchSize = 8192

func Main() error {
	flagSource := flag.String("src", os.Getenv("DB_ID"), "user/passw@sid to read from")
	flagSourcePrep := flag.String("src-prep", "", "prepare source connection (run statements separated by ;\\n)")
	flagDest := flag.String("dst", os.Getenv("DB_ID"), "user/passw@sid to write to")
	flagDestPrep := flag.String("dst-prep", "", "prepare destination connection (run statements separated by ;\\n)")
	flagReplace := flag.String("replace", "", "replace FIELD_NAME=WITH_VALUE,OTHER=NEXT")
	flagVerbose := flag.Bool("v", false, "verbose logging")
	flagTimeout := flag.Duration("timeout", 1*time.Minute, "timeout")
	flagTableTimeout := flag.Duration("table-timeout", 10*time.Second, "per-table-timeout")
	flagConc := flag.Int("concurrency", 8, "concurrency")
	flagTruncate := flag.Bool("truncate", false, "truncate dest tables (must have different name)")
	flagBatchSize := flag.Int("batch-size", DefaultBatchSize, "batch size")

	flag.Usage = func() {
		fmt.Fprintln(flag.CommandLine.Output(), strings.Replace(`Usage of {{.prog}}:
	{{.prog}} [options] 'T_able'

will execute a "SELECT * FROM T_able@source_db" and an "INSERT INTO T_able@dest_db"

	{{.prog}} [options] 'Source_table' 'F_ield=1'

will execute a "SELECT * FROM Source_table@source_db WHERE F_ield=1" and an "INSERT INTO Source_table@dest_db"

	{{.prog}} 'Source_table' '1=1' 'Dest_table'
will execute a "SELECT * FROM Source_table@source_db WHERE F_ield=1" and an "INSERT INTO Dest_table@dest_db", matching the fields.

`, "{{.prog}}", os.Args[0], -1))
		flag.PrintDefaults()
	}
	if *flagSource == "" {
		*flagSource = os.Getenv("BRUNO_ID")
	}
	if *flagDest == "" {
		*flagDest = os.Getenv("BRUNO_ID")
	}
	flag.Parse()
	if *flagTimeout == 0 {
		*flagTimeout = time.Hour
	}
	if *flagTableTimeout > *flagTimeout {
		*flagTableTimeout = *flagTimeout
	}

	var Log func(...interface{}) error
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
	var replace map[string]string
	if *flagReplace != "" {
		fields := strings.Split(*flagReplace, ",")
		replace = make(map[string]string, len(fields))
		for _, f := range fields {
			if i := strings.IndexByte(f, '='); i < 0 {
				continue
			} else {
				replace[strings.ToUpper(f[:i])] = f[i+1:]
			}
		}
	}

	tables := make([]copyTask, 0, 4)
	if flag.NArg() == 0 || flag.NArg() == 1 && flag.Arg(0) == "-" {
		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			parts := bytes.SplitN(scanner.Bytes(), []byte(" "), 2)
			tbl := copyTask{Replace: replace, Truncate: *flagTruncate}
			if i := bytes.IndexByte(parts[0], '='); i >= 0 {
				tbl.Src, tbl.Dst = string(parts[0][:i]), string(parts[0][i+1:])
			} else {
				tbl.Src = string(parts[0])
			}
			if len(parts) > 1 {
				tbl.Where = string(parts[1])
			}
			tables = append(tables, tbl)
		}
	} else {
		tbl := copyTask{Src: flag.Arg(0), Replace: replace, Truncate: *flagTruncate}
		if flag.NArg() > 1 {
			tbl.Where = flag.Arg(1)
			if flag.NArg() > 2 {
				tbl.Dst = flag.Args()[2]
			}
		}
		tables = append(tables, tbl)
	}

	mkInit := func(queries string) func(context.Context, driver.ConnPrepareContext) error {
		if queries == "" {
			return func(context.Context, driver.ConnPrepareContext) error { return nil }
		}
		qs := strings.Split(queries, ";\n")
		return func(ctx context.Context, conn driver.ConnPrepareContext) error {
			for _, qry := range qs {
				stmt, err := conn.PrepareContext(ctx, qry)
				if err != nil {
					return fmt.Errorf("%s: %w", qry, err)
				}
				_, err = stmt.(driver.StmtExecContext).ExecContext(ctx, nil)
				stmt.Close()
				if err != nil {
					return err
				}
			}
			return nil
		}
	}

	srcP, err := godror.ParseDSN(*flagSource)
	if err != nil {
		return fmt.Errorf("%q: %w", *flagSource, err)
	}
	if *flagSourcePrep != "" {
		srcP.OnInit = mkInit(*flagSourcePrep)
	}
	srcConnector := godror.NewConnector(srcP)
	srcDB := sql.OpenDB(srcConnector)
	defer srcDB.Close()

	dstP, err := godror.ParseDSN(*flagDest)
	if err != nil {
		return fmt.Errorf("%q: %w", *flagDest, err)
	}
	if *flagDestPrep != "" {
		dstP.OnInit = mkInit(*flagDestPrep)
	}
	dstConnector := godror.NewConnector(dstP)
	if err != nil {
		return err
	}
	dstDB := sql.OpenDB(dstConnector)
	defer dstDB.Close()

	ctx, cancel := context.WithTimeout(context.Background(), *flagTimeout)
	defer cancel()

	grp, subCtx := errgroup.WithContext(ctx)
	concLimit := make(chan struct{}, *flagConc)
	srcTx, err := srcDB.BeginTx(subCtx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		log.Printf("[WARN] Read-Only transaction: %v", err)
		if srcTx, err = srcDB.BeginTx(subCtx, nil); err != nil {
			return fmt.Errorf("%s: %w", "beginTx", err)
		}
	}
	defer srcTx.Rollback()

	dstTx, err := dstDB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer dstTx.Rollback()

	for _, task := range tables {
		if task.Src == "" {
			continue
		}
		if task.Dst == "" {
			task.Dst = task.Src
		}
		if !strings.EqualFold(task.Dst, task.Src) || dstP.String() != srcP.String() {
			dstDB.ExecContext(subCtx, fmt.Sprintf("CREATE TABLE %s AS SELECT * FROM %s WHERE 1=0", task.Dst, task.Src))
			if task.Truncate {
				if Log != nil {
					Log("msg", "TRUNCATE", "table", task.Dst)
				}
				if _, err := dstDB.ExecContext(subCtx, "TRUNCATE TABLE "+task.Dst); err != nil {
					if _, err = dstDB.ExecContext(subCtx, "DELETE FROM "+task.Dst); err != nil {
						return fmt.Errorf("TRUNCATE TABLE %s: %w", task.Dst, err)
					}
				}
			}
		}
	}
	for _, task := range tables {
		if task.Src == "" {
			continue
		}
		task := task
		grp.Go(func() error {
			select {
			case concLimit <- struct{}{}:
				defer func() { <-concLimit }()
			case <-subCtx.Done():
				return subCtx.Err()
			}
			start := time.Now()
			oneCtx, oneCancel := context.WithTimeout(subCtx, *flagTableTimeout)
			n, err := One(oneCtx, dstTx, srcTx, task, *flagBatchSize, Log)
			oneCancel()
			dur := time.Since(start)
			log.Println(task.Src, n, dur)
			return err
		})
	}
	if err := grp.Wait(); err != nil {
		return err
	}
	return dstTx.Commit()
}

type copyTask struct {
	Replace         map[string]string
	Src, Dst, Where string
	Truncate        bool
}

func One(ctx context.Context, dstTx, srcTx *sql.Tx, task copyTask, batchSize int, Log func(...interface{}) error) (int64, error) {
	Log("msg", "One", "task", task)
	if task.Dst == "" {
		task.Dst = task.Src
	}
	var n int64
	srcCols, err := getColumns(ctx, srcTx, task.Src)
	if err != nil {
		return n, fmt.Errorf("sources: %w", err)
	}

	dstCols, err := getColumns(ctx, dstTx, task.Dst)
	if err != nil {
		return n, fmt.Errorf("dest: %w", err)
	}
	m := make(map[string]struct{}, len(dstCols))
	for _, c := range dstCols {
		m[c] = struct{}{}
	}

	var srcBld, dstBld, ph strings.Builder
	srcBld.WriteString("SELECT ")
	fmt.Fprintf(&dstBld, "INSERT INTO %s (", task.Dst)
	var i int
	tbr := make([]string, 0, len(task.Replace))
	for _, k := range srcCols {
		if _, ok := m[k]; !ok {
			continue
		}
		if _, ok := task.Replace[k]; ok {
			tbr = append(tbr, k)
			continue
		}
		if i != 0 {
			srcBld.WriteByte(',')
			dstBld.WriteByte(',')
			ph.WriteByte(',')
		}
		i++
		srcBld.WriteString(k)
		dstBld.WriteString(k)
		fmt.Fprintf(&ph, ":%d", i)
	}
	for _, k := range tbr {
		dstBld.WriteByte(',')
		dstBld.WriteString(k)
		ph.WriteString(",'")
		ph.WriteString(strings.ReplaceAll(task.Replace[k], "'", "''"))
		ph.WriteByte('\'')
	}
	fmt.Fprintf(&srcBld, " FROM %s", task.Src)
	if task.Where != "" {
		fmt.Fprintf(&srcBld, " WHERE %s", task.Where)
	}
	fmt.Fprintf(&dstBld, ") VALUES (%s)", ph.String())

	srcQry, dstQry := srcBld.String(), dstBld.String()
	stmt, err := dstTx.PrepareContext(ctx, dstQry)
	if err != nil {
		return n, fmt.Errorf("%s: %w", dstQry, err)
	}
	defer stmt.Close()
	if Log != nil {
		Log("src", srcQry)
		Log("dst", dstQry)
	}

	if batchSize < 1 {
		batchSize = DefaultBatchSize
	}
	rows, err := srcTx.QueryContext(ctx, srcQry,
		godror.FetchArraySize(batchSize), godror.PrefetchCount(batchSize+1))
	if err != nil {
		return n, fmt.Errorf("%s: %w", srcQry, err)
	}
	defer rows.Close()
	types, err := rows.ColumnTypes()
	if err != nil {
		return n, fmt.Errorf("%s: %w", srcQry, err)
	}

	values := make([]interface{}, len(types))
	rBatch := make([]reflect.Value, len(values))
	batchValues := make([]interface{}, 0, len(rBatch))
	for i, t := range types {
		et := t.ScanType()
		values[i] = reflect.New(et).Interface()
		rBatch[i] = reflect.MakeSlice(reflect.SliceOf(et), 0, batchSize)
	}
	doInsert := func() error {
		batchValues = batchValues[:0]
		for _, v := range rBatch {
			batchValues = append(batchValues, v.Interface())
		}
		if _, err = stmt.ExecContext(ctx, batchValues...); err != nil {
			return fmt.Errorf("%s %v: %w", dstQry, batchValues, err)
		}
		return nil
	}

	for rows.Next() {
		if err = rows.Scan(values...); err != nil {
			return n, err
		}
		for i, v := range values {
			rBatch[i] = reflect.Append(rBatch[i], reflect.ValueOf(v).Elem())
		}
		if m := rBatch[0].Len(); m == batchSize {
			if err = doInsert(); err != nil {
				return n, err
			}

			n += int64(m)
			for i := range rBatch {
				rBatch[i] = rBatch[i].Slice(0, 0)
			}
		}
	}
	if m := rBatch[0].Len(); m != 0 {
		if err = doInsert(); err != nil {
			return n, fmt.Errorf("%s %v: %w", dstQry, batchValues, err)
		}
		n += int64(m)
	}
	return n, nil
}

func getColumns(ctx context.Context, tx *sql.Tx, tbl string) ([]string, error) {
	qry := "SELECT * FROM " + tbl + " WHERE 1=0"
	rows, err := tx.QueryContext(ctx, qry)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", qry, err)
	}
	cols, err := rows.Columns()
	rows.Close()
	return cols, err
}

// vim: se noet fileencoding=utf-8:
