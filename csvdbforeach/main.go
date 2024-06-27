// Copyright 2020 Tamás Gulácsi.
//
// SPDX-License-Identifier: Apache-2.0

package main

// nosemgrep: go.lang.security.audit.xss.import-text-template.import-text-template
import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"text/template"
	"time"

	"golang.org/x/sync/errgroup"
	"golang.org/x/text/encoding/htmlindex"
	"golang.org/x/text/transform"

	"github.com/UNO-SOFT/dbcsv"
	"github.com/UNO-SOFT/zlog/v2"

	_ "github.com/godror/godror"
)

var (
	stdout = io.Writer(os.Stdout)
	stderr = io.Writer(os.Stderr)

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
	if lang := os.Getenv("LANG"); lang != "" {
		if i := strings.LastIndex(lang, "."); i >= 0 {
			lang = lang[i+1:]
			enc, err := htmlindex.Get(lang)
			if err != nil {
				return fmt.Errorf("get encoding for %q: %w", lang, err)
			}
			stdout = transform.NewWriter(stdout, enc.NewEncoder())
			stderr = transform.NewWriter(stderr, enc.NewEncoder())
		}
	}

	var cfg dbcsv.Config
	flag.IntVar(&cfg.Sheet, "sheet", 0, "Index of sheet to convert, zero based")
	flagConnect := flag.String("connect", os.Getenv("DB_ID"), "database connection string")
	flagFunc := flag.String("call", "DBMS_OUTPUT.PUT_LINE", "function name to be called with each line")
	flagFixParams := flag.String("fix", "p_file_name=>{{.FileName}}", "fix parameters to add; uses text/template")
	flagFuncRetOk := flag.Int("call-ret-ok", 0, "OK return value")
	flagOneTx := flag.Bool("one-tx", true, "one transaction, or commit after each row")
	flag.StringVar(&cfg.Delim, "d", "", "Delimiter to use between fields")
	flag.StringVar(&cfg.Charset, "charset", "utf-8", "input charset")
	flag.IntVar(&cfg.Skip, "skip", 1, "skip first N rows")
	flag.StringVar(&cfg.ColumnsString, "columns", "", "column numbers to use, separated by comma, in param order, starts with 1")
	flag.Var(&verbose, "v", "verbose logging")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, `%s

	The specified code will be called with the cells as (string) arguments
	(except dates, where DATE will be provided), for each row.

Usage:
	%s [flags] <xlsx/xls/csv-to-be-read>
`, os.Args[0], os.Args[0])
		flag.PrintDefaults()
	}

	if *flagConnect == "" {
		if *flagConnect = os.Getenv("BRUNO_OWNER_ID"); *flagConnect == "" {
			*flagConnect = os.Getenv("BRUNO_ID")
		}
	}
	flag.Parse()
	if flag.NArg() != 1 {
		flag.Usage()
		return errors.New("one argument: the filename is needed")
	}

	slog.SetDefault(logger)

	ctxData := struct {
		FileName string
	}{FileName: flag.Arg(0)}
	var fixParams [][2]string
	var buf bytes.Buffer
	if strings.TrimSpace(*flagFixParams) != "" {
		for _, tup := range strings.Split(*flagFixParams, ",") {
			parts := strings.SplitN(tup, "=>", 2)
			tpl := template.Must(template.New(parts[0]).Parse(parts[1]))
			buf.Reset()
			if err := tpl.Execute(&buf, ctxData); err != nil {
				return err
			}
			fixParams = append(fixParams, [2]string{parts[0], buf.String()})
		}
	}

	if err := cfg.Open(flag.Arg(0)); err != nil {
		return err
	}

	columns, err := cfg.Columns()
	if err != nil {
		return err
	}

	ctx, cancel := dbcsv.Wrap(context.Background())
	defer cancel()
	ctx = zlog.NewSContext(ctx, logger)

	rows := make(chan dbcsv.Row, 8)
	grp, grpCtx := errgroup.WithContext(ctx)
	grp.Go(func() error {
		defer close(rows)
		return cfg.ReadRows(grpCtx,
			func(ctx context.Context, _ string, row dbcsv.Row) error {
				logger.Debug("read", "row", row)
				// filter out empty rows
				empty := true
				for _, s := range row.Values {
					if s != "" {
						empty = false
						break
					}
				}
				if empty {
					return nil
				}
				if len(columns) > 0 {
					row2 := dbcsv.Row{Line: row.Line, Values: make([]string, len(columns))}
					for i, j := range columns {
						if j < len(row.Values) {
							row2.Values[i] = row.Values[j]
						} else {
							row2.Values[i] = ""
						}
					}
					row = row2
				}

				select {
				case <-ctx.Done():
					return ctx.Err()
				case rows <- row:
					logger.Debug("filtered", "row", row)
				}
				return nil
			},
		)
	})

	dsn := os.ExpandEnv(*flagConnect)
	db, err := sql.Open("godror", dsn)
	if err != nil {
		return fmt.Errorf("%s: %w", dsn, err)
	}
	defer db.Close()

	var n int
	start := time.Now()
	n, err = dbExec(db, *flagFunc, fixParams, int64(*flagFuncRetOk), rows, *flagOneTx)
	if err != nil {
		return fmt.Errorf("exec %q: %w", *flagFunc, err)
	}
	if err = grp.Wait(); err != nil {
		return err
	}
	d := time.Since(start)
	logger.Debug("processed", "rows", n, "dur", d.String())
	return nil
}

// vim: set fileencoding=utf-8 noet:
