// Copyright 2020, 2026 Tamás Gulácsi.
//
// SPDX-License-Identifier: Apache-2.0

package main

// nosemgrep: go.lang.security.audit.xss.import-text-template.import-text-template
import (
	"bytes"
	"context"
	"database/sql"
	"errors"
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
	"github.com/peterbourgon/ff/v4"
	"github.com/peterbourgon/ff/v4/ffhelp"

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
	FS := ff.NewFlagSet("csvdbforeach")
	FS.IntVar(&cfg.Sheet, 'S', "sheet", 0, "Index of sheet to convert, zero based")
	flagConnect := FS.StringLong("connect", os.Getenv("DB_ID"), "database connection string")
	flagFunc := FS.StringLong("call", "DBMS_OUTPUT.PUT_LINE", "function name or code block to be called with each line")
	flagFixParams := FS.StringLong("fix", "p_file_name=>{{.FileName}}", "fix parameters to add; uses text/template")
	flagFuncRetOk := FS.IntLong("call-ret-ok", 0, "OK return value")
	flagOneTx := FS.BoolLongDefault("one-tx", true, "one transaction, or commit after each row")
	FS.StringVar(&cfg.Delim, 'd', "delim", "", "Delimiter to use between fields")
	FS.StringVar(&cfg.Charset, 0, "charset", "utf-8", "input charset")
	FS.IntVar(&cfg.Skip, 0, "skip", 1, "skip first N rows")
	FS.StringVar(&cfg.ColumnsString, 0, "columns", "", "column numbers to use, separated by comma, in param order, starts with 1")
	FS.Value('v', "verbose", &verbose, "verbose logging")
	app := ff.Command{Name: "csvdbforeach", Flags: FS,
		Usage: fmt.Sprintf(`%s

	The specified code will be called with the cells as (string) arguments
	(except dates, where DATE will be provided), for each row.

Usage:
	%s [flags] <xlsx/xls/csv-to-be-read>
`, os.Args[0], os.Args[0]),

		Exec: func(ctx context.Context, args []string) error {
			if len(args) != 1 {
				return errors.New("one argument: the filename is needed")
			}
			fn := args[0]
			ctxData := struct {
				FileName string
			}{FileName: fn}
			var fixParams [][2]string
			var buf bytes.Buffer
			if strings.TrimSpace(*flagFixParams) != "" {
				for tup := range strings.SplitSeq(*flagFixParams, ",") {
					parts := strings.SplitN(tup, "=>", 2)
					tpl := template.Must(template.New(parts[0]).Parse(parts[1]))
					buf.Reset()
					if err := tpl.Execute(&buf, ctxData); err != nil {
						return err
					}
					fixParams = append(fixParams, [2]string{parts[0], buf.String()})
				}
			}

			if err := cfg.Open(fn); err != nil {
				return err
			}

			columns, err := cfg.Columns()
			if err != nil {
				return err
			}

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
			fun := *flagFunc
			if b, err := os.ReadFile(fun); err == nil {
				fun = string(b)
			}
			n, err = dbExec(grpCtx, db, fun, fixParams, int64(*flagFuncRetOk), rows, *flagOneTx)
			if err != nil {
				return fmt.Errorf("exec %q: %w", *flagFunc, err)
			}
			if err = grp.Wait(); err != nil {
				return err
			}
			d := time.Since(start)
			logger.Debug("processed", "rows", n, "dur", d.String())
			return nil
		},
	}

	slog.SetDefault(logger)
	if *flagConnect == "" {
		if *flagConnect = os.Getenv("BRUNO_OWNER_ID"); *flagConnect == "" {
			*flagConnect = os.Getenv("BRUNO_ID")
		}
	}
	if err := app.Parse(os.Args[1:]); err != nil {
		ffhelp.Command(&app).WriteTo(os.Stderr)
		if errors.Is(err, ff.ErrHelp) {
			return nil
		}
		return err
	}

	ctx, cancel := dbcsv.Wrap(context.Background())
	defer cancel()
	ctx = zlog.NewSContext(ctx, logger)

	return app.Run(ctx)
}

// vim: set fileencoding=utf-8 noet:
