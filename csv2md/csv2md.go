// Copyright 2024 Tamás Gulácsi. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bufio"
	"bytes"
	"context"
	"flag"
	"io"
	"log"
	"os"
	"os/signal"
	"strings"

	"github.com/UNO-SOFT/dbcsv"
)

func main() {
	if err := Main(); err != nil {
		log.Fatal(err)
	}
}

func Main() error {
	flag.Parse()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	defer os.Stdout.Close()
	bw := bufio.NewWriter(os.Stdout)
	defer bw.Flush()

	var buf bytes.Buffer
	var emptyRows []string
	if err := dbcsv.ReadFile(ctx, flag.Arg(0),
		func(ctx context.Context, sheetName string, row dbcsv.Row) error {
			if row.Line == 0 {
				bw.WriteString("# " + sheetName + "\n")
			}
			buf.Reset()
			if err := printRow(&buf, row); err != nil {
				return err
			}
			if bytes.IndexFunc(buf.Bytes(), func(r rune) bool { return !(r == '|' || r == ' ' || r == '-' || r == '\n') }) < 0 {
				// empty row
				emptyRows = append(emptyRows, buf.String())
				return nil
			}
			for _, s := range emptyRows {
				bw.WriteString(s)
			}
			emptyRows = emptyRows[:0]
			bw.Write(buf.Bytes())
			if row.Line == 0 {
				// first row
				p := buf.Bytes()
				var afterPipe bool
				for i, b := range p {
					if b == '|' || b == '\n' {
						afterPipe = true
					} else if afterPipe {
						p[i] = ' '
						afterPipe = false
					} else if len(p) > i && p[i+1] == '|' { // beforePipe
						p[i] = ' '
					} else {
						p[i] = '-'
					}
				}
				bw.Write(p)
			}
			return nil
		},
	); err != nil {
		return err
	}
	return bw.Flush()
}

var quote = strings.NewReplacer("|", "&#124;", "\n", "<br/>")

func printRow(w io.Writer, row dbcsv.Row) error {
	for i, v := range row.Values {
		if i == 0 {
			w.Write([]byte("|"))
		}
		io.WriteString(w, " "+quote.Replace(v))
		w.Write([]byte(" |"))
	}
	_, err := w.Write([]byte("\n"))
	return err
}
