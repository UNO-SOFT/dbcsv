// Copyright 2020, 2022 Tamás Gulácsi.
//
// SPDX-License-Identifier: Apache-2.0

package dbcsv_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/UNO-SOFT/dbcsv"
	"github.com/google/go-cmp/cmp"
)

func TestRead(t *testing.T) {
	dh, err := os.Open("testdata")
	if err != nil {
		t.Skip(err)
	}
	defer dh.Close()
	names, err := dh.Readdirnames(-1)
	if err != nil && len(names) == 0 {
		t.Skip(err)
	}
	for _, fn := range names {
		fn = filepath.Join(dh.Name(), fn)
		var cfg dbcsv.Config
		if err = cfg.Open(fn); err != nil {
			t.Errorf("Open(%q): %v", fn, err)
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		sheets, err := cfg.ReadSheets(ctx)
		cancel()
		if err != nil {
			t.Errorf("ReadSheets(%q): %v", fn, err)
			continue
		}
		for cfg.Sheet = range sheets {
			ctx, cancel = context.WithTimeout(context.Background(), time.Minute)
			err = cfg.ReadRows(ctx, func(ctx context.Context, sheetName string, row dbcsv.Row) error {
				t.Log(sheetName, row)
				return nil
			})
			cancel()
			if err != nil {
				t.Errorf("ReadRows(%q): %v", fn, err)
			}
		}
	}
}

func TestCompressedTempCSV(t *testing.T) {
	stdr, stdw, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	oldStdin := os.Stdin
	os.Stdin = stdr
	defer func() { os.Stdin = oldStdin }()
	errCh := make(chan error, 1)
	go func() {
		defer stdw.Close()
		defer close(errCh)
		_, _ = stdw.Write([]byte("id;str\n"))
		for i := range 1000 {
			if _, err := fmt.Fprintf(stdw, "%d;árvíztűrő tükörfúrógép\n", i); err != nil {
				errCh <- err
				return
			}
		}
	}()

	var cfg dbcsv.Config
	if err = cfg.Open(""); err != nil {
		t.Fatal(err)
	}
	typ, err := cfg.Type()
	if err != nil {
		t.Fatal(err)
	}
	t.Log("type:", typ)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	for range 10 {
		if err := cfg.ReadRows(ctx, func(ctx context.Context, s string, r dbcsv.Row) error { t.Log(s, r); return nil }); err != nil {
			t.Error(err)
		}
	}
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
}

func TestReadDetectDelim(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	for _, tC := range []struct {
		Text string
		Want []dbcsv.Row
	}{
		{"HEADER_NO_DELIM\n123\n", []dbcsv.Row{
			{Columns: []string{"HEADER_NO_DELIM"}, Values: []string{"HEADER_NO_DELIM"}, Line: 0},
			{Columns: []string{"HEADER_NO_DELIM"}, Values: []string{"123"}, Line: 1},
		}},
		{`"COL1,";COL2` + "\na;b\n", []dbcsv.Row{
			{Columns: []string{"COL1,", "COL2"}, Values: []string{"COL1,", "COL2"}, Line: 0},
			{Columns: []string{"COL1,", "COL2"}, Values: []string{"a", "b"}, Line: 1},
		}},
	} {
		var i int
		if err := dbcsv.ReadCSV(ctx, func(ctx context.Context, r dbcsv.Row) error {
			if d := cmp.Diff(tC.Want[i], r); d != "" {
				t.Errorf("%d: %s", i, d)
			}
			i++
			return nil
		},
			strings.NewReader(tC.Text), "", nil, 0); err != nil {
			t.Fatal(err)
		}
	}
}
