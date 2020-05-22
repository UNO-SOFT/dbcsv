// Copyright 2020, Tamás Gulácsi.
//
// SPDX-License-Identifier: Apache-2.0

package dbcsv_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/UNO-SOFT/dbcsv"
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
			err = cfg.ReadRows(ctx, func(sheetName string, row dbcsv.Row) error {
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
