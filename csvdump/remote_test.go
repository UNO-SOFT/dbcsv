// Copyright 2023 Tamás Gulácsi.
//
//
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"io"
	"os"
	"testing"
	"time"

	"github.com/UNO-SOFT/zlog/v2"
)

func TestRemote(t *testing.T) {
	logger = zlog.NewT(t).SLog()
	ctx := zlog.NewSContext(context.Background(), logger)
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	var buf bytes.Buffer
	var pos int
	commands := []string{
		`{"c":"newSheet", "a":[{"s":"s"}]}`,
		`{"c":"insertPageBreak", "a":[{"s":"s"},{"s":"A100"}]}`,
		`{"c":"mergeCell", "a":[{"s":"s"}, {"s":"A1"}, {"s":"B1"}]}`,
		`{"c":"setCell","a":[{"s":"s"},{"s":"A2"},{"t":"f","f":3.14}]}`,
		`{"c":"newStyle","a":[{"s":"header"},{"t":"r","r":` + "{\"Font\":{\"Bold\":true,\"Size\":16},\"Alignment\":{\"Horizontal\":\"center\",\"WrapText\":true}}" + `}]}`,
	}
	if err := executeCommands(ctx, &buf, func() (string, error) {
		if pos >= len(commands) {
			return "", io.EOF
		}
		pos++
		return commands[pos-1], nil
	}); err != nil {
		t.Fatal(err)
	}
	if buf.Len() == 0 {
		t.Fatal("got 0 bytes")
	}
	if err := os.WriteFile("/tmp/remote.xlsx", buf.Bytes(), 0600); err != nil {
		t.Error(err)
	}
}
