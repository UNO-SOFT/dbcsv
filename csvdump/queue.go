// Copyright 2024 Tamás Gulácsi.
//
//
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"slices"
	"time"

	"github.com/godror/godror"
)

func dumpRemoteCSVQueue(ctx context.Context, w io.Writer, Q *godror.Queue, sep string) error {
	return remoteCSV(ctx, w, sep, queueNext(ctx, Q))
}

func queueNext(ctx context.Context, Q *godror.Queue) func() ([]byte, error) {
	var buf bytes.Buffer
	var data godror.Data
	messages := make([]godror.Message, 0, 16)
	var off int

	return func() ([]byte, error) {
		if off >= len(messages) {
			for {
				n, err := Q.Dequeue(messages[:])
				if err != nil {
					return nil, err
				}
				if n != 0 {
					messages = messages[:n]
					for i := 0; i < len(messages); i++ {
						if messages[i].Object == nil {
							messages = slices.Delete(messages, i, i+1)
						}
					}
					if len(messages) == 0 {
						continue
					}
					off = 0
					break
				}
				select {
				case <-time.After(time.Second):
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}
		}

		obj := messages[off].Object
		msgID := messages[off].MsgID
		off++
		if err := obj.GetAttribute(&data, "PAYLOAD"); err != nil {
			obj.Close()
			return nil, fmt.Errorf("%q.get BLOB: %w", msgID, err)
		}
		lob := data.GetLob()
		// This asserts that lob is a BLOB!
		size, err := lob.Size()
		if err != nil {
			obj.Close()
			return nil, fmt.Errorf("%q.getLOB: %w", msgID, err)
		}
		buf.Reset()
		_, err = io.Copy(&buf, io.LimitReader(lob, size))
		obj.Close()
		if err != nil && buf.Len() == 0 {
			return nil, err
		}

		payload := buf.Bytes()
		if bytes.Equal(payload, []byte("CLOSE")) {
			return nil, io.EOF
		}
		return payload, nil
	}
}
