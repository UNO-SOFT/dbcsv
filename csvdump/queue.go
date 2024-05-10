// Copyright 2024 Tamás Gulácsi.
//
//
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"io"
	"slices"
	"strings"
	"time"

	"github.com/godror/godror"
)

func dumpRemoteCSVQueue(ctx context.Context, w io.Writer, Q *godror.Queue, sep string) error {
	return remoteCSV(ctx, w, sep, queueNext(ctx, Q))
}

func queueNext(ctx context.Context, Q *godror.Queue) func() ([]byte, error) {
	var buf bytes.Buffer
	var data godror.Data
	messages := make([]godror.Message, 16)
	off := len(messages)

	return func() ([]byte, error) {
		if off >= len(messages) {
			for {
				n, err := Q.Dequeue(messages[:])
				logger.Debug("Dequeue", "n", n, "error", err)
				if err != nil {
					return nil, err
				}
				if n != 0 {
					messages = messages[:n]
					for i := 0; i < len(messages); i++ {
						if messages[i].Object == nil {
							logger.Warn("nil object", "i", i)
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
		corrID := messages[off].Correlation
		off++
		logger := logger.With("msgID", msgID)
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
		logger.Debug("payload", "length", size, "payload", payload, "corrid", corrID)
		if bytes.Equal(payload, []byte("CLOSE")) {
			return nil, io.EOF
		}
		return payload, nil
	}
}

func (Q *Query) ParseQueue() {
	cut := func(s string) (prefix, suffix string, found bool) {
		const sepChars = "/"
		i := strings.IndexAny(s, sepChars)
		if i < 0 {
			return s, "", false
		}
		return s[:i], strings.TrimLeftFunc(s[i+1:], func(r rune) bool { return strings.ContainsRune(sepChars, r) }), true
	}
	Q.QueueName, Q.Correlation, _ = cut(Q.Query)
	logger.Debug("ParseQueue", "src", Q.Query, "name", Q.QueueName, "correlation", Q.Correlation)
}

func (Q *Query) OpenQueue(ctx context.Context, db interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
	godror.Execer
}) (*godror.Queue, error) {
	const qry = `SELECT B.object_type FROM user_queue_tables B, user_queues A WHERE B.queue_table = A.queue_table AND A.NAME = UPPER(:1)`
	var typeName string
	if err := db.QueryRowContext(ctx, qry, Q.QueueName).Scan(&typeName); err != nil {
		return nil, fmt.Errorf("%s [%q]: %w", qry, Q.QueueName, err)
	}
	logger.Debug("NewQueue", "name", Q.QueueName, "type", typeName, "correlation", Q.Correlation)
	return godror.NewQueue(ctx, db, Q.QueueName, typeName, godror.WithDeqOptions(godror.DeqOptions{
		Mode:        godror.DeqRemove,
		Navigation:  godror.NavFirst,
		Visibility:  godror.VisibleImmediate,
		Correlation: Q.Correlation,
		Wait:        time.Second,
	}))
}
