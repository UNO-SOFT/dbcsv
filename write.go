// Copyright 2020, 2021 Tamás Gulácsi.
//
// SPDX-License-Identifier: Apache-2.0

package dbcsv

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/exp/slog"

	"github.com/UNO-SOFT/spreadsheet"
	"github.com/UNO-SOFT/zlog/v2"

	"github.com/godror/godror"
)

func DumpCSV(ctx context.Context, w io.Writer, rows *sql.Rows, columns []Column, header bool, sep string, raw bool) error {
	logger := zlog.SFromContext(ctx)
	sepB := []byte(sep)
	dest := make([]interface{}, len(columns))
	bw := bufio.NewWriterSize(w, 65536)
	defer bw.Flush()
	values := make([]Stringer, len(columns))
	for i, col := range columns {
		c := col.Converter(sep)
		values[i] = c
		dest[i] = c.Pointer()
	}
	if header && !raw {
		for i, col := range columns {
			if i > 0 {
				_, _ = bw.Write(sepB)
			}
			if _, err := csvQuote(bw, sep, col.Name); err != nil {
				return err
			}
		}
		if _, err := bw.Write([]byte{'\n'}); err != nil {
			return err
		}
	}

	start := time.Now()
	n := 0
	for rows.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := rows.Scan(dest...); err != nil {
			return fmt.Errorf("scan into %#v: %w", dest, err)
		}
		if raw {
			for i, data := range dest {
				if data == nil {
					continue
				}
				if sr, ok := values[i].(interface{ StringRaw() string }); ok {
					_, _ = bw.WriteString(sr.StringRaw())
				} else {
					_, _ = bw.WriteString(values[i].String())
				}
			}
		} else {
			for i, data := range dest {
				if i > 0 {
					_, _ = bw.Write(sepB)
				}
				if data == nil {
					continue
				}
				_, _ = bw.WriteString(values[i].String())
			}
		}
		if _, err := bw.Write([]byte{'\n'}); err != nil {
			return err
		}
		n++
	}
	err := rows.Err()
	dur := time.Since(start)
	logger.Debug("dump finished", "rows", n, "dur", dur.String(), "speed", fmt.Sprintf("%.3f 1/s", float64(n)/float64(dur*time.Second)), "error", err)
	return err
}

func DumpSheet(ctx context.Context, sheet spreadsheet.Sheet, rows *sql.Rows, columns []Column) error {
	logger := zlog.SFromContext(ctx)
	dest := make([]interface{}, len(columns))
	vals := make([]interface{}, len(columns))
	values := make([]Stringer, len(columns))
	for i, col := range columns {
		c := col.Converter("")
		values[i] = c
		vals[i] = c
		dest[i] = c.Pointer()
	}
	start := time.Now()
	n := 0
	for rows.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := rows.Scan(dest...); err != nil {
			return fmt.Errorf("scan into %#v: %w", dest, err)
		}
		if logger.Enabled(ctx, slog.LevelDebug) {
			logger.Debug("scan", "rows", dest, "vals", fmt.Sprintf("%#v", vals))
		}
		if err := sheet.AppendRow(vals...); err != nil {
			return err
		}
		n++
	}
	err := rows.Err()
	dur := time.Since(start)
	logger.Debug("dump finished", "rows", n, "dur", dur.String(), "speed", float64(n)/float64(dur)*float64(time.Second), "error", err)
	return err
}

type Column struct {
	reflect.Type
	Name, DatabaseType string
	Precision, Scale   int
}

func (col Column) Converter(sep string) Stringer {
	switch col.Type.Kind() {
	case reflect.Float32, reflect.Float64:
		return &ValFloat{}
	case reflect.Int32, reflect.Int64, reflect.Int:
		return &ValInt{}
	case reflect.String:
		if col.DatabaseType == "NUMBER" && !(col.Precision == 0 && col.Scale == 0) {
			if col.Scale == 0 && col.Precision <= 19 {
				return &ValInt{}
			}
			return &ValFloat{}
		}
		return &ValNumber{Sep: sep}
	}

	switch col.Type {
	case typeOfByteSlice:
		return &ValBytes{Sep: sep}
	case typeOfTime, typeOfNullTime:
		return &ValTime{Quote: sep != "" && strings.Contains(DateFormat, sep)}
	}
	return &ValString{Sep: sep}
}

type Stringer interface {
	String() string
	Pointer() interface{}
	sql.Scanner
	driver.Valuer
}
type ValNumber struct {
	Sep   string
	value godror.Number
}

func (v ValNumber) Value() (driver.Value, error) { return spreadsheet.Number(v.value), nil }
func (v ValNumber) String() string               { return csvQuoteString(v.Sep, string(v.value)) }
func (v ValNumber) StringRaw() string            { return string(v.value) }
func (v *ValNumber) Pointer() interface{}        { return &v.value }
func (v *ValNumber) Scan(x interface{}) error    { return v.value.Scan(x) }

type ValString struct {
	Sep   string
	value sql.NullString
}

func (v ValString) Value() (driver.Value, error) { return v.value, nil }
func (v ValString) String() string               { return csvQuoteString(v.Sep, v.value.String) }
func (v ValString) StringRaw() string            { return v.value.String }
func (v *ValString) Pointer() interface{}        { return &v.value }
func (v *ValString) Scan(x interface{}) error    { return v.value.Scan(x) }

type ValBytes struct {
	Sep   string
	value []byte
}

func (v ValBytes) Value() (driver.Value, error) { return v.value, nil }
func (v ValBytes) String() string               { return csvQuoteString(v.Sep, fmt.Sprintf("%x", v.value)) }
func (v ValBytes) StringRaw() string            { return fmt.Sprintf("%x", v.value) }
func (v *ValBytes) Pointer() interface{}        { return &v.value }
func (v *ValBytes) Scan(x interface{}) error {
	if x == nil {
		v.value = nil
		return nil
	}
	switch x := x.(type) {
	case []byte:
		v.value = x
	case string:
		v.value = []byte(x)
	default:
		return fmt.Errorf("unknown scan source %T", x)
	}
	return nil
}

type ValInt struct {
	value sql.NullInt64
}

func (v ValInt) Value() (driver.Value, error) { return v.value, nil }
func (v ValInt) String() string {
	if v.value.Valid {
		return strconv.FormatInt(v.value.Int64, 10)
	}
	return ""
}
func (v *ValInt) Pointer() interface{}     { return &v.value }
func (v *ValInt) Scan(x interface{}) error { return v.value.Scan(x) }

type ValFloat struct {
	value sql.NullFloat64
}

func (v ValFloat) Value() (driver.Value, error) { return v.value, nil }
func (v ValFloat) String() string {
	if v.value.Valid {
		return strconv.FormatFloat(v.value.Float64, 'f', -1, 64)
	}
	return ""
}
func (v *ValFloat) Pointer() interface{}     { return &v.value }
func (v *ValFloat) Scan(x interface{}) error { return v.value.Scan(x) }

type ValTime struct {
	value sql.NullTime
	Quote bool
}

var (
	DateEnd    string
	DateFormat = "2006-01-02"
)

func (v ValTime) Value() (driver.Value, error) { return v.value, nil }
func (v ValTime) String() string {
	if !v.value.Valid || v.value.Time.IsZero() {
		return ""
	}
	if v.value.Time.Year() < 0 {
		return DateEnd
	}
	if v.Quote {
		return `"` + v.value.Time.Format(DateFormat) + `"`
	}
	return v.value.Time.Format(DateFormat)
}
func (v ValTime) StringRaw() string {
	if !v.value.Valid || v.value.Time.IsZero() {
		return ""
	}
	if v.value.Time.Year() < 0 {
		return DateEnd
	}
	return v.value.Time.Format(DateFormat)
}

func (vt *ValTime) Scan(v interface{}) error {
	if v == nil {
		vt.value = sql.NullTime{}
		return nil
	}
	switch v := v.(type) {
	case sql.NullTime:
		vt.value = v
	case time.Time:
		vt.value = sql.NullTime{Valid: !v.IsZero(), Time: v}
	default:
		return fmt.Errorf("unknown scan source %T", v)
	}
	return nil
}
func (v *ValTime) Pointer() interface{} { return v }

var typeOfTime, typeOfNullTime, typeOfByteSlice = reflect.TypeOf(time.Time{}), reflect.TypeOf(sql.NullTime{}), reflect.TypeOf(([]byte)(nil))

var bufPool = sync.Pool{New: func() interface{} { return bytes.NewBuffer(make([]byte, 0, 1024)) }}

func csvQuoteString(sep, s string) string {
	if sep == "" {
		return s
	}
	buf := bufPool.Get().(*bytes.Buffer)
	defer bufPool.Put(buf)
	buf.Reset()
	if _, err := csvQuote(buf, sep, s); err != nil {
		panic(fmt.Errorf("csvQuote %q: %w", s, err))
	}
	return buf.String()
}

func csvQuote(w io.Writer, sep, s string) (int, error) {
	needQuote := strings.Contains(s, sep) || strings.ContainsAny(s, `"`+"\n")
	if !needQuote {
		return io.WriteString(w, s)
	}
	n, err := w.Write([]byte{'"'})
	if err != nil {
		return n, err
	}
	m, err := io.WriteString(w, strings.Replace(s, `"`, `""`, -1))
	n += m
	if err != nil {
		return n, err
	}
	m, err = w.Write([]byte{'"'})
	return n + m, err
}

func GetColumns(ctx context.Context, rows interface{}) ([]Column, error) {
	logger := zlog.SFromContext(ctx)
	if r, ok := rows.(*sql.Rows); ok {
		types, err := r.ColumnTypes()
		if err != nil {
			return nil, err
		}
		cols := make([]Column, len(types))
		for i, t := range types {
			precision, scale, _ := t.DecimalSize()
			cols[i] = Column{
				Name:         t.Name(),
				DatabaseType: t.DatabaseTypeName(),
				Type:         t.ScanType(),
				Precision:    int(precision), Scale: int(scale),
			}
			logger.Debug("column", "i", i, "t", fmt.Sprintf("%#v", t), "col", cols[i])
		}
		return cols, nil
	}

	colNames := rows.(driver.Rows).Columns()
	cols := make([]Column, len(colNames))
	st := rows.(driver.RowsColumnTypeScanType)
	dtn := rows.(driver.RowsColumnTypeDatabaseTypeName)
	ps := rows.(driver.RowsColumnTypePrecisionScale)
	for i, name := range colNames {
		precision, scale, _ := ps.ColumnTypePrecisionScale(i)
		cols[i] = Column{
			Name:         name,
			DatabaseType: dtn.ColumnTypeDatabaseTypeName(i),
			Type:         st.ColumnTypeScanType(i),
			Precision:    int(precision), Scale: int(scale),
		}
	}
	return cols, nil
}
