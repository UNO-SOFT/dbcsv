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

	"github.com/UNO-SOFT/spreadsheet"
)

func DumpCSV(ctx context.Context, w io.Writer, rows *sql.Rows, columns []Column, header bool, sep string, raw bool, Log func(...interface{}) error) error {
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
	if Log != nil {
		_ = Log("msg", "dump finished", "rows", n, "dur", dur.String(), "speed", fmt.Sprintf("%.3f 1/s", float64(n)/float64(dur*time.Second)), "error", err)
	}
	return err
}

func DumpSheet(ctx context.Context, sheet spreadsheet.Sheet, rows *sql.Rows, columns []Column, Log func(...interface{}) error) error {
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
		if err := sheet.AppendRow(vals...); err != nil {
			return err
		}
		n++
	}
	err := rows.Err()
	dur := time.Since(start)
	if Log != nil {
		_ = Log("msg", "dump finished", "rows", n, "dur", dur, "speed", float64(n)/float64(dur)*float64(time.Second), "error", err)
	}
	return err
}

type Column struct {
	reflect.Type
	Name string
}

func (col Column) Converter(sep string) Stringer {
	return getColConverter(col.Type, sep)
}

type Stringer interface {
	String() string
	Pointer() interface{}
	Scan(interface{}) error
}

type ValString struct {
	Sep   string
	Value sql.NullString
}

func (v ValString) String() string            { return csvQuoteString(v.Sep, v.Value.String) }
func (v ValString) StringRaw() string         { return v.Value.String }
func (v *ValString) Pointer() interface{}     { return &v.Value }
func (v *ValString) Scan(x interface{}) error { return v.Value.Scan(x) }

type ValInt struct {
	Value sql.NullInt64
}

func (v ValInt) String() string {
	if v.Value.Valid {
		return strconv.FormatInt(v.Value.Int64, 10)
	}
	return ""
}
func (v *ValInt) Pointer() interface{}     { return &v.Value }
func (v *ValInt) Scan(x interface{}) error { return v.Value.Scan(x) }

type ValFloat struct {
	Value sql.NullFloat64
}

func (v ValFloat) String() string {
	if v.Value.Valid {
		return strconv.FormatFloat(v.Value.Float64, 'f', -1, 64)
	}
	return ""
}
func (v *ValFloat) Pointer() interface{}     { return &v.Value }
func (v *ValFloat) Scan(x interface{}) error { return v.Value.Scan(x) }

type ValTime struct {
	Value sql.NullTime
	Quote bool
}

var (
	DateEnd    string
	DateFormat = "2006-01-02"
)

func (v ValTime) String() string {
	if !v.Value.Valid || v.Value.Time.IsZero() {
		return ""
	}
	if v.Value.Time.Year() < 0 {
		return DateEnd
	}
	if v.Quote {
		return `"` + v.Value.Time.Format(DateFormat) + `"`
	}
	return v.Value.Time.Format(DateFormat)
}
func (v ValTime) StringRaw() string {
	if !v.Value.Valid || v.Value.Time.IsZero() {
		return ""
	}
	if v.Value.Time.Year() < 0 {
		return DateEnd
	}
	return v.Value.Time.Format(DateFormat)
}

func (vt ValTime) ConvertValue(v interface{}) (driver.Value, error) {
	if v == nil {
		return sql.NullTime{}, nil
	}
	switch v := v.(type) {
	case sql.NullTime:
		return v, nil
	case time.Time:
		return sql.NullTime{Valid: !v.IsZero(), Time: v}, nil
	}
	return nil, fmt.Errorf("unknown value %T", v)
}
func (vt *ValTime) Scan(v interface{}) error {
	if v == nil {
		vt.Value = sql.NullTime{}
		return nil
	}
	switch v := v.(type) {
	case sql.NullTime:
		vt.Value = v
	case time.Time:
		vt.Value = sql.NullTime{Valid: !v.IsZero(), Time: v}
	default:
		return fmt.Errorf("unknown scan source %T", v)
	}
	return nil
}
func (v *ValTime) Pointer() interface{} { return v }

var typeOfTime, typeOfNullTime = reflect.TypeOf(time.Time{}), reflect.TypeOf(sql.NullTime{})

func getColConverter(typ reflect.Type, sep string) Stringer {
	switch typ.Kind() {
	case reflect.String:
		return &ValString{Sep: sep}
	case reflect.Float32, reflect.Float64:
		return &ValFloat{}
	case reflect.Int32, reflect.Int64, reflect.Int:
		return &ValInt{}
	}
	switch typ {
	case typeOfTime, typeOfNullTime:
		return &ValTime{Quote: sep != "" && strings.Contains(DateFormat, sep)}
	}
	return &ValString{Sep: sep}
}

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

func GetColumns(rows interface{}) ([]Column, error) {
	if r, ok := rows.(*sql.Rows); ok {
		types, err := r.ColumnTypes()
		if err != nil {
			return nil, err
		}
		cols := make([]Column, len(types))
		for i, t := range types {
			cols[i] = Column{Name: t.Name(), Type: t.ScanType()}
		}
		return cols, nil
	}

	colNames := rows.(driver.Rows).Columns()
	cols := make([]Column, len(colNames))
	r := rows.(driver.RowsColumnTypeScanType)
	for i, name := range colNames {
		cols[i] = Column{
			Name: name,
			Type: r.ColumnTypeScanType(i),
		}
	}
	return cols, nil
}
