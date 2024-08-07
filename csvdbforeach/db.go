// Copyright 2020 Tamás Gulácsi.
//
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"database/sql"
	"encoding/csv"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/UNO-SOFT/dbcsv"
)

const (
	DateFormat     = "20060102"
	DateTimeFormat = "20060102150405"
)

func safeConvert(conv func(string) (interface{}, error), s string) (v interface{}, err error) {
	defer func() {
		if r := recover(); r != nil {
			var ok bool
			if err, ok = r.(error); !ok {
				err = fmt.Errorf("PANIC: %v", r)
			}
		}
	}()
	return conv(s)
}

func dbExec(db *sql.DB, fun string, fixParams [][2]string, retOk int64, rows <-chan dbcsv.Row, oneTx bool) (int, error) {
	st, err := getQuery(db, fun, fixParams)
	if err != nil {
		return 0, err
	}
	var (
		stmt     *sql.Stmt
		tx       *sql.Tx
		values   = make([]interface{}, 0, st.ParamCount)
		startIdx int
		ret      int64
		n        int
		buf      bytes.Buffer
	)
	defer func() {
		if tx != nil {
			tx.Rollback()
		}
	}()
	if st.Returns {
		values = append(values, &ret)
		startIdx = 1
	}

	for row := range rows {
		logger.Debug("dbExec", "row", row)
		if tx == nil {
			if tx, err = db.Begin(); err != nil {
				return n, err
			}
			if stmt != nil {
				stmt.Close()
			}
			if stmt, err = tx.Prepare(st.Qry); err != nil {
				tx.Rollback()
				return n, err
			}
		}

		if len(row.Values) > len(st.Converters) {
			logger.Warn("converter number mismatch", "values", len(row.Values), "converters", len(st.Converters), "params", st.ParamCount)
		}
		values = values[:startIdx]
		for i, s := range row.Values {
			conv := st.Converters[i]
			if conv == nil {
				values = append(values, s)
				continue
			}
			v, convErr := safeConvert(conv, s)
			if convErr != nil {
				logger.Error("convert", "row", row, "error", convErr)
				return n, fmt.Errorf("convert %q (row %d, col %d): %w", s, row.Line, i+1, convErr)
			}
			values = append(values, v)
		}
		for i := len(values) + 1; i < st.ParamCount-len(st.FixParams); i++ {
			values = append(values, "")
		}
		values = append(values, st.FixParams...)
		//log.Printf("%q %#v", st.Qry, values)
		logger.Info("Exec", "values", values)
		if _, err = stmt.Exec(values...); err != nil {
			logger.Error("execute", "qry", st.Qry, "line", row.Line, "values", values, "error", err)
			return n, fmt.Errorf("qry=%q params=%#v: %w", st.Qry, values, err)
		}
		n++
		if st.Returns && values[0] != nil {
			out := strings.Join(deref(st.FixParams), ", ")
			logger.Debug("returns", "out", out, "ret", ret, "retOk", retOk, "eq", ret == retOk)
			if ret == retOk {
				fmt.Fprintf(stdout, "%d: OK [%s]\t%s\n", ret, out, row.Values)
				continue
			}
			fmt.Fprintf(stderr, "%d: %s\t%s\n", ret, out, row.Values)
			logger.Warn("ROLLBACK", "ret", ret)
			tx.Rollback()
			tx = nil
			buf.Reset()
			cw := csv.NewWriter(&buf)
			_ = cw.Write(append([]string{fmt.Sprintf("%d", ret), out}, row.Values...))
			cw.Flush()
			stdout.Write(buf.Bytes())
			if oneTx {
				return n, fmt.Errorf("returned %v (%s) for line %d (%q)",
					ret, out, row.Line, row.Values)
			}
		}
		if tx != nil && !oneTx {
			logger.Info("COMMIT")
			if err = tx.Commit(); err != nil {
				return n, err
			}
			tx = nil
		}
	}
	if stmt != nil {
		stmt.Close()
	}
	if tx != nil {
		logger.Info("COMMIT")
		return n, tx.Commit()
	}
	return n, nil
}

type ConvFunc func(string) (interface{}, error)

type Statement struct {
	Qry        string
	Converters []ConvFunc
	FixParams  []interface{}
	ParamCount int
	Returns    bool
}

type querier interface {
	Query(string, ...interface{}) (*sql.Rows, error)
}

func getQuery(db querier, fun string, fixParams [][2]string) (Statement, error) {
	var st Statement
	args := make([]Arg, 0, 32)
	fun = strings.TrimSpace(fun)

	if strings.HasPrefix(fun, "BEGIN ") && strings.HasSuffix(fun, "END;") {
		st.Qry = fun
		if i := strings.IndexByte(fun, '('); i >= 0 && strings.Contains(fun[5:i], ":=") { //function
			st.Returns = true
		}
		var nm []byte
		var state uint8
		names := make([]string, 0, strings.Count(fun, ":"))
		_ = strings.Map(func(r rune) rune {
			switch state {
			case 0:
				if r == ':' {
					state = 1
					nm = nm[:0]
				}
			case 1:
				if 'a' <= r && r <= 'z' || 'A' <= r && r <= 'Z' ||
					'0' <= r && r <= '9' ||
					len(nm) > 0 && r == '_' {
					nm = append(nm, byte(r))
				} else {
					names = append(names, string(nm))
					nm = nm[:0]
					state = 0
				}
			}
			return -1
		},
			fun)
		if len(nm) > 0 {
			names = append(names, string(nm))
		}
		st.ParamCount = len(names)
		st.Converters = make([]ConvFunc, len(names))
		return st, nil
	}

	parts := strings.Split(fun, ".")
	qry := "SELECT argument_name, data_type, in_out, data_length, data_precision, data_scale FROM "
	params := make([]interface{}, 0, 3)
	switch len(parts) {
	case 1:
		qry += "all_arguments WHERE owner = SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA') AND object_name = UPPER(:1)"
		params = append(params, fun)
	case 2:
		qry += "all_arguments WHERE owner = SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA') AND package_name = UPPER(:1) AND object_name = UPPER(:2)"
		params = append(params, parts[0], parts[1])
	case 3:
		qry += "all_arguments WHERE owner = UPPER(:1) AND package_name = UPPER(:2) AND object_name = UPPER(:3)"
		params = append(params, parts[0], parts[1], parts[2])
	default:
		return st, fmt.Errorf("bad function name: %s", fun)
	}
	qry += " ORDER BY sequence"
	rows, err := db.Query(qry, params...)
	if err != nil {
		return st, fmt.Errorf("%s: %w", qry, err)
	}
	defer rows.Close()

	for rows.Next() {
		var arg Arg
		var length, precision, scale sql.NullInt64
		if err = rows.Scan(&arg.Name, &arg.Type, &arg.InOut, &length, &precision, &scale); err != nil {
			return st, err
		}
		if length.Valid {
			arg.Length = int(length.Int64)
			if precision.Valid {
				arg.Precision = int(precision.Int64)
				if scale.Valid {
					arg.Scale = int(scale.Int64)
				}
			}
		}
		args = append(args, arg)
	}
	if err = rows.Err(); err != nil {
		return st, fmt.Errorf("%s: %w", qry, err)
	}
	if len(args) == 0 {
		return st, fmt.Errorf("%s has no arguments", fun)
	}

	st.Qry = "BEGIN "
	i := 1
	if args[0].Name == "" { // function
		st.Qry += ":x1 := "
		args = args[1:]
		st.Returns = true
		i++
	}
	fixParamNames := make([]string, len(fixParams))
	for j, x := range fixParams {
		fixParamNames[j] = strings.ToUpper(x[0])
	}
	vals := make([]string, 0, len(args))
	st.Converters = make([]ConvFunc, cap(vals))
ArgLoop:
	for j, arg := range args {
		for _, x := range fixParamNames {
			if x == arg.Name {
				continue ArgLoop
			}
		}
		vals = append(vals, fmt.Sprintf("%s=>:x%d", strings.ToLower(arg.Name), i))
		if arg.InOut == "OUT" {
			switch arg.Type {
			case "DATE":
				var t sql.NullTime
				st.FixParams = append(st.FixParams, sql.Out{Dest: &t})
			case "NUMBER":
				var f float64
				st.FixParams = append(st.FixParams, sql.Out{Dest: &f})
			default:
				var s string
				st.FixParams = append(st.FixParams, sql.Out{Dest: &s})
			}
		} else if arg.Type == "DATE" {
			st.Converters[j] = strToDate
		}
		i++
	}
	for _, p := range fixParams {
		vals = append(vals, fmt.Sprintf("%s=>:x%d", p[0], i))
		st.FixParams = append(st.FixParams, p[1])
		i++
	}
	st.ParamCount = i
	st.Qry += fun + "(" + strings.Join(vals, ", ") + "); END;"
	return st, err
}

type Arg struct {
	Name, Type, InOut        string
	Length, Precision, Scale int
}

func strToDate(s string) (interface{}, error) {
	if justNums(s, 14) == "" {
		return nil, nil
	}
	var buf strings.Builder
	buf.Grow(14)
	fields := strings.FieldsFunc(s, func(r rune) bool { return !('0' <= r && r <= '9') })
	if len(fields) >= 3 && len(fields[0]) < 4 && len(fields[2]) == 4 { // mm/dd/yyyy
		fields[0], fields[1], fields[2] = fields[2], fields[0], fields[1]
	}
	for i, f := range fields {
		reqLen := 2
		if i == 0 {
			reqLen = 4
		}
		for j := reqLen - len(f); j > 0; j-- {
			buf.WriteByte('0')
		}
		buf.WriteString(f)
	}
	s = buf.String()
	if len(s) > 14 {
		s = s[:14]
	}
	format := DateTimeFormat
	if length := len(s); length < len(format) {
		if length < len(DateFormat) {
			return sql.NullTime{}, fmt.Errorf("date %q too short", s)
		} else if length > len(DateFormat) {
			s = s[:8]
		}
		format = DateFormat
	}
	t, err := time.ParseInLocation(format, s, time.Local)
	if err != nil {
		return sql.NullTime{}, err
	}
	return sql.NullTime{Valid: !t.IsZero(), Time: t}, nil
}
func justNums(s string, maxLen int) string {
	var i int
	return strings.Map(
		func(r rune) rune {
			if maxLen >= 0 {
				if i > maxLen {
					return -1
				}
			}
			if '0' <= r && r <= '9' {
				i++
				return r
			}
			return -1
		},
		s)
}

func deref(in []interface{}) []string {
	out := make([]string, 0, len(in))
	for _, v := range in {
		if v == nil {
			out = append(out, "")
			continue
		}
		switch x := v.(type) {
		case *string:
			out = append(out, *x)
		case *int64:
			out = append(out, strconv.FormatInt(*x, 10))
		case *float64:
			out = append(out, fmt.Sprintf("%f", *x))
		case *sql.NullTime:
			if x.Valid {
				out = append(out, x.Time.Format("2006-01-02"))
			} else {
				out = append(out, "")
			}
		case *time.Time:
			out = append(out, x.Format("2006-01-02"))
		default:
			rv := reflect.ValueOf(v)
			if rv.Kind() != reflect.Ptr {
				continue
			}
			out = append(out, fmt.Sprintf("%v", rv.Elem().Interface()))
		}
	}
	return out
}

// vim: set fileencoding=utf-8 noet:
