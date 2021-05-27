// Copyright 2020, 2021 Tamás Gulácsi.
//
// SPDX-License-Identifier: Apache-2.0

package dbcsv

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
	"unicode"

	"golang.org/x/text/encoding"
	"golang.org/x/text/encoding/charmap"
	"golang.org/x/text/encoding/htmlindex"
	"golang.org/x/text/transform"

	"github.com/360EntSecGroup-Skylar/excelize/v2"
	"github.com/extrame/xls"
	"github.com/klauspost/compress/zstd"
)

var DefaultEncoding = NamedEncoding{Encoding: encoding.Replacement, Name: "utf-8"}

var ErrUnknownSheet = errors.New("unknown sheet")

type NamedEncoding struct {
	encoding.Encoding
	Name string
}

func init() {
	encName := os.Getenv("LANG")
	if i := strings.IndexByte(encName, '.'); i >= 0 {
		if enc, err := EncFromName(encName[i+1:]); err == nil {
			DefaultEncoding = enc
		}
	}
}
func EncFromName(e string) (NamedEncoding, error) {
	switch strings.NewReplacer("-", "", "_", "").Replace(strings.ToLower(e)) {
	case "", "utf8":
		return NamedEncoding{Encoding: encoding.Nop, Name: "utf-8"}, nil
	case "iso88591":
		return NamedEncoding{Encoding: charmap.ISO8859_1, Name: "iso-8859-1"}, nil
	case "iso88592":
		return NamedEncoding{Encoding: charmap.ISO8859_2, Name: "iso-8859-2"}, nil
	}
	if enc, err := htmlindex.Get(e); err == nil {
		return NamedEncoding{Encoding: enc, Name: e}, nil
	}
	return NamedEncoding{Encoding: encoding.Nop, Name: e}, fmt.Errorf("%s: %w", e, errors.New("unknown encoding"))
}

type FType string

type FileType struct {
	Type        FType
	Compression FType
}

const (
	Unknown = FType("")
	Csv     = FType("csv")
	Xls     = FType("xls")
	XlsX    = FType("xlsx")
	Gzip    = FType("gzip")
	Zstd    = FType("zstd")
)

func DetectReaderType(r io.Reader, fileName string) (FileType, error) {
	// detect file type
	var b [4]byte
	var buf bytes.Buffer
	if _, err := io.ReadFull(io.TeeReader(r, &buf), b[:]); err != nil {
		return FileType{Type: Unknown}, err
	}
	if bytes.Equal(b[:], []byte{0xd0, 0xcf, 0x11, 0xe0}) { // OLE2
		return FileType{Type: Xls}, nil
	} else if bytes.Equal(b[:], []byte{0x50, 0x4b, 0x03, 0x04}) { //PKZip, so xlsx
		return FileType{Type: XlsX}, nil
	}
	if bytes.Equal(b[:3], []byte{0x1f, 0x8b, 0x8}) { // GZIP
		zr, err := gzip.NewReader(io.MultiReader(bytes.NewReader(buf.Bytes()), r))
		if err != nil {
			return FileType{Type: Csv}, nil
		}
		sub, err := DetectReaderType(zr, fileName)
		zr.Close()
		sub.Compression = Gzip
		return sub, err
	}
	if bytes.Equal(b[:], []byte{0x28, 0xb5, 0x2f, 0xfd}) { // Zstd
		zr, err := zstd.NewReader(io.MultiReader(bytes.NewReader(buf.Bytes()), r))
		if err != nil {
			return FileType{Type: Csv}, nil
		}
		sub, err := DetectReaderType(zr, fileName)
		zr.Close()
		sub.Compression = Zstd
		return sub, err
	}
	// CSV
	return FileType{Type: Csv}, nil
}

type Config struct {
	rdr           io.ReadCloser
	encoding      encoding.Encoding
	file          *os.File
	zr            *zstd.Decoder
	typ           FileType
	Delim         string
	Charset       string
	ColumnsString string
	fileName      string
	columns       []int
	Sheet, Skip   int
}

func (cfg *Config) Encoding() (encoding.Encoding, error) {
	if cfg.encoding != nil {
		return cfg.encoding, nil
	}
	if cfg.Charset == "" {
		return DefaultEncoding, nil
	}
	var err error
	cfg.encoding, err = htmlindex.Get(cfg.Charset)
	return cfg.encoding, err
}

func (cfg *Config) Columns() ([]int, error) {
	err := cfg.parseColumnsString()
	return cfg.columns, err
}
func (cfg *Config) Rewind() error {
	if cfg.zr != nil {
		cfg.zr.Close()
	}
	_, err := cfg.file.Seek(0, 0)
	if err != nil {
		return err
	}
	if cfg.zr != nil {
		if cfg.zr, err = zstd.NewReader(cfg.file); err != nil {
			return err
		}
		cfg.rdr = cfg.zr.IOReadCloser()
	}
	return err
}

func (cfg *Config) Type() (FileType, error) {
	if cfg.typ.Type != Unknown {
		return cfg.typ, nil
	}
	var err error
	cfg.typ, err = DetectReaderType(cfg.file, cfg.fileName)
	if err == nil {
		err = cfg.Rewind()
	}
	return cfg.typ, err
}

func (cfg *Config) Open(fileName string) error {
	slurp := fileName == "-" || fileName == ""
	if slurp {
		cfg.file, fileName = os.Stdin, "-"
	} else {
		var err error
		if cfg.file, err = os.Open(fileName); err != nil {
			return fmt.Errorf("open %s: %w", fileName, err)
		}
		fi, err := cfg.file.Stat()
		if err != nil {
			cfg.file.Close()
			return fmt.Errorf("stat %s: %w", fileName, err)
		}
		slurp = !fi.Mode().IsRegular()
	}
	var err error
	var buf bytes.Buffer
	r := io.Reader(cfg.file)
	if cfg.typ, err = DetectReaderType(io.TeeReader(r, &buf), cfg.fileName); err != nil {
		return err
	}
	r = io.MultiReader(bytes.NewReader(buf.Bytes()), r)

	if cfg.typ.Compression != "" {
		if cfg.typ.Compression == Gzip {
			if r, err = gzip.NewReader(r); err != nil {
				return err
			}
		} else if cfg.typ.Compression == Zstd {
			if r, err = zstd.NewReader(r); err != nil {
				return err
			}
		}
		slurp = true
	}

	var fh *os.File
	if slurp {
		var tmpErr error
		if fh, tmpErr = ioutil.TempFile("", "ReadRows-"); tmpErr != nil {
			return tmpErr
		}
		defer fh.Close()
		fileName = fh.Name()
		defer os.Remove(fileName)

		compress := cfg.typ.Type == Csv
		w := io.WriteCloser(fh)
		if compress {
			if w, err = zstd.NewWriter(fh); err != nil {
				return err
			}
		}

		log.Printf("Copying into temporary file %q...", fh.Name())
		if _, err = io.Copy(w, r); err != nil {
			return err
		}
		if compress {
			if err = w.Close(); err != nil {
				return err
			}
		}
		if err = fh.Sync(); err != nil {
			return err
		}
		if err = fh.Close(); err != nil {
			return err
		}
		if cfg.file, err = os.Open(fh.Name()); err != nil {
			return err
		}
		os.Remove(fh.Name())
		if compress {
			if cfg.zr, err = zstd.NewReader(cfg.file); err != nil {
				return err
			}
			cfg.rdr = cfg.zr.IOReadCloser()
		}
	}
	cfg.fileName = fileName
	if cfg.rdr == nil {
		cfg.rdr = cfg.file
	}
	_, err = cfg.Type()
	if err != nil {
		return fmt.Errorf("type %s: %w", cfg.fileName, err)
	}
	return nil
}

func (cfg *Config) Close() error {
	zr, rdr, fh := cfg.zr, cfg.rdr, cfg.file
	cfg.zr, cfg.rdr, cfg.file, cfg.fileName, cfg.typ = nil, nil, nil, "", FileType{Type: Unknown}
	var err error
	if zr != nil {
		zr.Close()
	}
	if rdr != nil {
		err = rdr.Close()
	}
	if fh != nil && rdr != fh {
		err = fh.Close()
	}
	return err
}

func (cfg *Config) ReadRows(ctx context.Context, fn func(string, Row) error) (err error) {
	if err = ctx.Err(); err != nil {
		return err
	}
	if err := cfg.parseColumnsString(); err != nil {
		return err
	}

	if err := cfg.Rewind(); err != nil {
		return err
	}
	switch cfg.typ.Type {
	case Xls:
		return ReadXLSFile(ctx, fn, cfg.fileName, cfg.Charset, cfg.Sheet, cfg.columns, cfg.Skip)
	case XlsX:
		return ReadXLSXFile(ctx, fn, cfg.fileName, cfg.Sheet, cfg.columns, cfg.Skip)
	}
	enc, err := cfg.Encoding()
	if err != nil {
		return err
	}
	r := transform.NewReader(cfg.rdr, enc.NewDecoder())
	return ReadCSV(ctx, func(row Row) error { return fn(cfg.fileName, row) }, r, cfg.Delim, cfg.columns, cfg.Skip)
}
func (cfg *Config) parseColumnsString() error {
	if cfg.columns != nil || cfg.ColumnsString == "" {
		return nil
	}

	cfg.columns = make([]int, 0, strings.Count(cfg.ColumnsString, ",")+1)
	for _, x := range strings.Split(cfg.ColumnsString, ",") {
		i, err := strconv.Atoi(x)
		if err != nil {
			return fmt.Errorf("%s: %w", x, err)
		}
		cfg.columns = append(cfg.columns, i-1)
	}
	return nil
}

func (cfg *Config) ReadSheets(ctx context.Context) (map[int]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := cfg.Rewind(); err != nil {
		return nil, err
	}
	switch cfg.typ.Type {
	case Xls:
		wb, err := xls.Open(cfg.fileName, cfg.Charset)
		if err != nil {
			return nil, fmt.Errorf("open %q: %w", cfg.fileName, err)
		}
		n := wb.NumSheets()
		m := make(map[int]string, n)
		for i := 0; i < n; i++ {
			m[i] = wb.GetSheet(i).Name
		}
		return m, nil
	case XlsX:
		xlFile, err := excelize.OpenFile(cfg.fileName)
		if err != nil {
			return nil, err
		}
		return xlFile.GetSheetMap(), nil
	}
	// CSV
	return map[int]string{1: cfg.fileName}, nil
}

func ReadXLSXFile(ctx context.Context, fn func(string, Row) error, filename string, sheetIndex int, columns []int, skip int) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	xlFile, err := excelize.OpenFile(filename)
	if err != nil {
		return fmt.Errorf("open %q: %w", filename, err)
	}
	sheetName := xlFile.GetSheetName(sheetIndex)
	if sheetName == "" {
		m := xlFile.GetSheetMap()
		if sheetName = m[sheetIndex]; sheetName == "" {
			if sheetName = m[sheetIndex-1]; sheetName == "" {
				if len(m) == 1 {
					for _, sheetName = range m {
						break
					}
				}
			}
		}
		if sheetName == "" {
			return fmt.Errorf("%d (only: %v): %w", sheetIndex, m, ErrUnknownSheet)
		}
	}
	n := 0
	var need map[int]bool
	if len(columns) != 0 {
		need = make(map[int]bool, len(columns))
		for _, i := range columns {
			need[i] = true
		}
	}
	rows, err := xlFile.Rows(sheetName)
	if err != nil {
		return err
	}
	//xfs := xlFile.Styles.CellXfs.Xf
	var numN int
	if xlFile.Styles.NumFmts != nil {
		numN = len(xlFile.Styles.NumFmts.NumFmt)
	}
	dateFmts := make(map[int]struct{}, 5+numN)
	var token struct{}
	dateFmts[14], dateFmts[15], dateFmts[16], dateFmts[17], dateFmts[22] = token, token, token, token, token
	if xlFile.Styles.NumFmts != nil {
		for _, nf := range xlFile.Styles.NumFmts.NumFmt {
			//log.Printf("%d. ID=%d code=%q", i, nf.NumFmtID, nf.FormatCode)
			if strings.Contains(nf.FormatCode, "yy") {
				dateFmts[nf.NumFmtID] = token
			}
		}
	}
	i := 0
	for rows.Next() {
		i++
		if i <= skip {
			continue
		}
		row, err := rows.Columns()
		if err != nil {
			return err
		}
		if row == nil {
			continue
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		for j := range row {
			axis, err := excelize.CoordinatesToCellName(j+1, i)
			if err != nil {
				return fmt.Errorf("%d:%d: %w", j, i, err)
			}
			styleID, err := xlFile.GetCellStyle(sheetName, axis)
			if err != nil {
				return err
			}
			xf := xlFile.Styles.CellXfs.Xf[styleID]
			// http://officeopenxml.com/SSstyles.php
			// CellXfs, maybe it's XfID points to CellStyleXFs, and maybe that's ApplyNumberFormat is not 0.
			// enc := json.NewEncoder(os.Stdout); enc.SetIndent("", "  "); enc.Encode(xlFile.Styles)
			var numFmtID int
			if xf.NumFmtID != nil {
				numFmtID = *xf.NumFmtID
			}
			if xf.XfID != nil {
				if sxf := xlFile.Styles.CellStyleXfs.Xf[*xf.XfID]; sxf.ApplyNumberFormat != nil &&
					sxf.NumFmtID != nil && *sxf.ApplyNumberFormat {
					numFmtID = *sxf.NumFmtID
				}
			}
			if _, ok := dateFmts[numFmtID]; !ok {
				continue
			}
			if err = xlFile.SetCellStyle(sheetName, axis, axis, 0); err != nil {
				return err
			}
			v, err := xlFile.GetCellValue(sheetName, axis)
			if err != nil {
				return err
			}
			_ = xlFile.SetCellStyle(sheetName, axis, axis, styleID)
			if v == "" {
				continue
			}
			f, err := strconv.ParseFloat(v, 32)
			if err != nil && (v[0] == '-' || '0' <= v[0] && v[0] <= '9') {
				log.Printf("%d:%d.ParseFloat(%q): %+v", i, j+1, v, err)
				continue
			}

			t, err := excelize.ExcelDateToTime(f, false)
			if err != nil {
				return fmt.Errorf("%d:%d.ExcelDateToTime(%f): %w", i, j+1, f, err)
			}
			if t.Equal(time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())) {
				row[j] = t.Format("2006-01-02")
			} else {
				row[j] = t.Format(time.RFC3339)
			}
			//log.Println("dateCols:", dateCols)
		}

		if err := fn(sheetName, Row{Line: n, Values: row}); err != nil {
			return err
		}
		n++
	}
	return nil
}

func ReadXLSFile(ctx context.Context, fn func(string, Row) error, filename string, charset string, sheetIndex int, columns []int, skip int) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	wb, err := xls.Open(filename, charset)
	if err != nil {
		return fmt.Errorf("open %q: %w", filename, err)
	}
	sheet := wb.GetSheet(sheetIndex)
	if sheet == nil {
		return fmt.Errorf("this XLS file does not contain sheet no %d", sheetIndex)
	}
	var need map[int]bool
	if len(columns) != 0 {
		need = make(map[int]bool, len(columns))
		for _, i := range columns {
			need[i] = true
		}
	}
	var maxWidth int
	for n := 0; n < int(sheet.MaxRow); n++ {
		row := sheet.Row(n)
		if n < skip {
			continue
		}
		if row == nil {
			continue
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		vals := make([]string, 0, maxWidth)
		off := row.FirstCol()
		if w := row.LastCol() - off; cap(vals) < w {
			maxWidth = w
			vals = make([]string, w)
		} else {
			vals = vals[:w]
		}

		for j := off; j < row.LastCol(); j++ {
			if need != nil && !need[int(j)] {
				continue
			}
			vals[j-off] = row.Col(j)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if err := fn(sheet.Name, Row{Line: int(n), Values: vals}); err != nil {
			return err
		}
	}
	return nil
}

func ReadCSV(ctx context.Context, fn func(Row) error, r io.Reader, delim string, columns []int, skip int) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	br := bufio.NewReader(r)
	if delim == "" {
		b, err := br.Peek(1024)
		if err != nil && len(b) == 0 {
			return err
		}
		seen := make(map[rune]struct{})
		nonAlnum := make([]rune, 0, 4)
		for _, r := range string(b) {
			if r == '"' || unicode.IsDigit(r) || unicode.IsLetter(r) {
				continue
			}
			if _, ok := seen[r]; ok {
				continue
			}
			seen[r] = struct{}{}
			nonAlnum = append(nonAlnum, r)
		}
		for len(nonAlnum) > 1 && nonAlnum[0] == ' ' {
			nonAlnum = nonAlnum[1:]
		}
		delim = string(nonAlnum[:1])
		log.Printf("Non-alphanum characters are %q, so delim is %q.", nonAlnum, delim)
	}
	cr := csv.NewReader(br)

	cr.Comma = ([]rune(delim))[0]
	cr.FieldsPerRecord = -1
	cr.LazyQuotes = true
	cr.ReuseRecord = true
	var colNames []string
	n := 0
	for {
		row, err := cr.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		n++
		if n <= skip {
			continue
		}
		if columns != nil {
			r2 := make([]string, len(columns))
			for i, j := range columns {
				r2[i] = row[j]
			}
			row = r2
		}
		if colNames == nil {
			colNames = append(make([]string, 0, len(row)), row...)
		}

		select {
		default:
		case <-ctx.Done():
			log.Printf("Ctx: %v", ctx.Err())
			return ctx.Err()
		}
		if err := fn(Row{Columns: colNames, Line: n - 1, Values: row}); err != nil {
			if err != context.Canceled {
				log.Printf("Consume %d. row: %+v", n, err)
			}
			return err
		}
	}
	return nil
}

type Row struct {
	Values  []string
	Columns []string
	Line    int
}

func FlagStrings() *StringsValue {
	return &StringsValue{}
}

type StringsValue struct {
	Strings []string
}

func (ss StringsValue) String() string      { return fmt.Sprintf("%v", ss.Strings) }
func (ss *StringsValue) Set(s string) error { ss.Strings = append(ss.Strings, s); return nil }

// vim: set noet fileencoding=utf-8:
