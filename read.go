// Copyright 2017 Tamás Gulácsi. All rights reserved.

package dbcsv

import (
	"bufio"
	"bytes"
	"context"
	"encoding/csv"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"
	"unicode"

	"golang.org/x/text/encoding"
	"golang.org/x/text/encoding/htmlindex"
	"golang.org/x/text/transform"

	"github.com/360EntSecGroup-Skylar/excelize/v2"
	"github.com/extrame/xls"
	errors "golang.org/x/xerrors"
)

var DefaultEncoding = encoding.Replacement
var UnknownSheet = errors.New("unknown sheet")

func init() {
	encName := os.Getenv("LANG")
	if i := strings.IndexByte(encName, '.'); i >= 0 {
		if enc, err := htmlindex.Get(encName[i+1:]); err == nil {
			DefaultEncoding = enc
		}
	}
}

type FileType string

const (
	Unknown = FileType("")
	Csv     = FileType("csv")
	Xls     = FileType("xls")
	XlsX    = FileType("xlsx")
)

func DetectReaderType(r io.Reader, fileName string) (FileType, error) {
	// detect file type
	var b [4]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return Unknown, err
	}
	if bytes.Equal(b[:], []byte{0xd0, 0xcf, 0x11, 0xe0}) { // OLE2
		return Xls, nil
	} else if bytes.Equal(b[:], []byte{0x50, 0x4b, 0x03, 0x04}) { //PKZip, so xlsx
		return XlsX, nil
	} 
	// CSV
	return Csv, nil
}

type Config struct {
	typ           FileType
	Sheet, Skip   int
	Delim         string
	Charset       string
	ColumnsString string
	encoding      encoding.Encoding
	columns       []int
	fileName      string
	file          *os.File
	permanent     bool
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
	if cfg.ColumnsString == "" {
		return nil, nil
	}
	if cfg.columns != nil {
		return cfg.columns, nil
	}
	cfg.columns = make([]int, 0, strings.Count(cfg.ColumnsString, ",")+1)
	for _, x := range strings.Split(cfg.ColumnsString, ",") {
		i, err := strconv.Atoi(x)
		if err != nil {
			return cfg.columns, errors.Errorf("%s: %w", x, err)
		}
		cfg.columns = append(cfg.columns, i-1)
	}
	return cfg.columns, nil
}

func (cfg *Config) Type() (FileType, error) {
	if cfg.typ != Unknown {
		return cfg.typ, nil
	}
	var err error
	cfg.typ, err = DetectReaderType(cfg.file, cfg.fileName)
	if err == nil {
		_, err = cfg.file.Seek(0, 0)
	}
	return cfg.typ, err
}

func (cfg *Config) OpenVolatile(fileName string) error {
	cfg.fileName = fileName
	if fileName == "-" || fileName == "" {
		cfg.file, cfg.permanent = os.Stdin, false
		return nil
	}
	var err error
	cfg.file, err = os.Open(fileName)
	if fi, statErr := cfg.file.Stat(); statErr != nil || !fi.Mode().IsRegular() {
		cfg.permanent = false
	}
	return err
}
func (cfg *Config) Open(fileName string) error {
	slurp := fileName == "-" || fileName == ""
	cfg.permanent = true
	if slurp {
		cfg.file, fileName = os.Stdin, "-"
	} else {
		var err error
		if cfg.file, err = os.Open(fileName); err != nil {
			return errors.Errorf("open %s: %w", fileName, err)
		}
		fi, err := cfg.file.Stat()
		if err != nil {
			cfg.file.Close()
			return errors.Errorf("stat %s: %w", fileName, err)
		}
		slurp = !fi.Mode().IsRegular()
	}
	var err error
	if slurp {
		fh, tmpErr := ioutil.TempFile("", "ReadRows-")
		if tmpErr != nil {
			return tmpErr
		}
		defer fh.Close()
		fileName = fh.Name()
		defer os.Remove(fileName)
		log.Printf("Copying into temporary file %q...", fileName)
		if _, err = io.Copy(fh, cfg.file); err != nil {
			return errors.Errorf("copy into %s: %w", fh.Name(), err)
		}
		if err = fh.Close(); err != nil {
			return errors.Errorf("close %s: %w", fh.Name(), err)
		}
		if cfg.file, err = os.Open(fileName); err != nil {
			return errors.Errorf("open %s: %w", fileName, err)
		}
	}
	cfg.fileName = fileName
	_, err = cfg.Type()
	if err != nil {
		return errors.Errorf("type %s: %w", cfg.fileName, err)
	}
	return nil
}

func (cfg *Config) Close() error {
	fh := cfg.file
	cfg.file, cfg.fileName, cfg.typ = nil, "", Unknown
	if fh != nil {
		return fh.Close()
	}
	return nil
}

func (cfg *Config) ReadRows(ctx context.Context, fn func(string, Row) error) (err error) {
	if err = ctx.Err(); err != nil {
		return err
	}
	columns, err := cfg.Columns()
	if err != nil {
		return err
	}

	if cfg.permanent {
		if _, err = cfg.file.Seek(0, 0); err != nil {
			return err
		}
	}
	switch cfg.typ {
	case Xls:
		return ReadXLSFile(ctx, fn, cfg.fileName, cfg.Charset, cfg.Sheet, columns, cfg.Skip)
	case XlsX:
		return ReadXLSXFile(ctx, fn, cfg.fileName, cfg.Sheet, columns, cfg.Skip)
	}
	enc, err := cfg.Encoding()
	if err != nil {
		return err
	}
	r := transform.NewReader(cfg.file, enc.NewDecoder())
	return ReadCSV(ctx, func(row Row) error { return fn(cfg.fileName, row) }, r, cfg.Delim, columns, cfg.Skip)
}

const (
	DateFormat     = "20060102"
	DateTimeFormat = "20060102150405"
)

func ReadXLSXFile(ctx context.Context, fn func(string, Row) error, filename string, sheetIndex int, columns []int, skip int) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	xlFile, err := excelize.OpenFile(filename)
	if err != nil {
		return errors.Errorf("open %q: %w", filename, err)
	}
	sheetName := xlFile.GetSheetName(sheetIndex)
	if sheetName == "" {
		return errors.Errorf("%d: %w", sheetIndex, UnknownSheet)
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
		return errors.Errorf("open %q: %w", filename, err)
	}
	sheet := wb.GetSheet(sheetIndex)
	if sheet == nil {
		return errors.Errorf("This XLS file does not contain sheet no %d!", sheetIndex)
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
		if w := row.LastCol()-off ; cap(vals) < w {
			maxWidth = w
			vals = make([]string, w)
		} else {
			vals =vals[:w]
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
		select {
		default:
		case <-ctx.Done():
			return ctx.Err()
		}
		if err := fn(Row{Line: n - 1, Values: row}); err != nil {
			return err
		}
	}
	return nil
}

type Row struct {
	Line   int
	Values []string
}

// vim: set noet fileencoding=utf-8:
