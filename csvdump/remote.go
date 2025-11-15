// Copyright 2023 Tamás Gulácsi.
//
//
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strconv"
	"time"

	"github.com/xuri/excelize/v2"
)

type command struct {
	Name string     `json:"c"`
	Args []argument `json:"a,omitempty"`
}
type argument struct {
	Date     *time.Time             `json:"d,omitempty"`
	Coord    *coordinate            `json:"c,omitempty"`
	String   string                 `json:"s,omitempty"`
	Type     string                 `json:"t"`
	Raw      json.RawMessage        `json:"r,omitempty"`
	RichText []excelize.RichTextRun `json:"R,omitempty"`
	Float    float64                `json:"f,omitempty"`
	Int      int                    `json:"i,omitempty"`
	Bool     bool                   `json:"b,omitempty"`
}
type coordinate struct {
	Row int `json:"r"`
	Col int `json:"c"`
}

func colName(i int) string {
	s, err := excelize.ColumnNumberToName(i)
	if err != nil {
		panic(fmt.Errorf("%d is not a valid column: %w", i, err))
	}
	return s
}
func (c *coordinate) String() string {
	if c == nil {
		return ""
	}
	return colName(c.Col) + strconv.Itoa(c.Row)
}

func executeCommands(ctx context.Context, w io.Writer, next func() ([]byte, error)) error {
	f := excelize.NewFile()
	defer f.Close()
	var strs []string
	var arr []any
	row := 1
	styles := make(map[string]int)
	condStyles := make(map[string]int)
	sheets := make(map[string]int)
	var sheet string
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		s, err := next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}

		data := []byte(s)
		if len(s) != 0 && s[0] == '[' {
			if sheet == "" {
				idx := -1
				for i, nm := range f.GetSheetMap() {
					if idx < 0 || i < idx {
						idx, sheet = i, nm
					}
				}
				if sheet == "" {
					sheet = "sheet"
					if _, err = f.NewSheet(sheet); err != nil {
						return err
					}
				}
			}

			clear(strs)
			strs = strs[:0]
			if err := json.Unmarshal(data, &strs); err == nil {
				for i, s := range strs {
					var cell string
					if cell, err = excelize.CoordinatesToCellName(i+1, row); err == nil {
						err = f.SetCellStr(sheet, cell, s)
					}
					if err != nil {
						return err
					}
				}
			} else {
				clear(arr)
				arr = arr[:0]
				if err = json.Unmarshal(data, &arr); err != nil {
					return fmt.Errorf("decode %q into []any: %w", string(data), err)
				}
				clear(strs)
				for i, a := range arr {
					cell, err := excelize.CoordinatesToCellName(i+1, row)
					if err != nil {
						return err
					}
					switch x := a.(type) {
					case bool:
						err = f.SetCellBool(sheet, cell, x)
					case float32:
						err = f.SetCellFloat(sheet, cell, float64(x), -1, 32)
					case float64:
						err = f.SetCellFloat(sheet, cell, x, -1, 64)
					case int:
						err = f.SetCellInt(sheet, cell, int64(x))
					case int32:
						err = f.SetCellInt(sheet, cell, int64(x))
					case int64:
						err = f.SetCellInt(sheet, cell, int64(x))
					case string:
						err = f.SetCellStr(sheet, cell, x)
					default:
						err = f.SetCellStr(sheet, cell, fmt.Sprintf("%v", a))
					}
					if err != nil {
						return err
					}
				}
			}
			row++
			continue
		}

		var c command
		if err = json.Unmarshal(data, &c); err != nil {
			return fmt.Errorf("unmarshal %q: %w", s, err)
		}
		for i, a := range c.Args {
			if a.Type == "" {
				c.Args[i].Type = "s"
			}
		}
		slog.Debug("executing", "command", c)
		switch c.Name {
		case "insertPageBreak":
			if err = c.checkArgs("sc"); err == nil {
				err = f.InsertPageBreak(c.Args[0].String, c.Args[1].Coord.String())
			}
		case "mergeCell":
			if err = c.checkArgs("scc"); err == nil {
				err = f.MergeCell(c.Args[0].String, c.Args[1].Coord.String(), c.Args[2].Coord.String())
			}
		case "newSheet":
			sheet = c.Args[0].String
			if err = c.checkArgs("s"); err == nil {
				sheets[sheet], err = f.NewSheet(c.Args[0].String)
			}
		case "newStyle":
			if err = c.checkArgs("sr"); err == nil {
				var s excelize.Style
				if err = json.Unmarshal(c.Args[1].Raw, &s); err == nil {
					styles[c.Args[0].String], err = f.NewStyle(&s)
				}
				slog.Debug("newStyle", "raw", string(c.Args[1].Raw), "style", s, "name", c.Args[0].String, "index", styles[c.Args[0].String], "error", err)
			}
		case "newConditionalStyle":
			if err = c.checkArgs("sr"); err == nil {
				var s excelize.Style
				if err = json.Unmarshal(c.Args[1].Raw, &s); err == nil {
					condStyles[c.Args[0].String], err = f.NewConditionalStyle(&s)
				}
				slog.Debug("newConditionalStype", "raw", string(c.Args[1].Raw), "style", s, "name", c.Args[0].String, "index", condStyles[c.Args[0].String], "error", err)
			}
		case "protectSheet":
			if err = c.checkArgs("sr"); err == nil {
				var o excelize.SheetProtectionOptions
				if err = json.Unmarshal(c.Args[1].Raw, &o); err == nil {
					err = f.ProtectSheet(c.Args[0].String, &o)
				}
			}
		case "protectWorkbook":
			if err = c.checkArgs("r"); err == nil {
				var o excelize.WorkbookProtectionOptions
				if err = json.Unmarshal(c.Args[0].Raw, &o); err == nil {
					err = f.ProtectWorkbook(&o)
				}
			}
		case "setActiveSheet":
			if err = c.checkArgs("i"); err == nil {
				f.SetActiveSheet(c.Args[0].Int)
			}
		case "setCell":
			if len(c.Args) != 3 || c.Args[0].Type != "s" || c.Args[1].Type != "c" {
				return fmt.Errorf("setCell requires sheet,cell,value, got %v", c.Args)
			}
			var cell string
			sheet, cell = c.Args[0].String, c.Args[1].Coord.String()
			a := c.Args[2]
			switch a.Type {
			case "b", "bool":
				err = f.SetCellBool(sheet, cell, a.Bool)
			case "f", "float":
				err = f.SetCellFloat(sheet, cell, a.Float, -1, 64)
			case "F", "formula":
				err = f.SetCellFormula(sheet, cell, a.String)
			case "i", "int":
				err = f.SetCellInt(sheet, cell, int64(a.Int))
			case "R", "richtext":
				err = f.SetCellRichText(sheet, cell, a.RichText)
			case "s", "string":
				err = f.SetCellStr(sheet, cell, a.String)
			default:
				slog.Warn("setCell", "sheet", sheet, "cell", cell, "arg", a, "unknown type", a.Type)
				err = f.SetCellStr(sheet, cell, a.String)
			}
		case "setCellFormula":
			if err = c.checkArgs("scs"); err == nil {
				err = f.SetCellFormula(c.Args[0].String, c.Args[1].Coord.String(), c.Args[2].String)
			}
		case "setCellHyperlink":
			if err = c.checkArgs("scHs"); err == nil {
				err = f.SetCellHyperLink(c.Args[0].String, c.Args[1].Coord.String(), c.Args[2].String, c.Args[3].String)
			}
		case "setCellStyle":
			if err = c.checkArgs("sccs"); err == nil {
				if si, ok := styles[c.Args[3].String]; !ok {
					err = fmt.Errorf("style %q is not found (have: %v)", c.Args[3].String, styles)
				} else {
					err = f.SetCellStyle(c.Args[0].String, c.Args[1].Coord.String(), c.Args[2].Coord.String(), si)
				}
			}
		case "setConditionalFormat":
			if err = c.checkArgs("sccr"); err == nil {
				var cf []excelize.ConditionalFormatOptions
				if err = json.Unmarshal(c.Args[3].Raw, &cf); err == nil {
					s := c.Args[1].Coord.String()
					if c.Args[2].Coord != nil {
						s += ":" + c.Args[2].Coord.String()
					}
					err = f.SetConditionalFormat(c.Args[0].String, s, cf)
				}
			}
		case "setColStyle":
			if err = c.checkArgs("siis"); err == nil {
				if si, ok := styles[c.Args[3].String]; !ok {
					err = fmt.Errorf("style %q is not found", c.Args[3].String)
				} else {
					cols := colName(c.Args[1].Int)
					if c.Args[2].Int > c.Args[1].Int {
						cols += ":" + strconv.Itoa(c.Args[2].Int)
					}
					err = f.SetColStyle(c.Args[0].String, cols, si)
				}
			}
		case "setColOutlineLevel":
			if err = c.checkArgs("sii"); err == nil {
				err = f.SetColOutlineLevel(c.Args[0].String, colName(c.Args[1].Int), uint8(c.Args[2].Int))
			}
		case "setColWidth":
			if err = c.checkArgs("siif"); err == nil {
				err = f.SetColWidth(
					c.Args[0].String,
					colName(c.Args[1].Int), colName(c.Args[1].Int),
					c.Args[3].Float)
			}
		case "setDefaultFont":
			if err = c.checkArgs("s"); err == nil {
				err = f.SetDefaultFont(c.Args[0].String)
			}
		case "setRowHeight":
			if err = c.checkArgs("sif"); err == nil {
				err = f.SetRowHeight(c.Args[0].String, c.Args[1].Int, c.Args[2].Float)
			}
		case "setRowOutlineLevel":
			if err = c.checkArgs("sii"); err == nil {
				err = f.SetRowOutlineLevel(c.Args[0].String, c.Args[1].Int, uint8(c.Args[2].Int))
			}
		case "setRowStyle":
			if err = c.checkArgs("siis"); err == nil {
				err = f.SetRowStyle(c.Args[0].String, c.Args[1].Int, c.Args[2].Int, styles[c.Args[3].String])
			}
		case "setSheetName":
			if err = c.checkArgs("ss"); err == nil {
				err = f.SetSheetName(c.Args[0].String, c.Args[1].String)
			}
		default:
			return fmt.Errorf("unknown command %q", c.Name)
		}
		if err != nil {
			return fmt.Errorf("command %#v: %w", c, err)
		}
	}
	if _, err := f.WriteTo(w); err != nil {
		return fmt.Errorf("WriteTo: %w", err)
	}
	return nil
}

var (
	errArgTypeMismatch = errors.New("argument type mismatch")
	errArgNumMismatch  = errors.New("argument number mismatch")
)

func (c command) checkArgs(types string) error {
	if len(c.Args) != len(types) {
		return fmt.Errorf("%s wants %d args, got %d: %w", c.Name, len(types), len(c.Args), errArgNumMismatch)
	}
	for i, r := range types {
		if c.Args[i].Type != string([]rune{r}) {
			return fmt.Errorf("%s %d. arg wants %v, got %v: %w", c.Name, i, r, c.Args[i].Type, errArgTypeMismatch)
		}
	}
	return nil
}

func dumpRemoteCSV(ctx context.Context, w io.Writer, rows *sql.Rows, sep string) error {
	return remoteCSV(ctx, w, sep, func() ([]byte, error) {
		if !rows.Next() {
			err := rows.Close()
			if err == nil {
				err = rows.Err()
			}
			if err == nil {
				err = io.EOF
			}
			return nil, err
		}
		var s string
		err := rows.Scan(&s)
		return []byte(s), err
	})
}

func remoteCSV(ctx context.Context, w io.Writer, sep string, next func() ([]byte, error)) error {
	var strs []string
	var arr []any
	cw := csv.NewWriter(w)
	if sep != "" {
		cw.Comma = ([]rune(sep))[0]
	}

	for {
		data, err := next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		clear(strs)
		strs = strs[:0]
		if err := json.Unmarshal(data, &strs); err != nil {
			clear(arr)
			arr = arr[:0]
			if err = json.Unmarshal(data, &arr); err != nil {
				return fmt.Errorf("decode %q into []any: %w", string(data), err)
			}
			for _, a := range arr {
				strs = append(strs, fmt.Sprintf("%v", a))
			}
		}
		if err := cw.Write(strs); err != nil {
			return err
		}
	}

	cw.Flush()
	return cw.Error()
}
