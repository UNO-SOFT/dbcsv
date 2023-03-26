// Copyright 2023 Tamás Gulácsi.
//
//
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/xuri/excelize/v2"
)

type command struct {
	Name string     `json:"c"`
	Args []argument `json:"a,omitempty"`
}
type argument struct {
	Date     time.Time              `json:"d,omitempty"`
	String   string                 `json:"s,omitempty"`
	Type     string                 `json:"t"`
	Raw      json.RawMessage        `json:"r,omitempty"`
	RichText []excelize.RichTextRun `json:"rich,omitempty"`
	Float    float64                `json:"f,omitempty"`
	Int      int                    `json:"i,omitempty"`
	Bool     bool                   `json:"b,omitempty"`
}

func executeCommands(ctx context.Context, w io.Writer, next func() (string, error)) error {
	f := excelize.NewFile()
	defer f.Close()
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
		styles := make(map[string]int)
		sheets := make(map[string]int)
		var c command
		if err = json.Unmarshal([]byte(s), &c); err != nil {
			return fmt.Errorf("unmarshal %q: %w", s, err)
		}
		for i, a := range c.Args {
			if a.Type == "" {
				c.Args[i].Type = "s"
			}
		}
		logger.Debug("executing", "command", c)
		switch c.Name {
		case "insertPageBreak":
			if err = c.checkArgs("ss"); err == nil {
				err = f.InsertPageBreak(c.Args[0].String, c.Args[1].String)
			}
		case "mergeCell":
			if err = c.checkArgs("sss"); err == nil {
				err = f.MergeCell(c.Args[0].String, c.Args[1].String, c.Args[2].String)
			}
		case "newSheet":
			if err = c.checkArgs("s"); err == nil {
				sheets[c.Args[0].String], err = f.NewSheet(c.Args[0].String)
			}
		case "newStyle":
			if err = c.checkArgs("sr"); err == nil {
				var s excelize.Style
				if err = json.Unmarshal(c.Args[1].Raw, &s); err == nil {
					styles[c.Args[0].String], err = f.NewStyle(&s)
				}
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
			if len(c.Args) != 3 {
				return fmt.Errorf("setCell requires sheet,cell,value, got %v", c.Args)
			}
			sheet, cell := c.Args[0].String, c.Args[1].String
			a := c.Args[2]
			switch a.Type {
			case "b", "bool":
				err = f.SetCellBool(sheet, cell, a.Bool)
			case "f", "float":
				err = f.SetCellFloat(sheet, cell, a.Float, -1, 64)
			case "F", "formula":
				err = f.SetCellFormula(sheet, cell, a.String)
			case "i", "int":
				err = f.SetCellInt(sheet, cell, a.Int)
			case "R", "richtext":
				err = f.SetCellRichText(sheet, cell, a.RichText)
			default:
				err = f.SetCellStr(sheet, cell, a.String)
			}
		case "setCellHyperlink":
			if err = c.checkArgs("ssHs"); err == nil {
				err = f.SetCellHyperLink(c.Args[0].String, c.Args[1].String, c.Args[2].String, c.Args[3].String)
			}
		case "setCellStyle":
			if err = c.checkArgs("ssss"); err == nil {
				err = f.SetCellStyle(c.Args[0].String, c.Args[1].String, c.Args[2].String, styles[c.Args[3].String])
			}
		case "setColStyle":
			if err = c.checkArgs("sss"); err == nil {
				err = f.SetColStyle(c.Args[0].String, c.Args[1].String, styles[c.Args[2].String])
			}
		case "setColOutlineLevel":
			if err = c.checkArgs("ssi"); err == nil {
				err = f.SetColOutlineLevel(c.Args[0].String, c.Args[1].String, uint8(c.Args[2].Int))
			}
		case "setColWidth":
			if err = c.checkArgs("sssf"); err == nil {
				err = f.SetColWidth(c.Args[0].String, c.Args[1].String, c.Args[2].String, c.Args[3].Float)
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
