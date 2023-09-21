package testplsqlsplit_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/csv"
	"flag"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/godror/godror"
	"golang.org/x/text/encoding"
	"golang.org/x/text/encoding/charmap"
)

var (
	flagConnect = flag.String("connect", os.Getenv("BRUNO_ID"), "connection string")
	flagSep     = flag.String("comma", string([]rune{comma}), "separator")

	flagParseOnce sync.Once
)

var comma = ';'

func FuzzClob2CSV(f *testing.F) {
	flagParseOnce.Do(func() { flag.Parse() })

	comma = []rune(*flagSep)[0]
	db, err := sql.Open("godror", *flagConnect)
	if err != nil {
		f.Fatalf("connect to %q: %+v", *flagConnect, err)
	}
	defer db.Close()

	f.Add(strings.Join([]string{
		"", "a", "arvizturo tukorfurogep", "9", "\n", `"`, "\t",
	}, "\000"))
	f.Fuzz(func(t *testing.T, s string) { testClob2CSV(t, db, s) })
}

func TestClob2CSV(t *testing.T) {
	flagParseOnce.Do(func() { flag.Parse() })

	db, err := sql.Open("godror", *flagConnect)
	if err != nil {
		t.Fatalf("connect to %q: %+v", *flagConnect, err)
	}
	defer db.Close()
	for _, rec := range [][]string{
		{"", "a", "arvizturo tukorfurogep", "9", "\n", `"`, "\t"},
	} {
		testClob2CSV(t, db, strings.Join(rec, "\000"))
	}
}

func testClob2CSV(t *testing.T, db *sql.DB, s string) {
	if !utf8.ValidString(s) {
		return
	}
	encoded, err := encoding.ReplaceUnsupported(charmap.ISO8859_2.NewEncoder()).String(s)
	if err != nil {
		t.Logf("%q: %+v", s, err)
		return
	}
	var buf bytes.Buffer
	cw := csv.NewWriter(&buf)
	want := strings.Split(s, "\000")
	cw.Comma = comma
	cw.Write(want)
	cw.Flush()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	const qry = `DECLARE
  v_tab DB_cvt.typ_csv_tab;
  v_rec DB_cvt.typ_csv_rec;
BEGIN
  v_tab := DB_cvt.clob2csv(:1); 
  IF v_tab.FIRST IS NOT NULL THEN
    v_rec := v_tab(v_tab.FIRST);
  END IF;
  :2 := v_rec;
END;`
	var tt CsvRec
	tt.Values = make([]string, len(want))
	t.Logf("input[%d]: %q", buf.Len(), buf.String())
	if _, err := db.ExecContext(ctx, qry,
		buf.String(),
		sql.Out{Dest: &tt.Values},
		godror.PlSQLArrays,
	); err != nil {
		t.Fatalf("exec %s: %+v", qry, err)
	}
	t.Logf("want: %q\ngot: %q", want, tt.Values)
	if len(tt.Values) != len(want) {
		t.Errorf("got %d values, wanted %d", len(tt.Values), len(want))
	}
	for i, got := range tt.Values {
		if i >= len(want) {
			break
		}
		if got != want[i] {
			t.Errorf("%d. got %q(% x), wanted %q(% x)", i, got, got, want, encoded)
		}
	}
	t.Logf("tt: %q", tt)
}

// CsvTab
// TYPE typ_csv_rec IS TABLE OF VARCHAR2(32767) INDEX BY BINARY_INTEGER;
// TYPE typ_csv_tab IS TABLE OF typ_csv_rec INDEX BY BINARY_INTEGER;
type CsvTab struct {
	godror.ObjectTypeName `godror:"DB_cvt.typ_csv_tab" json:"-"`
	Records               []CsvRec `godror:"DB_cvt.typ_csv_rec"`
}
type CsvRec struct {
	godror.ObjectTypeName `godror:"DB_cvt.typ_csv_rec" json:"-"`
	Values                []string
}
