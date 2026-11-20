package sqlx

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"
)

var strictSchema = Schema{
	create: `
CREATE TABLE strict_person (
	id integer,
	first_name text,
	last_name text,
	email text,
	mood text
);
CREATE TABLE strict_kv (
	k text,
	v integer
);`,
	drop: `
drop table strict_person;
drop table strict_kv;`,
}

type strictPerson struct {
	ID        int    `db:"id"`
	FirstName string `db:"first_name"`
	LastName  string `db:"last_name"`
	Email     string `db:"email"`
	Mood      string `db:"mood"`
}

type strictPersonSlim struct {
	ID        int    `db:"id"`
	FirstName string `db:"first_name"`
}

type strictAddress struct {
	City string `db:"city"`
}

type strictNested struct {
	ID      int           `db:"id"`
	Address strictAddress `db:"address"`
}

func loadStrictFixture(db *DB, t *testing.T) {
	t.Helper()
	tx := db.MustBegin()
	tx.MustExec(tx.Rebind("INSERT INTO strict_person (id, first_name, last_name, email, mood) VALUES (?, ?, ?, ?, ?)"),
		1, "Jason", "Moiron", "jmoiron@jmoiron.net", "happy")
	tx.MustExec(tx.Rebind("INSERT INTO strict_person (id, first_name, last_name, email, mood) VALUES (?, ?, ?, ?, ?)"),
		2, "John", "Doe", "johndoe@example.net", "sleepy")
	tx.MustExec(tx.Rebind("INSERT INTO strict_kv (k, v) VALUES (?, ?)"), "hi", 1)
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func strictError(t *testing.T, err error) *StrictScanError {
	t.Helper()
	var se *StrictScanError
	if !errors.As(err, &se) {
		t.Fatalf("expected *StrictScanError, got %T: %v", err, err)
	}
	return se
}

func assertSorted(t *testing.T, name string, got []string) {
	t.Helper()
	if !sort.StringsAreSorted(got) {
		t.Errorf("%s = %v is not sorted", name, got)
	}
}

// mustQueryx opens rows or fails the test.  Callers close the rows
// explicitly before returning; RunWithSchema drops its tables from a deferred
// cleanup, so a t.Cleanup-registered close would run too late and hold a
// sqlite write lock over the drop.
func mustQueryx(t *testing.T, db *DB, query string) *Rows {
	t.Helper()
	rows, err := db.Queryx(query)
	if err != nil {
		t.Fatal(err)
	}
	return rows
}

// Requirement 1: a single report carries filled fields, missing target
// fields and unknown columns, and the error lists stable column names.
func TestStrictStructScanReport(t *testing.T) {
	RunWithSchema(strictSchema, t, func(db *DB, t *testing.T, now string) {
		loadStrictFixture(db, t)

		// exact mapping: filled fields reported, nothing missing/unknown.
		rows := mustQueryx(t, db, "SELECT id, first_name FROM strict_person ORDER BY id")
		defer rows.Close()
		if !rows.Next() {
			t.Fatal("expected a row")
		}
		var slim strictPersonSlim
		report, err := rows.StrictStructScan(&slim, StrictScanOptions{})
		if err != nil {
			t.Fatalf("exact mapping should scan: %v", err)
		}
		if !reflect.DeepEqual(report.FilledFields, []string{"first_name", "id"}) {
			t.Errorf("FilledFields = %v", report.FilledFields)
		}
		if report.Partial || report.RowsScanned != 1 {
			t.Errorf("unexpected Partial=%v RowsScanned=%d", report.Partial, report.RowsScanned)
		}

		// missing target fields (result has fewer columns).
		rows = mustQueryx(t, db, "SELECT id FROM strict_person ORDER BY id")
		defer rows.Close()
		if !rows.Next() {
			t.Fatal("expected a row")
		}
		var miss strictPersonSlim
		_, err = rows.StrictStructScan(&miss, StrictScanOptions{})
		se := strictError(t, err)
		if se.Phase != "mapping" {
			t.Errorf("expected mapping phase, got %q", se.Phase)
		}
		if !reflect.DeepEqual(se.Report.MissingFields, []string{"first_name"}) {
			t.Errorf("MissingFields = %v", se.Report.MissingFields)
		}
		if !reflect.DeepEqual(se.Report.FilledFields, []string{"id"}) {
			t.Errorf("FilledFields = %v", se.Report.FilledFields)
		}
		// the rejected destination must not have been written.
		if miss.ID != 0 || miss.FirstName != "" {
			t.Errorf("rejected scan wrote into dest: %+v", miss)
		}
		assertSorted(t, "MissingFields", se.Report.MissingFields)

		// unknown result columns (result has more columns than the struct).
		rows = mustQueryx(t, db, "SELECT id, first_name, last_name FROM strict_person ORDER BY id")
		defer rows.Close()
		if !rows.Next() {
			t.Fatal("expected a row")
		}
		var extra strictPersonSlim
		_, err = rows.StrictStructScan(&extra, StrictScanOptions{})
		se = strictError(t, err)
		if !reflect.DeepEqual(se.Report.UnknownColumns, []string{"last_name"}) {
			t.Errorf("UnknownColumns = %v", se.Report.UnknownColumns)
		}
		if extra.ID != 0 {
			t.Errorf("rejected scan wrote into dest: %+v", extra)
		}
		assertSorted(t, "UnknownColumns", se.Report.UnknownColumns)

		// both at once: the full audit must be returned together.
		rows = mustQueryx(t, db, "SELECT first_name, last_name FROM strict_person ORDER BY id")
		defer rows.Close()
		if !rows.Next() {
			t.Fatal("expected a row")
		}
		var both strictPersonSlim
		_, err = rows.StrictStructScan(&both, StrictScanOptions{})
		se = strictError(t, err)
		if !reflect.DeepEqual(se.Report.MissingFields, []string{"id"}) ||
			!reflect.DeepEqual(se.Report.UnknownColumns, []string{"last_name"}) ||
			!reflect.DeepEqual(se.Report.FilledFields, []string{"first_name"}) {
			t.Errorf("combined report = %+v", se.Report)
		}
		if both.ID != 0 || both.FirstName != "" {
			t.Errorf("rejected scan wrote into dest: %+v", both)
		}
	})
}

// Requirement 3a: StrictAllowPartial exposes an explicit Partial flag and
// only writes matched fields; missing targets keep whatever the caller set.
func TestStrictStructScanAllowPartial(t *testing.T) {
	RunWithSchema(strictSchema, t, func(db *DB, t *testing.T, now string) {
		loadStrictFixture(db, t)
		rows := mustQueryx(t, db, "SELECT id, last_name, email FROM strict_person ORDER BY id")
		defer rows.Close()
		if !rows.Next() {
			t.Fatal("expected a row")
		}
		p := strictPersonSlim{FirstName: "preset-default"}
		report, err := rows.StrictStructScan(&p, StrictScanOptions{Incomplete: StrictAllowPartial})
		if err != nil {
			t.Fatalf("partial scan should succeed: %v", err)
		}
		if !report.Partial {
			t.Error("Partial should be true")
		}
		if p.ID != 1 {
			t.Errorf("matched field not scanned: %+v", p)
		}
		if p.FirstName != "preset-default" {
			t.Errorf("missing field was silently overwritten: %+v", p)
		}
		if !reflect.DeepEqual(report.MissingFields, []string{"first_name"}) {
			t.Errorf("MissingFields = %v", report.MissingFields)
		}
		if !reflect.DeepEqual(report.UnknownColumns, []string{"email", "last_name"}) {
			t.Errorf("UnknownColumns = %v", report.UnknownColumns)
		}
	})
}

// Requirement 4: duplicate result columns and type conversion failures are
// merged into the report while the driver error stays identifiable.
func TestStrictStructScanDuplicatesAndConversion(t *testing.T) {
	RunWithSchema(strictSchema, t, func(db *DB, t *testing.T, now string) {
		loadStrictFixture(db, t)

		// duplicate columns: ambiguous ownership must fail before scanning.
		rows := mustQueryx(t, db, "SELECT id, id, first_name FROM strict_person ORDER BY id")
		defer rows.Close()
		if !rows.Next() {
			t.Fatal("expected a row")
		}
		var p strictPersonSlim
		_, err := rows.StrictStructScan(&p, StrictScanOptions{Incomplete: StrictAllowPartial})
		se := strictError(t, err)
		if !reflect.DeepEqual(se.Report.DuplicateColumns, []string{"id"}) {
			t.Errorf("DuplicateColumns = %v", se.Report.DuplicateColumns)
		}
		if p.ID != 0 {
			t.Error("ambiguous scan wrote into dest")
		}

		// type conversion failure: the driver error must remain unwrappable.
		type wrongTypes struct {
			K int    `db:"k"`
			V string `db:"v"`
		}
		rows = mustQueryx(t, db, "SELECT k, v FROM strict_kv")
		defer rows.Close()
		if !rows.Next() {
			t.Fatal("expected a row")
		}
		var wt wrongTypes
		report, err := rows.StrictStructScan(&wt, StrictScanOptions{})
		se = strictError(t, err)
		if se.Phase != "scan" {
			t.Errorf("expected scan phase, got %q", se.Phase)
		}
		if se.Unwrap() == nil {
			t.Error("underlying conversion error was lost")
		}
		if !reflect.DeepEqual(report.FilledFields, []string{"k", "v"}) || report.RowsScanned != 0 {
			t.Errorf("report on conversion failure = %+v", report)
		}
	})
}

// Requirement 4: rows closed before iteration surface through the strict
// error type instead of a generic failure.
func TestStrictStructScanClosedRows(t *testing.T) {
	RunWithSchema(strictSchema, t, func(db *DB, t *testing.T, now string) {
		loadStrictFixture(db, t)
		rows := mustQueryx(t, db, "SELECT id, first_name FROM strict_person")
		defer rows.Close()
		if err := rows.Close(); err != nil {
			t.Fatal(err)
		}
		var p strictPersonSlim
		_, err := rows.StrictStructScan(&p, StrictScanOptions{})
		if err == nil {
			t.Fatal("expected error on closed rows")
		}
		// Columns() on closed rows still succeeds with many drivers; the
		// failure then appears in the scan phase.  Either way it must be a
		// *StrictScanError with a populated report.
		var se *StrictScanError
		if !errors.As(err, &se) {
			t.Fatalf("expected *StrictScanError, got %T: %v", err, err)
		}
		if se.Report == nil {
			t.Fatal("report must be attached even on closed rows")
		}
	})
}

// The cached strict plan belongs to one struct type; reusing the same Rows
// with a different destination type must fail loudly instead of corrupting
// memory through stale traversals.
func TestStrictStructScanTypeMismatch(t *testing.T) {
	RunWithSchema(strictSchema, t, func(db *DB, t *testing.T, now string) {
		loadStrictFixture(db, t)
		rows := mustQueryx(t, db, "SELECT id, first_name FROM strict_person ORDER BY id")
		defer rows.Close()
		if !rows.Next() {
			t.Fatal("expected a row")
		}
		var slim strictPersonSlim
		if _, err := rows.StrictStructScan(&slim, StrictScanOptions{}); err != nil {
			t.Fatal(err)
		}
		if !rows.Next() {
			t.Fatal("expected a second row")
		}
		var full strictPerson
		_, err := rows.StrictStructScan(&full, StrictScanOptions{})
		if err == nil {
			t.Fatal("expected type mismatch error")
		}
		if !strings.Contains(err.Error(), "type mismatch") {
			t.Errorf("unexpected error: %v", err)
		}
	})
}

// StrictScanError text must remain stable and auditable, and the report must
// be reachable through errors.As while sql.ErrNoRows stays errors.Is-able.
func TestStrictScanErrorShape(t *testing.T) {
	RunWithSchema(strictSchema, t, func(db *DB, t *testing.T, now string) {
		loadStrictFixture(db, t)
		rows := mustQueryx(t, db, "SELECT id, last_name FROM strict_person")
		defer rows.Close()
		if !rows.Next() {
			t.Fatal("expected a row")
		}
		var slim strictPersonSlim
		_, err := rows.StrictStructScan(&slim, StrictScanOptions{})
		text := err.Error()
		if !strings.Contains(text, `missing destination fields ["first_name"]`) ||
			!strings.Contains(text, `unknown result columns ["last_name"]`) {
			t.Fatalf("unstable error text: %s", text)
		}
		var se *StrictScanError
		if !errors.As(err, &se) || se.Phase != "mapping" {
			t.Fatalf("mapping error shape wrong: %#v", err)
		}
		if se.Unwrap() != nil {
			t.Error("mapping-phase errors carry no underlying error")
		}
	})
}

// Scannable leaf structs (sql.NullString, sql.Scanner implementations) scan
// as a single value; their unexported internals must never appear in the
// missing-field audit. db:"-" fields and empty result sets are covered too.
func TestStrictStructScanScannableLeavesAndEdges(t *testing.T) {
	RunWithSchema(strictSchema, t, func(db *DB, t *testing.T, now string) {
		loadStrictFixture(db, t)

		type nullField struct {
			Email sql.NullString `db:"email"`
		}
		rows := mustQueryx(t, db, "SELECT email FROM strict_person ORDER BY id")
		defer rows.Close()
		if !rows.Next() {
			t.Fatal("expected a row")
		}
		var nf nullField
		report, err := rows.StrictStructScan(&nf, StrictScanOptions{})
		if err != nil {
			t.Fatalf("sql.NullString leaf: %v", err)
		}
		if !nf.Email.Valid || nf.Email.String != "jmoiron@jmoiron.net" {
			t.Errorf("null field = %+v", nf.Email)
		}
		if !reflect.DeepEqual(report.FilledFields, []string{"email"}) ||
			len(report.MissingFields) != 0 {
			t.Errorf("scannable internals leaked into audit: %+v", report)
		}

		// db:"-" fields are not audited as missing targets.
		type withSkipped struct {
			ID      int    `db:"id"`
			Ignored string `db:"-"`
		}
		rows = mustQueryx(t, db, "SELECT id FROM strict_person ORDER BY id")
		defer rows.Close()
		if !rows.Next() {
			t.Fatal("expected a row")
		}
		var ws withSkipped
		if _, err := rows.StrictStructScan(&ws, StrictScanOptions{}); err != nil {
			t.Fatalf("db:\"-\" field broke strict scan: %v", err)
		}
		if ws.ID != 1 {
			t.Errorf("id = %d", ws.ID)
		}

		// empty result set on the package-level scan.
		emptyRows := mustQueryx(t, db, "SELECT id, first_name FROM strict_person WHERE id < 0")
		var slim []strictPersonSlim
		emptyReport, err := StrictStructScan(emptyRows, &slim, StrictScanOptions{})
		if err != nil {
			t.Fatalf("empty result: %v", err)
		}
		if len(slim) != 0 || emptyReport.RowsScanned != 0 || emptyReport.Partial {
			t.Errorf("empty report = %+v slim=%v", emptyReport, slim)
		}

		// non-pointer argument is rejected without a panic.
		var bad strictPersonSlim
		if _, err := rows.StrictStructScan(bad, StrictScanOptions{}); err == nil {
			t.Error("expected error for non-pointer dest")
		}
	})
}

// Requirement 2: nested structs follow the existing dotted-path mapping
// rules, and db-tag collisions between non-embedded fields fail before the
// first row is read.
func TestStrictStructScanNestedAndCollision(t *testing.T) {
	RunWithSchema(strictSchema, t, func(db *DB, t *testing.T, now string) {
		loadStrictFixture(db, t)

		rows := mustQueryx(t, db, `SELECT 1 AS id, 'NYC' AS "address.city"`)
		defer rows.Close()
		if !rows.Next() {
			t.Fatal("expected a row")
		}
		var n strictNested
		report, err := rows.StrictStructScan(&n, StrictScanOptions{})
		if err != nil {
			t.Fatalf("nested exact mapping should scan: %v", err)
		}
		if n.ID != 1 || n.Address.City != "NYC" {
			t.Errorf("nested dest = %+v", n)
		}
		if !reflect.DeepEqual(report.FilledFields, []string{"address.city", "id"}) {
			t.Errorf("FilledFields = %v", report.FilledFields)
		}

		// nested partial: missing nested leaf is reported with its path.
		rows = mustQueryx(t, db, `SELECT 1 AS id`)
		defer rows.Close()
		if !rows.Next() {
			t.Fatal("expected a row")
		}
		var np strictNested
		report, err = rows.StrictStructScan(&np, StrictScanOptions{Incomplete: StrictAllowPartial})
		if err != nil {
			t.Fatalf("partial nested scan should succeed: %v", err)
		}
		if !report.Partial || !reflect.DeepEqual(report.MissingFields, []string{"address.city"}) {
			t.Errorf("nested report = %+v", report)
		}
		if np.Address.City != "" {
			t.Error("missing nested leaf must keep zero value")
		}

		// two non-embedded fields claiming the same db name: ambiguous.
		type collide struct {
			A string `db:"name"`
			B string `db:"name"`
		}
		rows = mustQueryx(t, db, `SELECT 'x' AS name`)
		defer rows.Close()
		if !rows.Next() {
			t.Fatal("expected a row")
		}
		var c collide
		_, err = rows.StrictStructScan(&c, StrictScanOptions{Incomplete: StrictAllowPartial})
		se := strictError(t, err)
		if len(se.Report.AmbiguousFields) == 0 {
			t.Errorf("AmbiguousFields = %v", se.Report.AmbiguousFields)
		}
		if c.A != "" || c.B != "" {
			t.Error("ambiguous collision wrote into dest")
		}
	})
}

// Embedded shadowing stays deterministic per the existing mapper rules; the
// strict scan accepts it as long as every result column maps somewhere.
func TestStrictStructScanEmbeddedShadowing(t *testing.T) {
	RunWithSchema(strictSchema, t, func(db *DB, t *testing.T, now string) {
		loadStrictFixture(db, t)
		type inner struct {
			FirstName string `db:"first_name"`
			LastName  string `db:"last_name"`
			Email     string `db:"email"`
		}
		type outer struct {
			FirstName string `db:"first_name"`
			inner
		}
		rows := mustQueryx(t, db, "SELECT first_name, last_name, email FROM strict_person")
		defer rows.Close()
		if !rows.Next() {
			t.Fatal("expected a row")
		}
		var o outer
		_, err := rows.StrictStructScan(&o, StrictScanOptions{})
		if err != nil {
			t.Fatalf("deterministic embedded shadowing must scan: %v", err)
		}
		if o.FirstName != "Jason" {
			t.Errorf("outer first_name = %q", o.FirstName)
		}
		if o.inner.LastName != "Moiron" {
			t.Errorf("promoted last_name = %q", o.inner.LastName)
		}
	})
}

// Package-level StrictStructScan handles slices, closes the rows itself, and
// leaves the slice untouched when the mapping is rejected.
func TestStrictStructScanSlice(t *testing.T) {
	RunWithSchema(strictSchema, t, func(db *DB, t *testing.T, now string) {
		loadStrictFixture(db, t)

		rows := mustQueryx(t, db, "SELECT id, first_name, last_name, email, mood FROM strict_person ORDER BY id")
		var people []strictPerson
		report, err := StrictStructScan(rows, &people, StrictScanOptions{})
		if err != nil {
			t.Fatalf("exact slice scan: %v", err)
		}
		if len(people) != 2 || report.RowsScanned != 2 {
			t.Fatalf("scanned %d rows, report=%+v", len(people), report)
		}
		if people[0].FirstName != "Jason" || people[1].Mood != "sleepy" {
			t.Errorf("unexpected rows: %+v", people)
		}
		if len(report.MissingFields) != 0 || len(report.UnknownColumns) != 0 || report.Partial {
			t.Errorf("exact report should be complete: %+v", report)
		}

		// rejected mapping must not reset or write the slice; the rows are
		// still the caller's to close because no row was ever read.
		seed := []strictPerson{{ID: 99}}
		rows = mustQueryx(t, db, "SELECT id FROM strict_person ORDER BY id")
		_, err = StrictStructScan(rows, &seed, StrictScanOptions{})
		strictError(t, err)
		if len(seed) != 1 || seed[0].ID != 99 {
			t.Errorf("rejected slice scan mutated dest: %+v", seed)
		}
		rows.Close()

		// partial slice scan: the result omits first_name and adds mood.
		rows = mustQueryx(t, db, "SELECT id, mood FROM strict_person ORDER BY id")
		var slim []strictPersonSlim
		report, err = StrictStructScan(rows, &slim, StrictScanOptions{Incomplete: StrictAllowPartial})
		if err != nil {
			t.Fatalf("partial slice scan: %v", err)
		}
		if !report.Partial || report.RowsScanned != 2 || len(slim) != 2 {
			t.Errorf("partial slice result report=%+v len=%d", report, len(slim))
		}
		if !reflect.DeepEqual(report.MissingFields, []string{"first_name"}) ||
			!reflect.DeepEqual(report.UnknownColumns, []string{"mood"}) {
			t.Errorf("partial slice audit = %+v", report)
		}
	})
}

// Row.StrictStructScan honors sql.ErrNoRows and closes the rows itself.
func TestStrictRowScan(t *testing.T) {
	RunWithSchema(strictSchema, t, func(db *DB, t *testing.T, now string) {
		loadStrictFixture(db, t)

		r := db.QueryRowx("SELECT id, first_name FROM strict_person WHERE id = 1")
		var p strictPersonSlim
		report, err := r.StrictStructScan(&p, StrictScanOptions{})
		if err != nil {
			t.Fatalf("row exact scan: %v", err)
		}
		if p.ID != 1 || p.FirstName != "Jason" || report.RowsScanned != 1 {
			t.Errorf("row dest = %+v report=%+v", p, report)
		}

		// no rows: sql.ErrNoRows must remain identifiable.
		r = db.QueryRowx("SELECT id, first_name FROM strict_person WHERE id = 9999")
		var p2 strictPersonSlim
		report, err = r.StrictStructScan(&p2, StrictScanOptions{})
		if !errors.Is(err, sql.ErrNoRows) {
			t.Errorf("expected sql.ErrNoRows, got %v", err)
		}
		if report == nil || report.RowsScanned != 0 {
			t.Errorf("no-row report = %+v", report)
		}
		if p2.ID != 0 {
			t.Error("no-row scan wrote into dest")
		}

		// incomplete mapping on a row is rejected before reading.
		r = db.QueryRowx("SELECT id FROM strict_person WHERE id = 1")
		var p3 strictPersonSlim
		_, err = r.StrictStructScan(&p3, StrictScanOptions{})
		strictError(t, err)
		if p3.ID != 0 {
			t.Error("rejected row scan wrote into dest")
		}

		// partial policy on a row: matched fields land, the rest are reported.
		r = db.QueryRowx("SELECT id, mood FROM strict_person WHERE id = 1")
		p4 := strictPersonSlim{FirstName: "kept"}
		report, err = r.StrictStructScan(&p4, StrictScanOptions{Incomplete: StrictAllowPartial})
		if err != nil {
			t.Fatalf("partial row scan: %v", err)
		}
		if !report.Partial || p4.ID != 1 || p4.FirstName != "kept" {
			t.Errorf("partial row dest=%+v report=%+v", p4, report)
		}
		if !reflect.DeepEqual(report.UnknownColumns, []string{"mood"}) ||
			!reflect.DeepEqual(report.MissingFields, []string{"first_name"}) {
			t.Errorf("partial row audit = %+v", report)
		}
	})
}

// Requirement 5: strict mode works through prepared statements and context
// queries with the same caching semantics, and context cancellation remains
// observable as the underlying error.
func TestStrictStructScanStmtAndContext(t *testing.T) {
	RunWithSchema(strictSchema, t, func(db *DB, t *testing.T, now string) {
		loadStrictFixture(db, t)

		stmt, err := db.Preparex("SELECT id, first_name FROM strict_person ORDER BY id")
		if err != nil {
			t.Fatal(err)
		}
		defer stmt.Close()
		rows, err := stmt.Queryx()
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		count := 0
		for rows.Next() {
			var p strictPersonSlim
			report, err := rows.StrictStructScan(&p, StrictScanOptions{})
			if err != nil {
				t.Fatalf("stmt strict scan: %v", err)
			}
			if !report.Partial {
				// cache reuse: the cached plan must be identical per row.
				if fmt.Sprint(report.FilledFields) != "[first_name id]" {
					t.Errorf("unexpected cached report %+v", report)
				}
			}
			count++
		}
		if count != 2 {
			t.Errorf("stmt scanned %d rows", count)
		}

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		rows, err = db.QueryxContext(ctx, "SELECT id, first_name FROM strict_person")
		if err != nil {
			// driver may reject the query outright; the error must pass through.
			if !errors.Is(err, context.Canceled) {
				t.Logf("query-time cancellation error: %v", err)
			}
			return
		}
		defer rows.Close()
		for rows.Next() {
			var p strictPersonSlim
			if _, err := rows.StrictStructScan(&p, StrictScanOptions{}); err != nil {
				if !errors.Is(err, context.Canceled) {
					t.Errorf("cancellation error not identifiable: %v", err)
				}
				return
			}
		}
		if err := rows.Err(); err != nil && !errors.Is(err, context.Canceled) {
			t.Logf("rows.Err after cancellation: %v", err)
		}
	})
}

// Requirement 5: ordinary SliceScan, Get and Select are unaffected by the
// strict additions, including their historic error messages.
func TestNonStrictPathsUnaffected(t *testing.T) {
	RunWithSchema(strictSchema, t, func(db *DB, t *testing.T, now string) {
		loadStrictFixture(db, t)

		var slim []strictPersonSlim
		err := db.Select(&slim, "SELECT id, first_name FROM strict_person ORDER BY id")
		if err != nil || len(slim) != 2 {
			t.Fatalf("Select changed behavior: %v %+v", err, slim)
		}

		var one strictPersonSlim
		err = db.Get(&one, "SELECT id, first_name FROM strict_person WHERE id = 1")
		if err != nil || one.FirstName != "Jason" {
			t.Fatalf("Get changed behavior: %v %+v", err, one)
		}

		sliceRows := mustQueryx(t, db, "SELECT id, first_name FROM strict_person ORDER BY id")
		defer sliceRows.Close()
		if !sliceRows.Next() {
			t.Fatal("expected a row")
		}
		slice, err := sliceRows.SliceScan()
		if err != nil || len(slice) != 2 {
			t.Errorf("SliceScan changed behavior: %v %v", err, slice)
		}

		// the classic unknown-column error text must be unchanged.
		legacyRows := mustQueryx(t, db, "SELECT id, mood FROM strict_person")
		defer legacyRows.Close()
		legacyRows.Next()
		err = legacyRows.StructScan(&strictPersonSlim{})
		if err == nil || err.Error() != "missing destination name mood in *sqlx.strictPersonSlim" {
			t.Errorf("legacy StructScan error changed: %v", err)
		}
	})
}
