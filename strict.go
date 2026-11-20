package sqlx

import (
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"

	"github.com/jmoiron/sqlx/reflectx"
)

// StrictIncompletePolicy decides what a strict scan does when the result
// columns and the destination struct fields do not line up exactly.
type StrictIncompletePolicy int

const (
	// StrictRejectIncomplete is the default zero-value policy.  A strict
	// scan fails before the first row is read whenever a target field has
	// no matching result column, a result column has no matching struct
	// field, or the ownership of a column cannot be determined (duplicate
	// result columns or two non-embedded struct fields claiming the same
	// db name).  The destination is never modified in this case, so zero
	// values can never be mistaken for scanned data.
	StrictRejectIncomplete StrictIncompletePolicy = iota

	// StrictAllowPartial permits a strict scan to proceed when columns and
	// fields do not line up exactly.  Only matched fields are written; the
	// returned MappingReport has Partial set to true and lists every
	// missing target field and unknown result column.  Ambiguous ownership
	// (duplicate columns / colliding db tags) is still rejected before the
	// first row is read.
	StrictAllowPartial
)

// StrictScanOptions configures a strict struct scan.  The zero value is valid
// and requests the most conservative behavior: incomplete mappings are
// rejected before any row is read.
type StrictScanOptions struct {
	// Incomplete controls whether an inexact column/field mapping is an
	// error or an explicitly reported partial result.
	Incomplete StrictIncompletePolicy
}

// MappingReport is the auditable result of planning and running a strict
// struct scan.  Every column or field name it carries is taken from the
// mapper's name space, so the same struct and result set always produce the
// same lists; slices are sorted and de-duplicated.
type MappingReport struct {
	// Columns are the result columns reported by the driver, in order.
	Columns []string

	// FilledFields lists the result columns that were mapped to a struct
	// field and handed to the driver's Scan.
	FilledFields []string

	// MissingFields lists mapped struct leaf fields that no result column
	// populated.  Nested fields use their dotted db path, e.g. "address.city".
	MissingFields []string

	// UnknownColumns lists result columns for which no struct field exists.
	UnknownColumns []string

	// DuplicateColumns lists result columns that appear more than once and
	// therefore have no unambiguous destination.
	DuplicateColumns []string

	// AmbiguousFields lists db names claimed by two or more non-embedded
	// struct fields.  Deterministic embedded-field shadowing, which follows
	// the normal mapper rules, is not reported here.
	AmbiguousFields []string

	// Partial is true when the mapping was inexact and, under
	// StrictAllowPartial, only part of the destination could be populated.
	Partial bool

	// RowsScanned is the number of rows successfully scanned.  A scan or
	// close failure leaves it at the count reached before the failure.
	RowsScanned int
}

// incomplete reports whether the planned mapping fails to cover the whole
// destination/result pair.
func (r *MappingReport) incomplete() bool {
	return len(r.MissingFields) != 0 ||
		len(r.UnknownColumns) != 0 ||
		len(r.DuplicateColumns) != 0 ||
		len(r.AmbiguousFields) != 0
}

// StrictScanError combines the mapping report with the first error encountered
// while planning or running a strict scan.  Use errors.As to extract the
// report for auditing and errors.Is / errors.Unwrap to reach the underlying
// database/sql or driver error.
type StrictScanError struct {
	// Report is never nil and always contains the column/field audit.
	Report *MappingReport
	// Phase is one of "columns", "mapping", "scan" or "close".
	Phase string
	// Err is the wrapped underlying error, nil only for a mapping rejection.
	Err error
}

func (e *StrictScanError) Error() string {
	var b strings.Builder
	b.WriteString("sqlx: strict struct scan")
	switch e.Phase {
	case "columns":
		b.WriteString(": cannot determine result columns: ")
		b.WriteString(e.Err.Error())
		return b.String()
	case "scan":
		fmt.Fprintf(&b, " failed while scanning row %d: %s", e.Report.RowsScanned, e.Err.Error())
		return b.String()
	case "close":
		b.WriteString(": closing rows failed: ")
		b.WriteString(e.Err.Error())
		return b.String()
	}

	b.WriteString(": incomplete or ambiguous column mapping")
	if len(e.Report.MissingFields) != 0 {
		fmt.Fprintf(&b, "; missing destination fields %s", quoteList(e.Report.MissingFields))
	}
	if len(e.Report.UnknownColumns) != 0 {
		fmt.Fprintf(&b, "; unknown result columns %s", quoteList(e.Report.UnknownColumns))
	}
	if len(e.Report.DuplicateColumns) != 0 {
		fmt.Fprintf(&b, "; duplicate result columns %s", quoteList(e.Report.DuplicateColumns))
	}
	if len(e.Report.AmbiguousFields) != 0 {
		fmt.Fprintf(&b, "; ambiguous destination fields %s", quoteList(e.Report.AmbiguousFields))
	}
	return b.String()
}

// Unwrap exposes the underlying error so errors.Is and errors.As keep working
// against database/sql sentinels (e.g. sql.ErrNoRows) and driver errors.
func (e *StrictScanError) Unwrap() error { return e.Err }

func quoteList(names []string) string {
	quoted := make([]string, len(names))
	for i, n := range names {
		quoted[i] = strconv.Quote(n)
	}
	return "[" + strings.Join(quoted, ", ") + "]"
}

// strictPlan holds the per-result-set reflect work, cached on Rows exactly
// like the non-strict StructScan cache.
type strictPlan struct {
	report    *MappingReport
	fields    [][]int
	structTyp reflect.Type
}

// scanError wraps a row-level failure with the current report.
func scanError(plan *strictPlan, err error) *StrictScanError {
	return &StrictScanError{Report: plan.report, Phase: "scan", Err: err}
}

// buildStrictPlan compares columns against the struct type using the given
// mapper and produces the audit report and traversals.  It performs no
// scanning and never touches the destination value.
func buildStrictPlan(m *reflectx.Mapper, typ reflect.Type, columns []string) *strictPlan {
	typ = reflectx.Deref(typ)
	tm := m.TypeMap(typ)
	traversals := m.TraversalsByName(typ, columns)

	report := &MappingReport{
		Columns: append([]string(nil), columns...),
	}

	filledSet := map[string]struct{}{}
	unknownSet := map[string]struct{}{}
	duplicateSet := map[string]struct{}{}
	seenColumns := map[string]struct{}{}

	for i, column := range columns {
		if _, ok := seenColumns[column]; ok {
			duplicateSet[column] = struct{}{}
		}
		seenColumns[column] = struct{}{}

		if len(traversals[i]) == 0 {
			unknownSet[column] = struct{}{}
			continue
		}
		filledSet[column] = struct{}{}
	}

	// A db name has no single deterministic owner when two fields claim it
	// at the same struct depth.  Go's promotion rules resolve different
	// depths deterministically (the shallower field wins, which is also the
	// order the mapper records them), so embedded shadowing is not
	// ambiguous.  A tie between sibling fields, or two embedded structs
	// promoting the same name to the same depth, cannot be owned by one
	// field and must fail before the first row is read.
	depthsByName := map[string][]int{}
	for _, fi := range tm.Index {
		if fi.Embedded {
			continue
		}
		depthsByName[fi.Path] = append(depthsByName[fi.Path], len(fi.Index))
	}
	ambiguousSet := map[string]struct{}{}
	for path, depths := range depthsByName {
		shallowest := depths[0]
		for _, d := range depths[1:] {
			if d < shallowest {
				shallowest = d
			}
		}
		claimants := 0
		for _, d := range depths {
			if d == shallowest {
				claimants++
			}
		}
		if claimants > 1 {
			ambiguousSet[path] = struct{}{}
		}
	}

	// Build the set of auditable leaf names.  The mapper expands every
	// struct, including scannable ones such as sql.NullString, into dotted
	// sub-paths; those internals are never independently addressable, so any
	// name whose nearest containing struct is scannable is dropped here.
	scannableContainers := map[string]bool{}
	for name, fi := range tm.Names {
		if isScannable(reflectx.Deref(fi.Field.Type)) {
			scannableContainers[name] = true
		}
	}
	leafByName := map[string]bool{}
	for name, fi := range tm.Names {
		if insideScannable(name, scannableContainers) {
			continue
		}
		if scannableContainers[name] || !hasMappedChild(tm, fi) {
			leafByName[name] = true
		}
	}
	for name := range leafByName {
		if _, ok := filledSet[name]; ok {
			continue
		}
		if coveredByFilled(name, filledSet) {
			continue
		}
		report.MissingFields = append(report.MissingFields, name)
	}

	for name := range filledSet {
		report.FilledFields = append(report.FilledFields, name)
	}

	report.UnknownColumns = sortedKeys(unknownSet)
	report.DuplicateColumns = sortedKeys(duplicateSet)
	report.AmbiguousFields = sortedKeys(ambiguousSet)
	report.MissingFields = sortedStrings(report.MissingFields)
	report.FilledFields = sortedStrings(report.FilledFields)
	report.Partial = report.incomplete()

	return &strictPlan{report: report, fields: traversals, structTyp: typ}
}

// hasMappedChild reports whether fi is a pure container whose own nested
// fields are independently mappable (dotted paths such as "address.city").
// Leaf structs without exported mapped fields, such as sql.NullString, return
// false and are scanned as a single value.
func hasMappedChild(tm *reflectx.StructMap, fi *reflectx.FieldInfo) bool {
	prefix := fi.Path + "."
	for other := range tm.Names {
		if strings.HasPrefix(other, prefix) {
			return true
		}
	}
	return false
}

func coveredByFilled(name string, filled map[string]struct{}) bool {
	prefix := name + "."
	for filledName := range filled {
		if filledName == name || strings.HasPrefix(filledName, prefix) {
			return true
		}
	}
	return false
}

// insideScannable reports whether name is a dotted descendant of one of the
// scannable container paths.
func insideScannable(name string, scannable map[string]bool) bool {
	for container := range scannable {
		if strings.HasPrefix(name, container+".") {
			return true
		}
	}
	return false
}

func sortedKeys(set map[string]struct{}) []string {
	if len(set) == 0 {
		return nil
	}
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func sortedStrings(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	sort.Strings(in)
	return in
}

// mappingError rejects an inexact plan according to the configured policy.
func mappingError(plan *strictPlan, opts StrictScanOptions) *StrictScanError {
	if !plan.report.incomplete() {
		return nil
	}
	if opts.Incomplete == StrictAllowPartial &&
		len(plan.report.DuplicateColumns) == 0 &&
		len(plan.report.AmbiguousFields) == 0 {
		return nil
	}
	return &StrictScanError{Report: plan.report, Phase: "mapping"}
}

// columnsError wraps a failure to read the result set's column metadata.
func columnsError(report *MappingReport, err error) *StrictScanError {
	return &StrictScanError{Report: report, Phase: "columns", Err: err}
}

// closeError wraps a failure returned by Rows.Close.
func closeError(report *MappingReport, err error) *StrictScanError {
	return &StrictScanError{Report: report, Phase: "close", Err: err}
}

func newStrictReport(columns []string) *MappingReport {
	return &MappingReport{Columns: append([]string(nil), columns...)}
}

// StrictStructScan scans the current row into dest under strict, auditable
// mapping rules.  It mirrors StructScan's manual-iteration usage: call Next
// first and reuse the same *Rows instance across rows to share the cached
// traversals.
//
// On success the returned *MappingReport is never nil; it lists every filled
// field and, when StrictScanOptions allows it, any missing fields or unknown
// columns that made the result partial.  On failure the returned error is a
// *StrictScanError carrying the same report; use errors.As to audit it and
// errors.Is to reach the underlying driver error.
func (r *Rows) StrictStructScan(dest interface{}, opts StrictScanOptions) (*MappingReport, error) {
	v := reflect.ValueOf(dest)
	if v.Kind() != reflect.Ptr {
		return nil, errors.New("must pass a pointer, not a value, to StructScan destination")
	}
	if v.IsNil() {
		return nil, errors.New("nil pointer passed to StructScan destination")
	}
	v = v.Elem()
	if v.Kind() != reflect.Struct {
		return nil, structOnlyError(v.Type())
	}

	if !r.strictStarted {
		columns, err := r.Columns()
		if err != nil {
			return nil, columnsError(newStrictReport(nil), err)
		}
		plan := buildStrictPlan(r.Mapper, v.Type(), columns)
		if err := mappingError(plan, opts); err != nil {
			return plan.report, err
		}
		r.strictPlan = plan
		r.strictValues = make([]interface{}, len(columns))
		r.strictStarted = true
	}
	plan := r.strictPlan

	// StructScan caches traversals per Rows instance; reusing the same Rows
	// with a different struct type cannot re-plan safely, mirroring the
	// non-strict StructScan contract.
	if plan.structTyp != v.Type() {
		return nil, fmt.Errorf("sqlx: strict StructScan type mismatch: Rows planned for %s but got %s",
			plan.structTyp, v.Type())
	}

	// Re-evaluate the policy even when reusing the cached plan, so switching
	// the option mid-iteration never silently changes the cached mapping.
	if err := mappingError(plan, opts); err != nil {
		return plan.report, err
	}

	if err := fieldsByTraversal(v, plan.fields, r.strictValues, true); err != nil {
		return plan.report, scanError(plan, err)
	}
	if err := r.Scan(r.strictValues...); err != nil {
		return plan.report, scanError(plan, err)
	}
	if err := r.Err(); err != nil {
		return plan.report, scanError(plan, err)
	}
	plan.report.RowsScanned++
	return plan.report, nil
}

// StrictStructScan scans the single row backing this Row under strict,
// auditable mapping rules.  The underlying rows are closed when the call
// returns, matching Row.StructScan's non-strict behavior.
func (r *Row) StrictStructScan(dest interface{}, opts StrictScanOptions) (*MappingReport, error) {
	if r.err != nil {
		return nil, r.err
	}
	if r.rows == nil {
		r.err = sql.ErrNoRows
		return nil, r.err
	}
	defer r.rows.Close()

	v := reflect.ValueOf(dest)
	if v.Kind() != reflect.Ptr {
		return nil, errors.New("must pass a pointer, not a value, to StructScan destination")
	}
	if v.IsNil() {
		return nil, errors.New("nil pointer passed to StructScan destination")
	}
	base := reflectx.Deref(v.Type())
	if base.Kind() != reflect.Struct || isScannable(base) {
		return nil, structOnlyError(base)
	}

	columns, err := r.Columns()
	if err != nil {
		return nil, columnsError(newStrictReport(nil), err)
	}
	plan := buildStrictPlan(r.Mapper, base, columns)
	if err := mappingError(plan, opts); err != nil {
		return plan.report, err
	}

	if !r.rows.Next() {
		if err := r.rows.Err(); err != nil {
			return plan.report, scanError(plan, err)
		}
		return plan.report, scanError(plan, sql.ErrNoRows)
	}

	values := make([]interface{}, len(columns))
	if err := fieldsByTraversal(v.Elem(), plan.fields, values, true); err != nil {
		return plan.report, scanError(plan, err)
	}
	if err := r.rows.Scan(values...); err != nil {
		return plan.report, scanError(plan, err)
	}
	plan.report.RowsScanned++
	return plan.report, nil
}

// StrictStructScan scans every row from rows into dest, which must be a
// pointer to a slice of structs, under strict auditable mapping rules.  It is
// the strict counterpart of the package-level StructScan.  The slice length
// is reset to zero only after the column mapping has been validated, so a
// rejected mapping leaves the destination (and any elements it already held)
// untouched.  The rows are closed before returning, including on failure, and
// a close failure is reported as a *StrictScanError in the "close" phase.
func StrictStructScan(rows rowsi, dest interface{}, opts StrictScanOptions) (*MappingReport, error) {
	value := reflect.ValueOf(dest)
	if value.Kind() != reflect.Ptr {
		return nil, errors.New("must pass a pointer, not a value, to StructScan destination")
	}
	if value.IsNil() {
		return nil, errors.New("nil pointer passed to StructScan destination")
	}
	direct := reflect.Indirect(value)

	slice, err := baseType(value.Type(), reflect.Slice)
	if err != nil {
		return nil, err
	}
	base := reflectx.Deref(slice.Elem())
	if isScannable(base) {
		return nil, structOnlyError(base)
	}

	columns, err := rows.Columns()
	if err != nil {
		return nil, columnsError(newStrictReport(nil), err)
	}

	m := mapper()
	if rx, ok := rows.(*Rows); ok {
		m = rx.Mapper
	}
	plan := buildStrictPlan(m, base, columns)
	if err := mappingError(plan, opts); err != nil {
		return plan.report, err
	}

	// The mapping is accepted; only now is it safe to discard prior contents.
	direct.SetLen(0)
	values := make([]interface{}, len(columns))
	isPtr := slice.Elem().Kind() == reflect.Ptr

	for rows.Next() {
		vp := reflect.New(base)
		v := reflect.Indirect(vp)
		if err := fieldsByTraversal(v, plan.fields, values, true); err != nil {
			return plan.report, scanError(plan, err)
		}
		if err := rows.Scan(values...); err != nil {
			return plan.report, scanError(plan, err)
		}
		if isPtr {
			direct.Set(reflect.Append(direct, vp))
		} else {
			direct.Set(reflect.Append(direct, v))
		}
		plan.report.RowsScanned++
	}
	if err := rows.Err(); err != nil {
		return plan.report, scanError(plan, err)
	}
	if err := rows.Close(); err != nil {
		return plan.report, closeError(plan.report, err)
	}
	return plan.report, nil
}
