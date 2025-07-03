package dsquery

import (
	"cloud.google.com/go/datastore"
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"
)

// Tested elsewhere TODO move here.

func TestExtractMapStringKeysKey(t *testing.T) {
	type args struct {
		m map[string]*datastore.Key
	}
	withEmpty := KeyMapCreate("1", "2", "3")
	withEmpty["a"] = nil
	tests := []struct {
		name string
		args args
		want []*datastore.Key
	}{
		{"Simple", args{m: KeyMapCreate("1", "2", "3")}, KeyArrayCreate("1", "2", "3")},
		{"Empty removal", args{m: withEmpty}, KeyArrayCreate("1", "2", "3")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ExtractMapStringKeysKey(tt.args.m); !KeyArraysEqual(got, tt.want) {
				t.Errorf("ExtractMapStringKeysKey() = %v, want %v", got, tt.want)
			}
		})
	}
}

func KeyArrayCreate(s ...string) []*datastore.Key {
	result := make([]*datastore.Key, len(s))
	for i, e := range s {
		result[i] = datastore.NameKey("asdf", e, nil)
	}
	return result
}

func KeyMapCreate(s ...string) map[string]*datastore.Key {
	result := map[string]*datastore.Key{}
	for _, e := range s {
		k := datastore.NameKey("asdf", e, nil)
		result[k.Encode()] = k
	}
	return result
}

type MockDSResult struct {
	r1   []*datastore.Key
	err  error
	Key  chan struct{}
	Lock chan struct{}
}

type MockDS struct {
	m []MockDSResult
	sync.Mutex
}

func (m *MockDS) GetAll(ctx context.Context, q *datastore.Query, dst interface{}) (keys []*datastore.Key, err error) {
	m.Lock()
	var r *MockDSResult
	if len(m.m) > 0 {
		r = &m.m[0]
		m.m = m.m[1:]
	}
	m.Unlock()
	if r != nil {
		if r.Lock != nil {
			<-r.Lock
		}
		if r.Key != nil {
			r.Key <- struct{}{}
		}
		return r.r1, r.err
	}
	panic("not implemented")
}

func TestAnd_Query(t *testing.T) {
	type args struct {
		dsClient DatastoreClient
		ctx      context.Context
	}
	q := datastore.NewQuery("")
	tests := []struct {
		name    string
		qa      *And
		args    args
		want    []*datastore.Key
		wantErr bool
		wantLen int
	}{
		{"Two queries 1 result", &And{Queries: []*datastore.Query{q, q}}, args{CreateMockDS1(), nil}, KeyArrayCreate("2"), false, 2},
		{"Two queries 2 results - values reversed", &And{Queries: []*datastore.Query{q, q}}, args{CreateMockDS2(), nil}, KeyArrayCreate("3", "2"), false, 2},
		{"Error passthrough query", &And{Queries: []*datastore.Query{q}}, args{CreateMockDSErr(), nil}, nil, true, 1},
		{"Error passthrough subquery", &And{SubQueries: []Query{&Error{errors.New("err")}}}, args{nil, nil}, nil, true, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.qa.Query(tt.args.dsClient, tt.args.ctx)
			if (err != nil) != tt.wantErr {
				t.Errorf("Query() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			l := tt.qa.Len()
			if l != tt.wantLen {
				t.Errorf("Len() = %v, wantLen %v", l, tt.wantLen)
				return
			}
			if !KeyArraysEqual(got, tt.want) {
				t.Errorf("Query() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestOr_Query(t *testing.T) {
	type args struct {
		dsClient DatastoreClient
		ctx      context.Context
	}
	q := datastore.NewQuery("")
	tests := []struct {
		name    string
		qo      *Or
		args    args
		want    []*datastore.Key
		wantErr bool
		wantLen int
	}{
		{"Two queries 1 result", &Or{Queries: []*datastore.Query{q, q}}, args{CreateMockDS1(), nil}, KeyArrayCreate("1", "2", "3"), false, 2},
		{"Two queries 2 results - values reversed", &Or{Queries: []*datastore.Query{q, q}}, args{CreateMockDS2(), nil}, KeyArrayCreate("3", "2"), false, 2},
		{"Two queries 3 results - reversed return", &Or{Queries: []*datastore.Query{q, q}}, args{CreateMockDS3(), nil}, KeyArrayCreate("2", "3", "4", "5", "1"), false, 2},
		{"Two sub queries 2 results - reversed return", &Or{SubQueries: MockSubQ1()}, args{nil, nil}, KeyArrayCreate("2", "3"), false, 2},
		{"Error passthrough query", &Or{Queries: []*datastore.Query{q}}, args{CreateMockDSErr(), nil}, nil, true, 1},
		{"Error passthrough subquery", &Or{SubQueries: []Query{&Error{errors.New("err")}}}, args{nil, nil}, nil, true, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.qo.Query(tt.args.dsClient, tt.args.ctx)
			if (err != nil) != tt.wantErr {
				t.Errorf("Query() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			l := tt.qo.Len()
			if l != tt.wantLen {
				t.Errorf("Len() = %v, wantLen %v", l, tt.wantLen)
				return
			}
			if !KeyArraysEqual(got, tt.want) {
				t.Errorf("Query() got = %v, want %v", got, tt.want)
			}
		})
	}
}

type StoredResult struct {
	r1  []*datastore.Key
	err error
}

func (s *StoredResult) Query(dsClient DatastoreClient, ctx context.Context) ([]*datastore.Key, error) {
	return s.r1, s.err
}

func (s *StoredResult) Len() int {
	return 1
}

func MockSubQ1() []Query {
	locks := []chan struct{}{
		make(chan struct{}, 1),
	}

	return []Query{
		&Lockable{StoredQuery: &StoredResult{KeyArrayCreate("3"), nil}, Lock: locks[0]},
		&Lockable{StoredQuery: &StoredResult{KeyArrayCreate("2"), nil}, Key: locks[0]},
	}
}

func KeyArraysEqual(a1 []*datastore.Key, a2 []*datastore.Key) bool {
	if len(a1) != len(a2) {
		return false
	}
	m := make(map[string]int, len(a1))
	for _, k := range a1 {
		m[k.String()] += 1
	}
	for _, k := range a2 {
		m[k.String()] -= 1
	}
	for _, v := range m {
		if v != 0 {
			return false
		}
	}
	return true
}

func CreateMockDS1() *MockDS {
	locks := []chan struct{}{
		make(chan struct{}, 1),
	}
	mockDs := &MockDS{
		m: []MockDSResult{
			{r1: KeyArrayCreate("1", "2"), Key: locks[0]},
			{r1: KeyArrayCreate("2", "3"), Lock: locks[0]},
		},
	}
	return mockDs
}

func CreateMockDSErr() *MockDS {
	mockDs := &MockDS{
		m: []MockDSResult{
			{err: errors.New("err")},
		},
	}
	return mockDs
}

func CreateMockDS2() *MockDS {
	locks := []chan struct{}{
		make(chan struct{}, 1),
	}
	mockDs := &MockDS{
		m: []MockDSResult{
			{r1: KeyArrayCreate("3", "2"), Key: locks[0]},
			{r1: KeyArrayCreate("3", "2"), Lock: locks[0]},
		},
	}
	return mockDs
}

func CreateMockDS3() *MockDS {
	locks := []chan struct{}{
		make(chan struct{}, 1),
	}
	mockDs := &MockDS{
		m: []MockDSResult{
			{r1: KeyArrayCreate("1", "2", "3", "4"), Lock: locks[0]},
			{r1: KeyArrayCreate("2", "3", "4", "5"), Key: locks[0]},
		},
	}
	return mockDs
}

type Count struct {
	Name         string
	Count        int
	StoredResult []*datastore.Key
}

func (qc *Count) Len() int {
	return 1
}

func (qc *Count) Query(dsClient DatastoreClient, ctx context.Context) ([]*datastore.Key, error) {
	qc.Count++
	return qc.StoredResult, nil
}

type Once struct {
	Count
}

var (
	ErrCountExceeded = errors.New("count exceeded")
)

func (qo *Once) Query(dsClient DatastoreClient, ctx context.Context) ([]*datastore.Key, error) {
	if qo.Count.Count > 0 {
		return nil, ErrCountExceeded
	}
	return qo.Count.Query(dsClient, ctx)
}

type Lockable struct {
	Key         chan struct{}
	Lock        chan struct{}
	StoredQuery Query
}

func (qo *Lockable) Len() int {
	return 1
}

func (qo *Lockable) Query(dsClient DatastoreClient, ctx context.Context) ([]*datastore.Key, error) {
	if qo.Lock != nil {
		<-qo.Lock
	}
	if qo.Key != nil {
		qo.Key <- struct{}{}
	}
	return qo.StoredQuery.Query(dsClient, ctx)
}

func TestIdent_Query(t *testing.T) {
	type fields struct {
		StoredQuery *datastore.Query
		Name        string
	}
	type args struct {
		dsClient DatastoreClient
		ctx      context.Context
	}
	q := datastore.NewQuery("")
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []*datastore.Key
		wantErr bool
		wantLen int
	}{
		{"Pass Through test", fields{StoredQuery: q}, args{CreateMockDS1(), nil}, KeyArrayCreate("1", "2"), false, 1},
		{"Error passthrough", fields{StoredQuery: q}, args{CreateMockDSErr(), nil}, nil, true, 1}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			qi := &Ident{
				StoredQuery: tt.fields.StoredQuery,
				Name:        tt.fields.Name,
			}
			got, err := qi.Query(tt.args.dsClient, tt.args.ctx)
			if (err != nil) != tt.wantErr {
				t.Errorf("Query() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			l := qi.Len()
			if l != tt.wantLen {
				t.Errorf("Len() = %v, wantLen %v", l, tt.wantLen)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Query() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCached_Query(t *testing.T) {
	type args struct {
		dsClient DatastoreClient
		ctx      context.Context
	}
	c2 := &Cached{StoredQuery: &Once{Count{StoredResult: KeyArrayCreate("a")}}, TTL: time.Second}
	o3 := &Once{Count{StoredResult: KeyArrayCreate("a")}}
	c4 := &Cached{StoredQuery: &Once{Count{StoredResult: KeyArrayCreate("a")}}, TTL: time.Second}
	tests := []struct {
		name    string
		query   Query
		args    args
		want    []*datastore.Key
		wantErr bool
		wantLen int
	}{
		{"One query is fine", &Cached{StoredQuery: &Once{Count{StoredResult: KeyArrayCreate("a")}}, TTL: time.Second}, args{nil, nil}, KeyArrayCreate("a"), false, 1},
		{"Twice only runs once", &And{SubQueries: []Query{c2, c2}}, args{nil, nil}, KeyArrayCreate("a"), false, 2},
		{"once run twice fails", &And{SubQueries: []Query{o3, o3}}, args{nil, nil}, nil, true, 2},
		{"Concurrent fine", &Or{SubQueries: []Query{c4, c4, c4, c4, c4, c4, c4, c4, c4, c4, c4, c4, c4, c4, c4}}, args{nil, nil}, KeyArrayCreate("a"), false, 15},
		{"Error passthrough", &Cached{StoredQuery: &Error{errors.New("err")}}, args{nil, nil}, nil, true, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.query.Query(tt.args.dsClient, tt.args.ctx)
			if (err != nil) != tt.wantErr {
				t.Errorf("Query() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			l := tt.query.Len()
			if l != tt.wantLen {
				t.Errorf("Len() = %v, wantLen %v", l, tt.wantLen)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Query() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNot_Query(t *testing.T) {
	type args struct {
		dsClient DatastoreClient
		ctx      context.Context
	}
	q := datastore.NewQuery("")
	tests := []struct {
		name    string
		nq      *Not
		args    args
		want    []*datastore.Key
		wantErr bool
		wantLen int
	}{
		{"Simple exclusion", &Not{Queries: []*datastore.Query{q}, SubQueries: []Query{&StoredResult{KeyArrayCreate("2"), nil}}}, args{CreateMockDS1(), nil}, KeyArrayCreate("1"), false, 2},
		{"Multiple queries", &Not{Queries: []*datastore.Query{q, q}, SubQueries: []Query{&StoredResult{KeyArrayCreate("2"), nil}}}, args{CreateMockDS1(), nil}, KeyArrayCreate("1", "3"), false, 3},
		{"Error passthrough query", &Not{Queries: []*datastore.Query{q}}, args{CreateMockDSErr(), nil}, nil, true, 1},
		{"Error passthrough subquery", &Not{SubQueries: []Query{&Error{errors.New("err")}}}, args{nil, nil}, nil, true, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.nq.Query(tt.args.dsClient, tt.args.ctx)
			if (err != nil) != tt.wantErr {
				t.Errorf("Query() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			l := tt.nq.Len()
			if l != tt.wantLen {
				t.Errorf("Len() = %v, wantLen %v", l, tt.wantLen)
				return
			}
			if !KeyArraysEqual(got, tt.want) {
				t.Errorf("Query() got = %v, want %v", got, tt.want)
			}
		})
	}
}

type Error struct{ error }

func (e *Error) Query(dsClient DatastoreClient, ctx context.Context) ([]*datastore.Key, error) {
	return nil, e.error
}

func (e *Error) Len() int {
	return 1
}

func TestCached_Expiration(t *testing.T) {
	c := &Count{StoredResult: KeyArrayCreate("a")}
	cache := &Cached{StoredQuery: c, TTL: 10 * time.Millisecond}

	if _, err := cache.Query(nil, nil); err != nil {
		t.Fatalf("first query error %v", err)
	}
	if c.Count != 1 {
		t.Fatalf("count = %d, want 1", c.Count)
	}

	if _, err := cache.Query(nil, nil); err != nil {
		t.Fatalf("second query error %v", err)
	}
	if c.Count != 1 {
		t.Fatalf("count changed before expiration = %d", c.Count)
	}

	time.Sleep(15 * time.Millisecond)

	if _, err := cache.Query(nil, nil); err != nil {
		t.Fatalf("third query error %v", err)
	}
	if c.Count != 2 {
		t.Fatalf("count after expiration = %d, want 2", c.Count)
	}
}

func TestCachedQuery_StoredResults(t *testing.T) {
	c := &Cached{
		StoredQuery:   &Count{StoredResult: KeyArrayCreate("b")},
		StoredResults: KeyArrayCreate("a"),
	}
	got, err := c.Query(nil, nil)
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	if !KeyArraysEqual(got, KeyArrayCreate("a")) {
		t.Errorf("Query() got = %v, want %v", got, KeyArrayCreate("a"))
	}
	if c.StoredQuery.(*Count).Count != 0 {
		t.Errorf("StoredQuery executed %d times, want 0", c.StoredQuery.(*Count).Count)
	}
}

func TestDSKeyMapMergeAnd(t *testing.T) {
	t.Run("map bigger than slice", func(t *testing.T) {
		m := KeyMapCreate("1", "2", "3")
		keys := KeyArrayCreate("2")
		got := DSKeyMapMergeAnd(m, keys)
		if !KeyArraysEqual(ExtractMapStringKeysKey(got), KeyArrayCreate("2")) {
			t.Errorf("DSKeyMapMergeAnd() = %v, want %v", got, KeyArrayCreate("2"))
		}
	})
	t.Run("nil key ignored", func(t *testing.T) {
		m := KeyMapCreate("1")
		keys := []*datastore.Key{nil, datastore.NameKey("asdf", "1", nil), nil}
		got := DSKeyMapMergeAnd(m, keys)
		if !KeyArraysEqual(ExtractMapStringKeysKey(got), KeyArrayCreate("1")) {
			t.Errorf("DSKeyMapMergeAnd() = %v, want %v", got, KeyArrayCreate("1"))
		}
	})
	t.Run("no intersection", func(t *testing.T) {
		m := KeyMapCreate("1")
		keys := KeyArrayCreate("2")
		got := DSKeyMapMergeAnd(m, keys)
		if len(got) != 0 {
			t.Errorf("expected empty result, got %v", got)
		}
	})
}

func TestDSKeyMapMergeNot(t *testing.T) {
	m := KeyMapCreate("1", "2")
	keys := []*datastore.Key{datastore.NameKey("asdf", "1", nil)}
	got := DSKeyMapMergeNot(m, keys)
	if !KeyArraysEqual(ExtractMapStringKeysKey(got), KeyArrayCreate("2")) {
		t.Errorf("DSKeyMapMergeNot() = %v, want %v", got, KeyArrayCreate("2"))
	}
}

func TestMapIntersect(t *testing.T) {
	m := map[string]string{"1": "a", "2": "b"}
	items := []string{"2", "3"}
	got := MapIntersect(m, items, func(v string) string { return v })
	want := map[string]string{"2": "2"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("MapIntersect() = %v, want %v", got, want)
	}
}

func TestMapExclude(t *testing.T) {
	m := map[string]string{"1": "a", "2": "b"}
	items := []string{"1"}
	got := MapExclude(m, items, func(v string) string { return v })
	want := map[string]string{"2": "b"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("MapExclude() = %v, want %v", got, want)
	}
}
