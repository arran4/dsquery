package dsquery

import (
	"cloud.google.com/go/datastore"
	"context"
	"fmt"
)

// staticDS is a simple DatastoreClient used in the examples.
// Each call to GetAll returns the next slice of keys from results.
type staticDS struct{ results [][]*datastore.Key }

func (m *staticDS) GetAll(ctx context.Context, q *datastore.Query, dst interface{}) ([]*datastore.Key, error) {
	if len(m.results) == 0 {
		return nil, nil
	}
	r := m.results[0]
	m.results = m.results[1:]
	return r, nil
}

// countQuery counts how many times it has been executed.
type countQuery struct {
	n    int
	keys []*datastore.Key
}

func (c *countQuery) Query(dsClient DatastoreClient, ctx context.Context) ([]*datastore.Key, error) {
	c.n++
	return c.keys, nil
}

func (c *countQuery) Len() int { return 1 }

// ExampleOr demonstrates combining multiple queries with Or.
func ExampleOr() {
	ds := &staticDS{results: [][]*datastore.Key{
		{datastore.NameKey("Fruit", "1", nil), datastore.NameKey("Fruit", "2", nil)},
		{datastore.NameKey("Fruit", "3", nil)},
	}}

	q := &Or{Queries: []*datastore.Query{
		datastore.NewQuery("Fruit").Filter("Color =", "Brown"),
		datastore.NewQuery("Fruit").Filter("Color =", "Orange"),
	}}

	keys, _ := q.Query(ds, context.Background())
	for _, k := range keys {
		fmt.Println(k.Name)
	}
	// Output:
	// 1
	// 2
	// 3
}

// ExampleAnd demonstrates using And with subqueries.
func ExampleAnd() {
	ds := &staticDS{results: [][]*datastore.Key{
		{datastore.NameKey("Fruit", "1", nil), datastore.NameKey("Fruit", "2", nil), datastore.NameKey("Fruit", "3", nil)},
		{datastore.NameKey("Fruit", "2", nil), datastore.NameKey("Fruit", "3", nil), datastore.NameKey("Fruit", "4", nil)},
	}}

	q := &And{Queries: []*datastore.Query{
		datastore.NewQuery("Fruit").Filter("Color =", "Red"),
		datastore.NewQuery("Fruit").Filter("Producers =", "USA"),
	}}

	keys, _ := q.Query(ds, context.Background())
	for _, k := range keys {
		fmt.Println(k.Name)
	}
	// Output:
	// 2
	// 3
}

// ExampleCached shows how Cached prevents a query from running more than once.
func ExampleCached() {
	q := &countQuery{keys: []*datastore.Key{datastore.NameKey("Fruit", "1", nil)}}
	cached := &Cached{StoredQuery: q}

	cached.Query(nil, context.Background())
	cached.Query(nil, context.Background())

	fmt.Println(q.n)
	// Output: 1
}
