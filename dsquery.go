// Package dsquery provides helpers for composing and executing Google Cloud
// Datastore queries. It wraps datastore.Query objects with logical operators
// such as AND/OR and supports optional caching of results.
package dsquery

import (
	"cloud.google.com/go/datastore"
	"context"
	"fmt"
	"sync"
	"time"
)

// Query interface
type Query interface {
	// Query function runs the queries as per data structure
	Query(dsClient DatastoreClient, ctx context.Context) ([]*datastore.Key, error)
	// Count of all queries
	Len() int
}

type DatastoreClient interface {
	GetAll(ctx context.Context, q *datastore.Query, dst interface{}) (keys []*datastore.Key, err error)
}

// Extracts all the datastore keys from a `map[string]*datastore.Key`
func ExtractMapStringKeysKey(m map[string]*datastore.Key) []*datastore.Key {
	result := make([]*datastore.Key, 0, len(m))
	for _, v := range m {
		if v == nil {
			continue
		}
		result = append(result, v)
	}
	return result
}

// Base entity
type Base struct {
	// Data store queries to run
	Queries []*datastore.Query
	// Sub queries after that to run
	SubQueries []Query
	// Provided for your convenience when debugging
	Name string
}

// AND Query
type And Base

// Count of all queries
func (qa *And) Len() int {
	return len(qa.SubQueries) + len(qa.Queries)
}

// Query function
func (qa *And) Query(dsClient DatastoreClient, ctx context.Context) ([]*datastore.Key, error) {
	m := map[string]*datastore.Key{}
	v := 0
	for i, q := range qa.Queries {
		v++
		if v > 1 && len(m) == 0 {
			return []*datastore.Key{}, nil
		}
		keys, err := dsClient.GetAll(ctx, q.KeysOnly(), nil)
		if err != nil {
			return nil, fmt.Errorf("query error in %s:%d error %w", qa.Name, i, err)
		}
		if v == 1 {
			for _, k := range keys {
				if k == nil {
					continue
				}
				m[k.Encode()] = k
			}
		} else {
			m = DSKeyMapMergeAnd(m, keys)
		}
	}
	for i, q := range qa.SubQueries {
		v++
		if v > 1 && len(m) == 0 {
			return []*datastore.Key{}, nil
		}
		keys, err := q.Query(dsClient, ctx)
		if err != nil {
			return nil, fmt.Errorf("query error in subquery %s:%d error %w", qa.Name, i, err)
		}
		if v == 1 {
			for _, k := range keys {
				if k == nil {
					continue
				}
				m[k.Encode()] = k
			}
		} else {
			m = DSKeyMapMergeAnd(m, keys)
		}
	}

	return ExtractMapStringKeysKey(m), nil
}

// Helper function to merge a set with an array, and produce a set of datastore keys.
func DSKeyMapMergeAnd(m map[string]*datastore.Key, keys []*datastore.Key) map[string]*datastore.Key {
	s := len(m)
	if s > len(keys) {
		s = len(keys)
	}
	m2 := make(map[string]*datastore.Key, s)
	for _, k := range keys {
		if k == nil {
			continue
		}
		ks := k.Encode()
		if _, ok := m[ks]; ok {
			m2[ks] = k
		}
	}
	return m2
}

// Helper function to remove keys from a set and produce a new set of datastore keys.
func DSKeyMapMergeNot(m map[string]*datastore.Key, keys []*datastore.Key) map[string]*datastore.Key {
	m2 := make(map[string]*datastore.Key, len(m))
	for k, v := range m {
		m2[k] = v
	}
	for _, k := range keys {
		if k == nil {
			continue
		}
		delete(m2, k.Encode())
	}
	return m2
}

// The NOT query
type Not Base

// Count of all queries
func (qn *Not) Len() int {
	return len(qn.SubQueries) + len(qn.Queries)
}

// Query function
func (qn *Not) Query(dsClient DatastoreClient, ctx context.Context) ([]*datastore.Key, error) {
	m := map[string]*datastore.Key{}
	for i, q := range qn.Queries {
		keys, err := dsClient.GetAll(ctx, q.KeysOnly(), nil)
		if err != nil {
			return nil, fmt.Errorf("query error in %s:%d error %w", qn.Name, i, err)
		}
		for _, k := range keys {
			if k == nil {
				continue
			}
			m[k.Encode()] = k
		}
	}
	for i, q := range qn.SubQueries {
		keys, err := q.Query(dsClient, ctx)
		if err != nil {
			return nil, fmt.Errorf("query error in subquery %s:%d error %w", qn.Name, i, err)
		}
		m = DSKeyMapMergeNot(m, keys)
	}

	return ExtractMapStringKeysKey(m), nil
}

// The OR query
type Or Base

// Count of all queries
func (qo *Or) Len() int {
	return len(qo.SubQueries) + len(qo.Queries)
}

// Query function
func (qo *Or) Query(dsClient DatastoreClient, ctx context.Context) ([]*datastore.Key, error) {
	l := sync.Mutex{}
	m := map[string]*datastore.Key{}
	errChan := make(chan error)
	add := func(keys []*datastore.Key) {
		l.Lock()
		defer l.Unlock()
		for _, k := range keys {
			if k == nil {
				continue
			}
			m[k.Encode()] = k
		}
	}
	for i, q := range qo.Queries {
		go func(i int, q *datastore.Query) {
			keys, err := dsClient.GetAll(ctx, q.KeysOnly(), nil)
			if err != nil {
				errChan <- fmt.Errorf("query error in %s:%d error %w", qo.Name, i, err)
				return
			}
			add(keys)
			errChan <- nil
		}(i, q)
	}
	for i, q := range qo.SubQueries {
		go func(i int, q Query) {
			keys, err := q.Query(dsClient, ctx)
			if err != nil {
				errChan <- fmt.Errorf("query error in subquery %s:%d error %w", qo.Name, i, err)
				return
			}
			add(keys)
			errChan <- nil
		}(i, q)
	}
	var err error
	for c := qo.Len(); c > 0; c-- {
		r := <-errChan
		if r != nil {
			err = r
		}
	}
	close(errChan)
	return ExtractMapStringKeysKey(m), err
}

// An object that contains just a single query, provided for debugging or intentinally ordering queries in a particular way
type Ident struct {
	StoredQuery *datastore.Query
	Name        string
}

// Count of all queries
func (qi *Ident) Len() int {
	if qi.StoredQuery != nil {
		return 1
	}
	return 0
}

// Query function
func (qi *Ident) Query(dsClient DatastoreClient, ctx context.Context) ([]*datastore.Key, error) {
	keys, err := dsClient.GetAll(ctx, qi.StoredQuery.KeysOnly(), nil)
	if err != nil {
		return nil, fmt.Errorf("query error in %s error %w", qi.Name, err)
	}
	return keys, nil
}

// An object that contains just a single query, provides thread safe caching
type Cached struct {
	StoredQuery   Query
	StoredResults []*datastore.Key
	Name          string
	TTL           time.Duration
	Expiration    time.Time
	sync.RWMutex
}

// Count of all queries
func (c *Cached) Len() int {
	c.RWMutex.RLock()
	defer c.RWMutex.RUnlock()
	if c.StoredQuery != nil {
		return c.StoredQuery.Len()
	}
	return 0
}

// Query function
func (c *Cached) Query(dsClient DatastoreClient, ctx context.Context) ([]*datastore.Key, error) {
	c.RWMutex.RLock()
	if c.StoredResults != nil && (c.Expiration.IsZero() || time.Now().Before(c.Expiration)) {
		c.RWMutex.RUnlock()
		return c.StoredResults, nil
	}
	c.RWMutex.RUnlock()

	c.RWMutex.Lock()
	defer c.RWMutex.Unlock()

	if c.StoredResults != nil && (c.Expiration.IsZero() || time.Now().Before(c.Expiration)) {
		return c.StoredResults, nil
	}

	keys, err := c.StoredQuery.Query(dsClient, ctx)
	if err != nil {
		return nil, fmt.Errorf("query error in %s error %w", c.Name, err)
	}
	c.StoredResults = keys
	if c.TTL > 0 {
		c.Expiration = time.Now().Add(c.TTL)
	}
	return c.StoredResults, nil
}
