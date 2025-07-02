# dsquery

This is a simple "query builder / executor" for Google's Datastore.

For the moment it is simple, but I would like to build it out with some better features.

The primary purpose right now is to make some go based queries more readable or at least more consistent especially.

## How it works

All this library does is provide wrappers which can be used to wrap existing datastore queries.

It allows you to preform operations like "and" or "or" between two queries
in a more standard way.

It runs key-only queries meaning you will need to query the keys with the resulting query
yourself with a GetMulti.

## Examples

Given this data structure:
```go
package example

import (
	"cloud.google.com/go/datastore"
)

const (
	FruitKind = "Fruit"
)

type Fruit struct {
	DSKey         *datastore.Key `datastore:"__key__"`
	Name          string         `datastore:",noindex"`
	Color         string
	Producers     []string
}
```

And the data

| Key | Name | Color | Producers |
| --- | --- | --- | --- |
| `123` | `Potato` | `Brown` | `[Ireland, Peru, Scotland]` |
| `234` | `Orange` | `Orange` | `[USA, Brazil, China]` |
| `345` | `Chilli` | `Red` | `[Mexico, Turkey, China]` |

Say for the query:
* All Fruits that are Orange or Red
```go
    fruitQuery := &dsquery.Or{
        Name: "root fruit query",
		Queries:    []*datastore.Query{
            datastore.NewQuery(FruitKind).Filter("Color =", "Orange"),
            datastore.NewQuery(FruitKind).Filter("Color =", "Red"),
        },
		SubQueries: nil,
    }

	keys, err := fruitQuery.Query(dsClient, request.Context())
    if err != nil {
        logger.Errorf("Query error %v", err)
        return
    }

	logger.Infof("Running GetMulti for %d fruit", len(keys))
	fruits := make([]*Fruit, len(keys), len(keys))
	if err := dsClient.GetMulti(request.Context(), keys, fruits); err != nil {
		logger.Errorf("GetMulti error %v", err)
		return err
	}

```
Should return: ```Orange and Chilli```

Say for the query:
* All Fruits that are (Orange or Red) AND (Grown in China OR USA)
```go
    fruitQuery := &dsquery.And{
        Name: "root fruit query",
        Queries:    []*datastore.Query{},
        SubQueries: []dsquery.Query{
            &dsquery.Or{
                Name: "color query",
                Queries:    []*datastore.Query{
                datastore.NewQuery(FruitKind).Filter("Color =", "Orange"),
                datastore.NewQuery(FruitKind).Filter("Color =", "Red"),
            },
            &dsquery.Or{
                Name: "country query",
                Queries:    []*datastore.Query{
                datastore.NewQuery(FruitKind).Filter("Producers =", "USA"),
                datastore.NewQuery(FruitKind).Filter("Producers =", "China"),
            },
        },
    }

	keys, err := fruitQuery.Query(dsClient, request.Context())
    if err != nil {
        logger.Errorf("Query error %v", err)
        return
    }

	logger.Infof("Running GetMulti for %d fruit", len(keys))
	fruits := make([]*Fruit, len(keys), len(keys))
	if err := dsClient.GetMulti(request.Context(), keys, fruits); err != nil {
		logger.Errorf("GetMulti error %v", err)
		return err
	}

```
Should return: ```Orange and Chilli```

# Notes

Happy to receive PRs.. Especially around documentation and testing.
