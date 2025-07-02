package main

import (
	"context"
	"flag"
	"fmt"
	"log"

	"cloud.google.com/go/datastore"
	"github.com/arran4/dsquery"
)

const FruitKind = "Fruit"

type Fruit struct {
	DSKey     *datastore.Key `datastore:"__key__"`
	Name      string         `datastore:",noindex"`
	Color     string
	Producers []string
}

func main() {
	projectID := flag.String("project", "", "GCP project ID")
	flag.Parse()
	if *projectID == "" {
		log.Fatal("-project is required")
	}

	ctx := context.Background()
	client, err := datastore.NewClient(ctx, *projectID)
	if err != nil {
		log.Fatalf("failed to create datastore client: %v", err)
	}
	defer client.Close()

	// Build a query: (Color is Orange or Red) AND (Producers includes China)
	q := &dsquery.And{
		Name: "example",
		SubQueries: []dsquery.Query{
			&dsquery.Or{
				Name: "color",
				Queries: []*datastore.Query{
					datastore.NewQuery(FruitKind).Filter("Color =", "Orange"),
					datastore.NewQuery(FruitKind).Filter("Color =", "Red"),
				},
			},
			&dsquery.Ident{
				Name:        "producer",
				StoredQuery: datastore.NewQuery(FruitKind).Filter("Producers =", "China"),
			},
		},
	}

	keys, err := q.Query(client, ctx)
	if err != nil {
		log.Fatalf("query error: %v", err)
	}

	fruits := make([]*Fruit, len(keys))
	if err := client.GetMulti(ctx, keys, fruits); err != nil {
		log.Fatalf("GetMulti error: %v", err)
	}

	fmt.Println("Fruits:")
	for i, f := range fruits {
		if f == nil {
			continue
		}
		fmt.Printf("%d: %s (%s)\n", i+1, f.Name, f.Color)
	}
}
