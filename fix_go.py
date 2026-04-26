import os

def replace_in_file(filename, old, new):
    with open(filename, 'r') as f:
        content = f.read()
    content = content.replace(old, new)
    with open(filename, 'w') as f:
        f.write(content)

# Change Query signature in dsquery.go
replace_in_file('dsquery.go',
                'Query(dsClient DatastoreClient, ctx context.Context)',
                'Query(ctx context.Context, dsClient DatastoreClient)')

replace_in_file('dsquery.go',
                'q.Query(dsClient, ctx)',
                'q.Query(ctx, dsClient)')

replace_in_file('dsquery.go',
                'qi.Query.Query(dsClient, ctx)',
                'qi.Query.Query(ctx, dsClient)')

replace_in_file('dsquery.go',
                'c.StoredQuery.Query(dsClient, ctx)',
                'c.StoredQuery.Query(ctx, dsClient)')

# Change Query signature in dsquery_test.go
replace_in_file('dsquery_test.go',
                'Query(dsClient DatastoreClient, ctx context.Context)',
                'Query(ctx context.Context, dsClient DatastoreClient)')

replace_in_file('dsquery_test.go',
                'tt.qa.Query(tt.args.dsClient, tt.args.ctx)',
                'tt.qa.Query(tt.args.ctx, tt.args.dsClient)')

replace_in_file('dsquery_test.go',
                'tt.qo.Query(tt.args.dsClient, tt.args.ctx)',
                'tt.qo.Query(tt.args.ctx, tt.args.dsClient)')

replace_in_file('dsquery_test.go',
                'qi.Query(tt.args.dsClient, tt.args.ctx)',
                'qi.Query(tt.args.ctx, tt.args.dsClient)')

replace_in_file('dsquery_test.go',
                'tt.query.Query(tt.args.dsClient, tt.args.ctx)',
                'tt.query.Query(tt.args.ctx, tt.args.dsClient)')

replace_in_file('dsquery_test.go',
                'tt.nq.Query(tt.args.dsClient, tt.args.ctx)',
                'tt.nq.Query(tt.args.ctx, tt.args.dsClient)')

replace_in_file('dsquery_test.go',
                'qo.Count.Query(dsClient, ctx)',
                'qo.Count.Query(ctx, dsClient)')

replace_in_file('dsquery_test.go',
                'qo.StoredQuery.Query(dsClient, ctx)',
                'qo.StoredQuery.Query(ctx, dsClient)')

# Unused ctx in MockDS.GetAll
replace_in_file('dsquery_test.go',
                'func (m *MockDS) GetAll(ctx context.Context, q *datastore.Query, dst interface{}) (keys []*datastore.Key, err error) {',
                'func (m *MockDS) GetAll(_ context.Context, q *datastore.Query, dst interface{}) (keys []*datastore.Key, err error) {')

# Increment/Decrement in dsquery_test.go
replace_in_file('dsquery_test.go',
                'm[k.String()] -= 1',
                'm[k.String()]--')
replace_in_file('dsquery_test.go',
                'm[k.String()] += 1',
                'm[k.String()]++')

# example_test.go Query
replace_in_file('example_test.go',
                'func (s *StaticQuery) Query(dsClient dsquery.DatastoreClient, ctx context.Context) ([]*datastore.Key, error) {',
                'func (s *StaticQuery) Query(ctx context.Context, dsClient dsquery.DatastoreClient) ([]*datastore.Key, error) {')

replace_in_file('example_test.go',
                'fruitQuery.Query(dsClient, context.Background())',
                'fruitQuery.Query(context.Background(), dsClient)')

# Unused ctx in example_test.go
replace_in_file('example_test.go',
                'func (c *MockClient) GetAll(ctx context.Context, q *datastore.Query, dst interface{}) (keys []*datastore.Key, err error) {',
                'func (c *MockClient) GetAll(_ context.Context, q *datastore.Query, dst interface{}) (keys []*datastore.Key, err error) {')

replace_in_file('cmd/dsquery-example/main.go',
                'q.Query(client, ctx)',
                'q.Query(ctx, client)')

replace_in_file('example_test.go',
                'q.Query(ds, context.Background())',
                'q.Query(context.Background(), ds)')

replace_in_file('example_test.go',
                'func (c *countQuery) Query(dsClient DatastoreClient, ctx context.Context) ([]*datastore.Key, error) {',
                'func (c *countQuery) Query(ctx context.Context, dsClient DatastoreClient) ([]*datastore.Key, error) {')

replace_in_file('example_test.go',
                'cached.Query(ds, context.Background())',
                'cached.Query(context.Background(), ds)')

replace_in_file('example_test.go',
                'cached.Query(nil, context.Background())',
                'cached.Query(context.Background(), nil)')
