import os

def replace_in_file(filename, old, new):
    with open(filename, 'r') as f:
        content = f.read()
    content = content.replace(old, new)
    with open(filename, 'w') as f:
        f.write(content)

# example_test.go:18
replace_in_file('example_test.go',
                'func (m *staticDS) GetAll(ctx context.Context, q *datastore.Query, dst interface{}) ([]*datastore.Key, error) {',
                'func (m *staticDS) GetAll(_ context.Context, _ *datastore.Query, _ interface{}) ([]*datastore.Key, error) {')

# example_test.go:35
replace_in_file('example_test.go',
                'func (c *countQuery) Query(ctx context.Context, dsClient DatastoreClient) ([]*datastore.Key, error) {',
                'func (c *countQuery) Query(_ context.Context, _ DatastoreClient) ([]*datastore.Key, error) {')

# dsquery_test.go:67
replace_in_file('dsquery_test.go',
                'func (m *MockDS) GetAll(_ context.Context, q *datastore.Query, dst interface{}) (keys []*datastore.Key, err error) {',
                'func (m *MockDS) GetAll(_ context.Context, _ *datastore.Query, _ interface{}) (keys []*datastore.Key, err error) {')

# dsquery_test.go:170
replace_in_file('dsquery_test.go',
                'func (s *StoredResult) Query(ctx context.Context, dsClient DatastoreClient) ([]*datastore.Key, error) {',
                'func (s *StoredResult) Query(_ context.Context, _ DatastoreClient) ([]*datastore.Key, error) {')

# dsquery_test.go: SA1012 nil context -> context.TODO()
replace_in_file('dsquery_test.go',
                'cache.Query(nil, nil)',
                'cache.Query(context.TODO(), nil)')

# Actually wait, `example_test.go:35` `dsClient` isn't unused maybe? Let's check:
