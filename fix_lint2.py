import os

def replace_in_file(filename, old, new):
    with open(filename, 'r') as f:
        content = f.read()
    content = content.replace(old, new)
    with open(filename, 'w') as f:
        f.write(content)

replace_in_file('dsquery_test.go',
                'func (qc *Count) Query(ctx context.Context, dsClient DatastoreClient) ([]*datastore.Key, error) {',
                'func (qc *Count) Query(_ context.Context, dsClient DatastoreClient) ([]*datastore.Key, error) {')

replace_in_file('dsquery_test.go',
                'func (e *Error) Query(ctx context.Context, dsClient DatastoreClient) ([]*datastore.Key, error) {',
                'func (e *Error) Query(_ context.Context, _ DatastoreClient) ([]*datastore.Key, error) {')

replace_in_file('dsquery_test.go',
                'c.Query(nil, nil)',
                'c.Query(context.TODO(), nil)')
