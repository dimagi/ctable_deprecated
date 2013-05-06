class MockCouch(object):
    """
    An in-memory mock of the CouchDBKit db, instantiated
    with a simple mapping of resource and params to results.

    Since dictionaries are not hashable, the mapping is
    written as a pair of tuples, handled appropriately
    internallly.

    MockCommCareHqClient({
        'forms': [
            (
                {'_search': 'test1'},
                [
                   ... objects ...
                ]
            ),
        ]
    })
    """
    def __init__(self, mock_data):
        self.mock_data = mock_data

    def view(self, *args, **kwargs):
        return MockResult(self.mock_data)


class MockResult(object):
    def __init__(self, rows):
        self.rows = rows

    @property
    def total_rows(self):
        return len(self.rows)

    def __iter__(self):
        return iter(self.rows)