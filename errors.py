class QueryIsEmptyError(AssertionError):
    def __init__(self, *args, **kwargs):
        super(QueryIsEmptyError, self).__init__(*args, **kwargs)

