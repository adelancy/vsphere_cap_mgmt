from pyVmomi import vmodl


class QueryIsEmptyError(AssertionError):
    def __init__(self, *args, **kwargs):
        super(QueryIsEmptyError, self).__init__(*args, **kwargs)


class HostConnectionError(vmodl.fault.HostCommunication):
    def __init__(self, *args, **kwargs):
        super(HostConnectionError, self).__init__(* args, **kwargs)
