import unittest
from datetime import datetime, timedelta
from get_vSphere_data import VcenterApi


class TestVmwareScripts(unittest.TestCase):

    def setUp(self):
        self.test_host = '10.12.30.20'
        self.api = VcenterApi(vCenter_host=self.test_host, user='splab\captest', password='Password123!', port=443)

    def tearDown(self):
        pass

    def test_get_all_vms(self):
        vms = self.api.get_all_vms_view()
        self.assertIsNotNone(vms)

    # def test_get_vm_performance_stats(self):
    #     vm_perf_stats = get_vm_perf_stats(self.db_conn, get_vm_by_ip(self.test_vm_ip))
    #     self.assertIsNotNone(vm_perf_stats)

    def test_get_vm_perf_properties(self):
        info = self.api.get_vm_properties(props=['name', 'runtime.powerState'])
        self.api.print_vm_details(self.api.get_all_vms()[0], interval=1)

if __name__ == '__main__':
    unittest.main()
