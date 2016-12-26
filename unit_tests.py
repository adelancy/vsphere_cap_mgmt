import unittest
from datetime import datetime, timedelta
from get_vSphere_data import create_vSphere_connection, get_all_vms_view, get_vm_by_ip, build_query, \
    get_conn_search_index, get_performance_counters, get_vm_properties, print_vm_details


class TestVmwareScripts(unittest.TestCase):

    def setUp(self):
        self.db_conn = create_vSphere_connection()
        self.test_vm_ip = '10.12.30.20'

    def tearDown(self):
        pass

    def test_get_all_vms(self):
        vms = get_all_vms_view(self.db_conn)
        self.assertIsNotNone(vms)

    # def test_get_vm_performance_stats(self):
    #     vm_perf_stats = get_vm_perf_stats(self.db_conn, get_vm_by_ip(self.test_vm_ip))
    #     self.assertIsNotNone(vm_perf_stats)

    def test_build_perf_query(self):
        si = get_conn_search_index(self.db_conn)
        test_vm = get_vm_by_ip(si, self.test_vm_ip)
        results = build_query(self.db_conn.content, datetime.now(), test_vm, timedelta(minutes=1))
        self.assertIsNotNone(results)
        print(results)

    def test_get_vm_perf_properties(self):
        managed_obj = get_all_vms_view(self.db_conn)
        info = get_vm_properties(self.db_conn, managed_obj, ['name', 'runtime.powerState'])
        print_vm_details(info[0]['moref'], self.db_conn, get_performance_counters(self.db_conn))

if __name__ == '__main__':
    unittest.main()
