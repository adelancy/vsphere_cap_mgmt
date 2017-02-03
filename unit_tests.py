import unittest
import json
from datetime import datetime, timedelta
from vCenterApi import VcenterApi


class TestVmwareScripts(unittest.TestCase):

    def setUp(self):
        self.test_host = '10.12.30.20'
        self.api = VcenterApi(vCenter_host=self.test_host, user='splab\captest', password='Password1234!', port=443)

    def tearDown(self):
        pass

    def test_get_all_vms(self):
        vms = self.api.get_all_vms_view()
        self.assertIsNotNone(vms)

    def test_get_vm_perf_properties(self):
        info = self.api.get_vm_properties(props=['name', 'runtime.powerState'])
        self.assertIsNotNone(info)
        self.assertTrue(len(info) > 2)

    def test_get_esxi_hosts_info(self):
        self.api.set_query_time_window(start=None, time_interval=24*60)
        data = self.api.get_esxi_hosts_info()
        data = self.api.get_esxi_hosts_capacity_details(data.view)
        self.assertEqual(len(data), 3)
        for k, v in data[0]['performanceStats'].items():
            #print data[0]['hostname']
            #print k, v
            self.assertTrue(v['value'])
            # self.assertLessEqual(v['value'], 100.0)

    def test_get_cluster_info(self):
        info = self.api.get_cluster_capacity_details(self.api.get_compute_cluster_view())
        self.assertIsNotNone(info)
        self.assertTrue(len(info) > 1)
        self.assertIsNotNone(info[0]['name'])

    def test_calculate_cluster_cap_cpu_slots_available(self):
        clusters = self.api.get_compute_cluster_view()
        data = self.api.get_cpu_slots_available(clusters)

        self.assertIsNotNone(data['SP1-Cluster-1'])

    def test_calculate_cluster_mem_cpu_slots(self):
        clusters = self.api.get_compute_cluster_view()
        data = self.api.get_memory_slots_available(clusters)
        self.assertTrue(data)

    def test_calculate_datastore_slots(self):
        clusters = self.api.get_compute_cluster_view()
        data = self.api.get_datastore_slots_available(clusters)
        self.assertIsNotNone(data)

    def test_get_vm_info(self):
        vms = self.api.get_all_vms()
        data = self.api.get_vm_capacity_details(vms[1])
        #print data['performanceStats']['dataStoreLatRead']
        self.assertTrue(data['performanceStats'])

    def test_get_datastore_capacity(self):
        data = self.api.get_datastore_capacity_info(self.api.get_datastores_view())
        for k, v in data[0].items():
            self.assertTrue(k)
            self.assertTrue(v)

    def test_get_cpu_ready_average(self):
        cpu_ready = self.api.get_avg_cpu_ready(self.api.get_all_vms())
        self.assertTrue(cpu_ready)

if __name__ == '__main__':
    unittest.main()
