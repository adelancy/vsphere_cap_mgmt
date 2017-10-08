import unittest
from vCenterApi import VcenterApi


class TestVmwareScripts(unittest.TestCase):

    def setUp(self):
        self.test_host = '10.12.30.11'
        self.api = VcenterApi(vCenter_host=self.test_host, user='adrian', password='Password123', port=443,
                              sample_period=20, time_interval=1)

    def tearDown(self):
        pass

    def test_get_all_vms(self):
        vms = self.api.get_view_container()
        self.assertIsNotNone(vms)

    def test_get_vm_perf_properties(self):
        info = self.api.get_vm_properties(props=['name', 'runtime.powerState'])
        self.assertIsNotNone(info)
        self.assertTrue(len(info) > 2)

    def test_get_esxi_hosts_info(self):
        self.api.set_query_time_window(start=None, time_interval=24*60)
        data = self.api.get_all_esxi_hosts()
        data = self.api.get_esxi_hosts_capacity_details(data)
        self.assertEqual(len(data), 3)
        for k, v in data[0]['performanceStats'].items():
            try:
                #print v
                # self.assertIsNotNone(v['value'])
                self.assertIsNotNone(v)
            except TypeError:
                print v

    def test_get_cluster_info(self):
        info = self.api.get_cluster_capacity_details(self.api.get_compute_cluster_view())
        self.assertIsNotNone(info)
        self.assertTrue(len(info) > 1)
        self.assertIsNotNone(info[0]['name'])

    def test_calculate_host_vCPU_capacity(self):
        hosts = self.api.get_all_esxi_hosts()
        for host in hosts:
            data = self.api.get_available_host_vcpus(host)
            print 'number of slots are {0}'.format(data)
            self.assertIsNotNone(hosts)
            self.assertGreaterEqual(data, 0, 'Invalid Value')

    def test_calculate_cluster_mem_cpu_slots(self):
        hosts = self.api.get_all_esxi_hosts()
        for host in hosts:
            data = self.api.get_memory_slots_available(host)
            self.assertTrue(data)
            print data

    def test_calculate_datastore_slots(self):
        clusters = self.api.get_compute_cluster_view()
        data = self.api.get_datastore_slots_available(clusters)
        self.assertIsNotNone(data)

    def test_get_vm_info(self):
        vms = self.api.get_all_vms()
        data = self.api.get_vm_capacity_details(vms[1])
        #print data['performanceStats']['datastoreLatRead']
        self.assertTrue(data['performanceStats'])

    def test_get_datastore_capacity(self):
        data = self.api.get_datastore_capacity_info(self.api.get_datastores_view())
        for k, v in data[0].items():
            self.assertTrue(k)
            self.assertTrue(v)

    def test_get_cpu_ready_average(self):
        cpu_ready = self.api.get_cpu_stat_moments(self.api.get_all_vms())
        self.assertTrue(cpu_ready)

    # def test_get_cpu_ready_value(self):
    #     from _test_config import creds
    #     api = VcenterApi(vCenter_host='10.15.254.100', user=creds['username'], password=creds['password'],
    #                      time_interval=7*24*60, sample_period=1800)
    #
    #     vm = api.get_vm_by_uuid('5017efe3-0f15-d017-507d-5388bd69334c')
    #     self.assertTrue(vm)
    #     result = api.get_cpu_queue_stats(vm)
    #     self.assertTrue(result['average']['percentStat']['value'] <= 100)

if __name__ == '__main__':
    unittest.main()
