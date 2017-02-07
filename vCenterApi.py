from datetime import timedelta, datetime
import atexit
import ssl

from pyvim import connect
from pyVmomi import vmodl
from pyVmomi import vim
import numpy as np

from errors import QueryIsEmptyError


class VcenterApi(object):
    def __init__(self, vCenter_host, user, password, port=443, sample_period=20,
                 start=None, time_interval=24*60):
        """
        :param vCenter_host:
        :param user:
        :param password:
        :param port:
        :param sample_period: Sampling period in seconds
        :param time_interval: Time interval to query for in minutes
        """
        self.vCenter_host = vCenter_host
        self.user = user
        self.password = password
        self.port = port
        self.sample_period = sample_period
        self.db_conn = self.create_vSphere_connection(self.vCenter_host, self.user, self.password, self.port)
        self.perf_dict = self.get_performance_counters()
        self._start, self._end = self.set_query_time_window(start, time_interval)

    @classmethod
    def create_vSphere_connection(cls, host, user, password, port):
        # form a connection...
        context = ssl.SSLContext(ssl.PROTOCOL_TLSv1)
        context.verify_mode = ssl.CERT_NONE

        conn = connect.SmartConnect(host=host, user=user, pwd=password,
                                    port=port, sslContext=context)

        # Note: some daemons use a shutdown hook to do this, not the atexit
        atexit.register(connect.Disconnect, conn)

        return conn

    def get_conn_search_index(self):
        return self.db_conn.content.searchIndex

    def set_query_time_window(self, start=None, time_interval=24*60):
        if start is None:
            start = self.db_conn.CurrentTime()
        self._start = start - timedelta(minutes=time_interval+1)
        self._end = start - timedelta(minutes=1)
        return self._start, self._end

    def get_all_vms_view(self, recursive=True, view_type=None):
        """
        Builds the container view object that represents the list of VMs from the vSphere Connection passed.
        :param db_conn: Vsphere Connection
        :param recursive:
        :param view_type:
        :return:
        """
        try:
            content = self.db_conn.RetrieveContent()
            container = content.rootFolder  # starting point to look into
            if view_type is None:
                view_type = [vim.VirtualMachine]
            return content.viewManager.CreateContainerView(container, view_type, recursive)
            # return containerView.view
        except vmodl.MethodFault as error:
            raise error('Caught vm')

    def get_all_vms(self):
        return self.get_all_vms_view().view

    def get_vm_by_uuid(self, uuid):
        return self.db_conn.content.searchIndex.FindByUuid(None, uuid, True, True)

    def get_vm_by_ip(self, search_index, ip_address):
        return search_index.FindByIp(None, ip_address, True)

    def get_esxi_host_by_name(self):
        pass

    def execute_perf_query(self, counter_name, **kwargs):
        return self.build_perf_query(counter_id=self.stat_check(counter_name=counter_name), **kwargs)

    def build_perf_query(self, counter_id, instance='all', resource_entity=vim.VirtualMachine):
        """
        :param datetime vc_time:
        :param Int counter_id: Performance counter ID
        :param instance:
        :param resource_entity:
        :param interval: time interval to query over in seconds
        :return:
        """
        perf_manager = self.db_conn.content.perfManager
        if instance == "all":
            instance = '*'
        if instance == 'aggregated':
            instance = ""
        metric_id = vim.PerformanceManager.MetricId(counterId=counter_id, instance=instance)

        query = vim.PerformanceManager.QuerySpec(intervalId=self.sample_period, entity=resource_entity,
                                                 metricId=[metric_id], startTime=self._start, endTime=self._end)

        perf_results = perf_manager.QueryPerf(querySpec=[query])

        if not perf_results:
            raise QueryIsEmptyError('No Data Found! Tip: Check Drift between Vcenter and Source Servers \
            and that VM is Powered on')

        return perf_results

    def get_performance_counters(self):
        """
        Returns a listing of the valid performance counters. See link for more information...
        https://communities.vmware.com/docs/DOC-5600
        :return:
        """
        # Get all the performance counters
        perf_list = self.db_conn.content.perfManager.perfCounter
        self.perf_dict = dict()
        for counter in perf_list:
            counter_full = "{}.{}.{}".format(counter.groupInfo.key, counter.nameInfo.key, counter.rollupType)
            self.perf_dict[counter_full] = counter.key
        return self.perf_dict

    def get_vm_properties(self, managed_obj=None, props=None, property_type=vim.VirtualMachine):
        content = self.db_conn.content
        if managed_obj is None:
            managed_obj = self.get_all_vms_view()
        # Build a view and get basic properties for all Virtual Machines
        t_spec = vim.PropertyCollector.TraversalSpec(name='tSpecName', path='view', skip=False,
                                                     type=vim.view.ContainerView)
        p_spec = vim.PropertyCollector.PropertySpec(all=False, pathSet=props, type=property_type)
        o_spec = vim.PropertyCollector.ObjectSpec(obj=managed_obj, selectSet=[t_spec], skip=False)
        pf_spec = vim.PropertyCollector.FilterSpec(objectSet=[o_spec], propSet=[p_spec],
                                                   reportMissingObjectsInResults=False)

        ret_options = vim.PropertyCollector.RetrieveOptions()
        total_props = []
        ret_props = content.propertyCollector.RetrievePropertiesEx(specSet=[pf_spec], options=ret_options)
        total_props += ret_props.objects
        while ret_props.token:
            ret_props = content.propertyCollector.ContinueRetrievePropertiesEx(token=ret_props.token)
            total_props += ret_props.objects
        managed_obj.Destroy()
        # Turn the output in retProps into a usable dictionary of values
        gp_output = []
        for each_prop in total_props:
            prop_dic = dict()
            for prop in each_prop.propSet:
                prop_dic[prop.name] = prop.val
            prop_dic['moref'] = each_prop.obj
            gp_output.append(prop_dic)
        return gp_output

    def stat_check(self, counter_name):
        """
        Obtains the counter id for a given counter name passed in . syntax. Ex cpu.ready.summation
        :param counter_name: name of counter in dot syntax
        """
        counter_key = self.perf_dict[counter_name]
        return counter_key

    def get_esxi_hosts_info(self):
        """
        Returns a view container that has the information on the esxi hosts. Access the view property to get the list
        of esix hosts.
        :return:
        """
        # Search for all ESXi hosts
        content = self.db_conn.content
        return content.viewManager.CreateContainerView(content.rootFolder, [vim.HostSystem], True)

    def get_esxi_hosts_capacity_details(self, esxi_hosts_container):
        out = []
        # Verify hosts properties
        for host in esxi_hosts_container:
            info = {
                'hostname': host.config.network.dnsConfig.hostName,
                'cluster': host.parent.name,
                'domain': host.config.network.dnsConfig.searchDomain[0],
                # can be the .domainName property also if not blank
                'vendor': host.summary.hardware.vendor,
                'model': host.summary.hardware.model,
                'cpuModel': host.summary.hardware.numCpuPkgs,
                'cpuCores': host.summary.hardware.numCpuCores,
                'cpuThreads': host.summary.hardware.numNics,
                'runtime': host.RetrieveHardwareUptime() / (60 * 60 * 24),
                'idInfo': None,  # host.summary.hardware.otherIdentifyingInfo,  # Todo: Parse better
                # List of id tags, can loop thru to identify service tag
                'status': host.summary.overallStatus,
                'cpuUsageInMhz': host.summary.quickStats.overallCpuUsage,
                'ramUsageInMb': host.summary.quickStats.overallMemoryUsage,  # MB
                'cpuCapacityInMhz': host.summary.hardware.cpuMhz,
                'ramCapacityInMb': host.summary.hardware.memorySize / 1024 / 1024 / 1024,  # GB
                'uptime': host.summary.quickStats.uptime,
                'powerState': host.summary.runtime.powerState,
                'connectionState': host.summary.runtime.connectionState,
                'performanceStats': self.get_esxi_host_performance_stats(host)
            }
            out.append(info)
        return out

    def get_esxi_host_performance_stats(self, esxi_host):
        """
        By default provides average values over the last 60 minutes
        :param esxi_host:
        :param interval:
        :param kwargs:
        :return:
        """
        keymap = dict(
            cpuUsageAvg=dict(counter='cpu.usage.average', units='percentage'),
            cpuUsageMax=dict(counter='cpu.usage.maximum', units='percentage'),
            memUsageAvg=dict(counter='mem.usage.average', units='percentage'),
            memUsageMax=dict(counter='mem.usage.maximum', units='percentage')
        )
        return self.get_resource_performance_stats(esxi_host, keymap)

    def get_host_by_dns(self, dns_name=None, ip_address=None):
        try:
            return self.db_conn.content.searchIndex.FindByDnsName(dnsName=dns_name, vmSearch=False)
        except (AttributeError, TypeError):
            return self.get_vm_by_ip(self.db_conn.content.searchIndex, self.vCenter_host)  # Todo: Fix

    def get_datastores_view(self):
        # Search for all Datastores hosts
        content = self.db_conn.content
        return content.viewManager.CreateContainerView(content.rootFolder, [vim.Datastore], True).view

    def get_datastore_capacity_info(self, datastore_view):
        output = []
        for datastore in datastore_view:
            info = {
                'name': datastore.summary.name,
                'url': datastore.info.url,
                'capacity': dict(value=convert_byte_units(datastore.summary.capacity, unit='giga'), units='GB'),
                'freeSpace': dict(value=convert_byte_units(datastore.summary.freeSpace, unit='giga'), units='GB'),
                'uncommitted ': dict(value=convert_byte_units(datastore.summary.uncommitted, unit='giga'), units='GB'),
                'vmCount': len(datastore.vm),
                'accessible ': datastore.summary.accessible,
                'type': datastore.summary.type,
                'timestamp': self.db_conn.CurrentTime()
            }
            output.append(info)
        return output

    def get_datastore_performance_stats(self, datastore):
        keymap = dict(
            usageAvg=dict(counter='datastore.write.average', units='kBps'),
            UsageMax=dict(counter='datastore.read.average', units='kBps'),
        )
        return self.get_resource_performance_stats(datastore, keymap)

    def get_compute_cluster_view(self):
        content = self.db_conn.content
        return content.viewManager.CreateContainerView(content.rootFolder, [vim.ClusterComputeResource], True).view

    def get_cluster_capacity_details(self, clusters):
        output = []
        for cluster in clusters:
            cluster_usage = cluster.GetResourceUsage()
            cluster_info = {
                'name': cluster.name,
                'cpuCapacityMHz': cluster.summary.totalCpu,
                'effectiveCpuMHz': cluster.summary.effectiveCpu,
                'cpuUsedMHz': cluster_usage.cpuUsedMHz,
                'memCapacityMB': cluster_usage.memCapacityMB,
                'memUsedMB': cluster_usage.memUsedMB,
                'numCpuCores': cluster.summary.numCpuCores,
                'numCpuThreads': cluster.summary.numCpuThreads,
                'storageUsedMB': cluster_usage.storageUsedMB,
                'storageCapacityMB': cluster_usage.storageCapacityMB,
                'effectiveMemoryMB': cluster.summary.effectiveMemory,  # MB
                'numHosts': cluster.summary.numHosts,
                'numEffectiveHosts': cluster.summary.numEffectiveHosts,
                'overallStatus': cluster.summary.overallStatus,
                'timestamp': self.db_conn.CurrentTime()
            }
            output.append(cluster_info)
        return output

    @classmethod
    def get_cluster_vms(cls, cluster, include_templates=False):
        output = []
        for host in cluster.host:
            for vm in host.vm:
                if include_templates is False:
                    if vm.config.template is False:
                        output.append(vm)
                else:
                    output.append(vm)
        return output

    def get_cpu_slots_available(self, clusters):  # Todo: Modify to remove two hosts
        #  1. Get total CPU MHz for all VM Hosts in cluster
        #  2. Get total CPU MHz used by VM Hosts in cluster
        #  3. Calculate 90% of the total cpu mhz
        #  4. Calculate CPU MHz available by subtracting used from total
        #  5. Divide Available CPU MHz by average cluster VM CPU usage
        cluster_cpu_slots = dict()
        for cluster in clusters:
            total_eff_cpu = cluster.summary.effectiveCpu * 0.9  # 10% padding
            total_used_cpu = cluster.GetResourceUsage().cpuUsedMHz
            total_available_cpu = total_eff_cpu - total_used_cpu
            if total_available_cpu <= 0:
                return 0
            vms = self.get_cluster_vms(cluster)
            vm_cpu_usage_vals = []
            for vm in vms:
                if vm.summary.quickStats.overallCpuDemand != 0:
                    vm_cpu_usage_vals.append(vm.summary.quickStats.overallCpuDemand)
            try:
                avg_vm_cpu_usage = sum(vm_cpu_usage_vals) / len(vm_cpu_usage_vals)
                slots = total_available_cpu / avg_vm_cpu_usage
                cluster_cpu_slots[cluster.name] = dict(metric=avg_vm_cpu_usage, unit='MHz', slots=slots,
                                                       type='cluster-cpu-slots')
            except ZeroDivisionError as error:
                print('Cluster has no active VMs ' + error.message)  # Todo: Add logging
                cluster_cpu_slots[cluster.name] = dict(slots='unlimited', metric=None, unit=None,
                                                       type='cluster-cpu-slots')
        return cluster_cpu_slots

    def get_memory_slots_available(self, clusters):
        cluster_mem_slots = dict()
        for cluster in clusters:
            cluster_usage = cluster.GetResourceUsage()
            total_mem = cluster.summary.effectiveMemory * 0.9  # Pad the capacity for more conservative est
            total_used_mem = cluster_usage.memUsedMB
            available_mem = total_mem - total_used_mem
            # Get the usage across all vms in cluster to calculate average vm usage
            vms = self.get_cluster_vms(cluster)
            vms_mem_values_tmp = [x.summary.quickStats.guestMemoryUsage for x in vms]
            vms_mem_values = filter(lambda mem: mem > 0, vms_mem_values_tmp)
            try:
                avg_vm_mem_usage = sum(vms_mem_values) / len(vms_mem_values)  # calculate what a mem slot is
                slots = available_mem / avg_vm_mem_usage
                cluster_mem_slots[cluster.name] = dict(slots=slots, metric=avg_vm_mem_usage,
                                                       unit='MB', type='cluster-mem-slots')
            except ZeroDivisionError:
                print('Cluster {0} has no VMs'.format(cluster.name))
                cluster_mem_slots[cluster.name] = dict(slots='unlimited', metric=None,
                                                       unit=None, type='cluster-mem-slots')
        return cluster_mem_slots

    def get_datastore_slots_available(self, clusters):
        cluster_datastore_slots = dict()
        for cluster in clusters:
            usage_info = cluster.GetResourceUsage()
            total_capacity = usage_info.storageCapacityMB * 0.95
            used = usage_info.storageUsedMB
            available = total_capacity - used
            # Calculate VM average
            vms = self.get_cluster_vms(cluster)
            vm_storage_vals = []
            for vm in vms:  # Todo: comitted + uncomitted
                used = 0
                for datastore in vm.storage.perDatastoreUsage:
                    used = used + datastore.committed + datastore.uncommitted  # Units - Bytes
                vm_storage_vals.append(used)

            # Calculate average
            avg_vm_datastore_usage = convert_byte_units(mean(vm_storage_vals), unit='mega')
            try:
                slots = available / avg_vm_datastore_usage
                cluster_datastore_slots[cluster.name] = dict(slots=slots, metric=avg_vm_datastore_usage,
                                                             unit='MB', type='cluster-datastore-slots')
            except ZeroDivisionError:
                cluster_datastore_slots[cluster.name] = dict(slots='unlimited', metric=None, unit=None,
                                                             type='cluster-datastore-slots')
        return cluster_datastore_slots

    def get_vm_capacity_details(self, resource_entity):
        output = dict()
        summary = resource_entity.summary
        output['vDisks'] = []
        output['vNICs'] = []

        output['uuid'] = resource_entity.config.instanceUuid  # vCenter globally unique ID
        output['powerState'] = resource_entity.runtime.powerState
        output['cpuUsage'] = dict(value=resource_entity.summary.quickStats.overallCpuUsage, units='MHz')
        output['memoryUsage'] = dict(value=resource_entity.summary.quickStats.guestMemoryUsage, units='MB')
        output['status'] = resource_entity.summary.overallStatus
        # Convert limit and reservation values from -1 to None
        if resource_entity.resourceConfig.cpuAllocation.limit == -1:
            output['cpuLimit'] = dict(value=None, units='MHz')
        else:
            output['cpuLimit'] = dict(value=resource_entity.resourceConfig.cpuAllocation.limit, units='MHz')
        if resource_entity.resourceConfig.memoryAllocation.limit == -1:
            output['memLimit'] = dict(value=None, units='MB')
        else:
            output['memLimit'] = dict(value=resource_entity.resourceConfig.cpuAllocation.memoryAllocation, units='MB')
        if resource_entity.resourceConfig.cpuAllocation.reservation == 0:
            output['cpuReservation'] = dict(value=None, units='MHz')
        else:
            output['cpuReservation'] = dict(value=resource_entity.resourceConfig.cpuAllocation.reservation, units='MHz')
        if resource_entity.resourceConfig.memoryAllocation.reservation == 0:
            output['memReservation'] = dict(value=None, units='MB')
        else:
            output['memReservation'] = dict(value=resource_entity.resourceConfig.memoryAllocation.reservation,
                                            units='MB')

        output['memCapacity'] = dict(value=summary.config.memorySizeMB, units='MB')
        output['name'] = summary.config.name
        output['description'] = summary.config.annotation
        output['guestOS'] = summary.config.guestFullName
        output['numOfCpus'] = summary.config.numCpu
        output['datstoreUsage'] = []
        for datastoreInfo in resource_entity.storage.perDatastoreUsage:
            output['datstoreUsage'].append(
                dict(
                    name=datastoreInfo.datastore.info.name,
                    url=datastoreInfo.datastore.info.url,
                    committed=dict(value=float(datastoreInfo.committed / pow(1024, 3)), units='GB'),
                    uncommitted=dict(value=float(datastoreInfo.uncommitted / pow(1024, 3)), units='GB'),
                    unshared=dict(value=float(datastoreInfo.unshared / pow(1024, 3)), units='GB')
                )
            )
        vm_hardware = resource_entity.config.hardware
        for each_vm_hardware in vm_hardware.device:
            if (each_vm_hardware.key >= 2000) and (each_vm_hardware.key < 3000):
                vdisk = {
                    'label': each_vm_hardware.deviceInfo.label,
                    'capacity': dict(value=each_vm_hardware.capacityInKB / 1024 / 1024, units='GB'),
                    'thinProvisioned': each_vm_hardware.backing.thinProvisioned,
                    'fileName': each_vm_hardware.backing.fileName,
                    'type': 'vDisk'
                }
                output['vDisks'].append(vdisk)

            elif (each_vm_hardware.key >= 4000) and (each_vm_hardware.key < 5000):
                vnetwork_device = {
                    'label': each_vm_hardware.deviceInfo.label,
                    'summary': each_vm_hardware.deviceInfo.summary,
                    'macAddress': each_vm_hardware.macAddress,
                    'type': 'vNIC'
                }
                output['vNICs'].append(vnetwork_device)
        output['timestamp'] = self.db_conn.CurrentTime()
        output['performanceStats'] = self.get_vm_performance_stats(resource_entity)
        return output

    def get_vm_performance_stats(self, resource_entity):
        """
        See https://www.vmware.com/support/developer/converter-sdk/conv61_apireference/datastore_counters.html
        for more information
        :param resource_entity:
        :param interval: Time interval to query against in minutes
        :param vchtime:
        :return:
        """

        keymap = dict(
            cpuUsageAvg=dict(counter='cpu.usage.average', units='percentage'),
            cpuUsageMax=dict(counter='cpu.usage.maximum', units='percentage', instance='*'),
            memUsageAvg=dict(counter='mem.usage.average', units='percentage'),
            memUsageMax=dict(counter='mem.usage.maximum', units='percentage'),
            memActive=dict(counter='mem.active.average', units='kB'),
            memShared=dict(counter='mem.shared.average', units='kB'),
            memSwapped=dict(counter='mem.swapused.average', units='kB'),
            datastoreIoWrites=dict(counter='datastore.numberWriteAveraged.average', units=None, instance='*'),
            datastoreIoReads=dict(counter='datastore.numberReadAveraged.average', units=None, instance='*'),
            datastoreLatWrite=dict(counter='datastore.totalWriteLatency.average', units='ms', instance='*'),
            datastoreLatRead=dict(counter='datastore.totalReadLatency.average', units='ms', instance='*'),
            networkTx=dict(counter='net.transmitted.average', units='MB', instance='*'),
            networkRx=dict(counter='net.received.average', units='MB', instance='*'),
        )

        queue_stats = dict(
            cpuReady=dict(counter='cpu.ready.summation', units='ms'),
            cpuWait=dict(counter='cpu.wait.summation', units='ms'),
            cpuIdle=dict(counter='cpu.idle.summation', units='ms'),
            cpuCoStop=dict(counter='cpu.costop.summation', units='ms'),
        )

        output = self.get_resource_performance_stats(resource_entity, keymap)

        for k, v in queue_stats.items():
            try:
                output[k] = self.get_cpu_queue_stats(resource_entity, counter_name=v.get('counter'))
            except QueryIsEmptyError:
                pass

        return output

    def get_resource_performance_stats(self, resource_entity, keymap):
        out = dict()
        out['start'] = self._start
        out['end'] = self._end

        for k, v in keymap.items():
            try:
                val = self.get_averaged_performance_stat(resource_entity, counter_name=v['counter'],
                                                         instance=v.get('instance', ""))
                if v['units'] == 'percentage':
                    try:
                        val /= 100  # Because % values stored as Longs, need to divide by 100
                    except TypeError as err:
                        pass

                out[k] = dict(
                    value=val,
                    units=v['units']
                )
            except QueryIsEmptyError:
                pass
        return out

    def get_averaged_performance_stat(self, resource_entity, counter_name, instance=""):
        try:
            stat_data = self.execute_perf_query(counter_name, instance=instance, resource_entity=resource_entity)
            return float(mean(stat_data[0].value[0].value))
        except QueryIsEmptyError:
            pass

    def get_avg_cpu_ready(self, vms):
        vals = []
        percent_ready_vals = []
        for vm in vms:
            try:
                cpu_ready = self.get_cpu_queue_stats(vm)['average']['stat']['value']
                percent_ready = self.get_cpu_queue_stats(vm)['average']['percentStat']['value']
                if cpu_ready:
                    vals.append(cpu_ready)
                    percent_ready_vals.append(percent_ready)
            except (AttributeError, TypeError, QueryIsEmptyError):
                pass
        return mean(vals), mean(percent_ready_vals)

    def get_cpu_queue_stats(self, vm, counter_name='cpu.ready.summation'):
        """
        Design to calculate stats for queing time
        :param vm:
        :param counter_name:
        :return:
        """
        results = self.build_perf_query((self.stat_check(counter_name)), 'aggregated', vm)
        avg_stat = float(mean(results[0].value[0].value))  # actual ready time in ms
        percent_stat = 100 * avg_stat / (self.sample_period * 1000)  # AVG % of sample time in ms that vm is ready
        max_stat = float(max(results[0].value[0].value))
        percent_max_stat = 100 * max_stat / (self.sample_period * 1000)
        min_stat = float(min(results[0].value[0].value))
        percent_min_stat = 100 * min_stat / (self.sample_period * 1000)

        output = dict()
        output['average'] = dict(
            stat=dict(value=avg_stat, units='ms'),
            percentStat=dict(value=percent_stat, units='percent'),
        )
        output['max'] = dict(
            stat=dict(value=max_stat, units='ms'),
            percentStat=dict(value=percent_max_stat, units='percent')
        )
        output['min'] = dict(
            stat=dict(value=min_stat, units='ms'),
            percentStat=dict(value=percent_min_stat, units='percent')
        )
        return output


def get_epoch_value():
    return (datetime.utcnow() - datetime(1970, 1, 1)).seconds


def mean(numbers):
    """
    Calculates the arithmetic mean of an array of numbers
    :param numbers:  list of number
    """
    vector = np.array(numbers)
    return np.mean(vector)
    # return float(sum(numbers)) / max(len(numbers), 1)


def convert_byte_units(bytes, unit='mega'):
    if unit == 'kilo':
        return bytes / 1024
    if unit == 'mega':
        return bytes / 1024 / 1024
    if unit == 'giga':
        return bytes / 1024 / 1024 / 1024
    return bytes
