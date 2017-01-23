from datetime import timedelta, datetime
import atexit
import ssl

from pyvim import connect
from pyVmomi import vmodl
from pyVmomi import vim
import numpy as np

from errors import QueryIsEmptyError


class VcenterApi(object):
    def __init__(self, vCenter_host, user, password, port=443, sample_period=20):
        """

        :param vCenter_host:
        :param user:
        :param password:
        :param port:
        :param sample_period: Sampling period in seconds
        """
        self.vCenter_host = vCenter_host
        self.user = user
        self.password = password
        self.port = port
        self.sample_period = sample_period
        self.db_conn = self.create_vSphere_connection(self.vCenter_host, self.user, self.password, self.port)
        self.perf_dict = self.get_performance_counters()

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

    def get_all_vms_view(self, recursive=True, view_type=None):
        """
        Builds the container view object that represents the list of VMs from the Vsphere Connection passed.
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

    def build_perf_query(self, vc_time, counter_id, instance='', resource_entity=vim.VirtualMachine, interval=1):
        """

        :param datetime vc_time:
        :param Int counter_id: Performance counter ID
        :param instance:
        :param resource_entity:
        :param interval:
        :return:
        """
        perf_manager = self.db_conn.content.perfManager
        metric_id = vim.PerformanceManager.MetricId(counterId=counter_id, instance=instance)
        start_time = vc_time - timedelta(minutes=(interval + 1))
        end_time = vc_time - timedelta(minutes=1)
        # 20s sampling period to use
        query = vim.PerformanceManager.QuerySpec(intervalId=self.sample_period, entity=resource_entity,
                                                 metricId=[metric_id], startTime=start_time, endTime=end_time)

        perf_results = perf_manager.QueryPerf(querySpec=[query])

        if not perf_results:
            raise QueryIsEmptyError('No Data Found! Tip: Check Drift between Vcenter and Source Servers \
            and that VM is Powered on')

        return perf_results

    def get_performance_counters(self):
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
        t_spec = vim.PropertyCollector.TraversalSpec(name='tSpecName', path='view', skip=False, type=vim.view.ContainerView)
        p_spec = vim.PropertyCollector.PropertySpec(all=False, pathSet=props, type=property_type)
        o_spec = vim.PropertyCollector.ObjectSpec(obj=managed_obj, selectSet=[t_spec], skip=False)
        pf_spec = vim.PropertyCollector.FilterSpec(objectSet=[o_spec], propSet=[p_spec], reportMissingObjectsInResults=False)

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
                'domain': host.config.network.dnsConfig.searchDomain[0], # can be the .domainName property also if not blank
                'vendor': host.summary.hardware.vendor,
                'model': host.summary.hardware.model,
                'ramCapacityInMb': host.summary.hardware.memorySize / 1024 / 1024 / 1024,  # GB
                'cpuModel': host.summary.hardware.numCpuPkgs,
                'cpuCores': host.summary.hardware.numCpuCores,
                'cpuThreads': host.summary.hardware.numNics,
                'runtime': host.RetrieveHardwareUptime() / (60 * 60 * 24),
                'idInfo': host.summary.hardware.otherIdentifyingInfo,  # List of id tags, can loop thru to identify service tag
                'status': host.summary.overallStatus,
                'cpuUsageInMhz': host.summary.quickStats.overallCpuUsage,
                'ramUsageInMb': host.summary.quickStats.overallMemoryUsage,  # MB
                'cpuCapacityInMhz': host.summary.hardware.cpuMhz,
                'uptime': host.summary.quickStats.uptime,
                'powerState': host.summary.runtime.powerState,
                'connectionState': host.summary.runtime.connectionState
            }
            out.append(info)
        return out

    def get_host_perf_info(self, dns_name):
        # host = self.get_vm_by_ip(self.db_conn.content.searchIndex, self.vCenter_host)
        host = self.db_conn.content.searchIndex.FindByDnsName(dnsName=dns_name, vmSearch=False)
        perfManager = self.db_conn.content.perfManager
        metricId = vim.PerformanceManager.MetricId(counterId=6, instance="*")
        startTime = datetime.now() - timedelta(hours=1)
        endTime = datetime.now()

        query = vim.PerformanceManager.QuerySpec(maxSample=1,
                                                 entity=host,
                                                 metricId=[metricId],
                                                 startTime=startTime,
                                                 endTime=endTime)

        return perfManager.QueryPerf(querySpec=[query])

    def get_datastores_view(self):
        # Search for all Datastores hosts
        content = self.db_conn.content
        return content.viewManager.CreateContainerView(content.rootFolder, [vim.Datastore], True).view

    def get_datastore_capacity_info(self, datastore_view):
        output = []
        # epoch = get_epoch_value()
        for datastore in datastore_view:
            info = {
                'name': datastore.summary.name,
                'capacityInGb': convert_byte_units(datastore.summary.capacity, unit='giga'),
                'freeSpaceInGb': convert_byte_units(datastore.summary.freeSpace, unit='giga'),
                'uncommitted ': convert_byte_units(datastore.summary.uncommitted, unit='giga'),
                'vmCount': len(datastore.vm),
                'accessible ': datastore.summary.accessible,
                'type': datastore.summary.type
            }
            output.append(info)
        return output

    def get_compute_cluster_view(self):
        content = self.db_conn.content
        return content.viewManager.CreateContainerView(content.rootFolder, [vim.ClusterComputeResource], True).view

    def get_cluster_capacity_details(self, clusters):
        # clusters = self.get_compute_cluster_view().view
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

    def get_cpu_slots_available(self, clusters):
        # 1. Get total CPU MHz for all VM Hosts in cluster
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

    def get_vm_capacity_details(self, vm, interval=1, vchtime=None):
        """

        :param vm:
        :param interval: Time interval to query against in minutes
        :param vchtime:
        :return:
        """
        if vchtime is None:
            vchtime = self.db_conn.CurrentTime()
        statInt = interval * (60 / self.sample_period)
        summary = vm.summary
        disk_list = []
        vdisks = []
        network_list = []
        network_devices = []
        output = dict()

        output['uuid'] = vm.config.instanceUuid  # Vcenter globaly unique ID

        # Convert limit and reservation values from -1 to None
        if vm.resourceConfig.cpuAllocation.limit == -1:
            vmcpulimit = 'None'
            output['cpuLimit'] = dict(value=None, units=None)
        else:
            vmcpulimit = "{} Mhz".format(vm.resourceConfig.cpuAllocation.limit)
            output['cpuLimit'] = dict(value=vm.resourceConfig.cpuAllocation.limit, units='MHz')
        if vm.resourceConfig.memoryAllocation.limit == -1:
            vmmemlimit = "None"
            output['memLimit'] = dict(value=None, unit=None)
        else:
            vmmemlimit = "{} MB".format(vm.resourceConfig.cpuAllocation.limit)
            output['memLimit'] = dict(value=vm.resourceConfig.cpuAllocation.memoryAllocation)

        if vm.resourceConfig.cpuAllocation.reservation == 0:
            vmcpures = "None"
            output['cpuReservation'] = dict(value=None, units=None)
        else:
            vmcpures = "{} Mhz".format(vm.resourceConfig.cpuAllocation.reservation)
            output['cpuReservation'] = dict(value=vm.resourceConfig.cpuAllocation.reservation, units='MHz')
        if vm.resourceConfig.memoryAllocation.reservation == 0:
            vmmemres = "None"
            output['memReservation'] = dict(value=None, units=None)
        else:
            vmmemres = "{} MB".format(vm.resourceConfig.memoryAllocation.reservation)
            output['memReservation'] = dict(value=vm.resourceConfig.memoryAllocation.reservation, units='MB')

        vm_hardware = vm.config.hardware
        for each_vm_hardware in vm_hardware.device:
            if (each_vm_hardware.key >= 2000) and (each_vm_hardware.key < 3000):
                disk_list.append('{} | {:.1f}GB | Thin: {} | {}'.format(each_vm_hardware.deviceInfo.label,
                                                             each_vm_hardware.capacityInKB/1024/1024,
                                                             each_vm_hardware.backing.thinProvisioned,
                                                             each_vm_hardware.backing.fileName))

                vdisk = {
                    'label': each_vm_hardware.deviceInfo.label,
                    'capacity': dict(value=each_vm_hardware.capacityInKB/1024/1024, units='GB'),
                    'thinProvisioned':each_vm_hardware.backing.thinProvisioned,
                    'filename': each_vm_hardware.backing.fileName,
                    'type': 'vDisk'
                }
                vdisks.append(vdisk)

            elif (each_vm_hardware.key >= 4000) and (each_vm_hardware.key < 5000):
                network_list.append('{} | {} | {}'.format(each_vm_hardware.deviceInfo.label,
                                                             each_vm_hardware.deviceInfo.summary,
                                                             each_vm_hardware.macAddress))
                vnetwork_device = {
                    'label': each_vm_hardware.deviceInfo.label,
                    'summary': each_vm_hardware.deviceInfo.summary,
                    'macAddress': each_vm_hardware.macAddress
                }
                network_devices.append(vnetwork_device)

        try:
            # # CPU Ready Average
            # statCpuReady = self.build_perf_query(vchtime, (self.stat_check('cpu.ready.summation')),
            #                                 "", vm, interval)
            #
            # cpuReady = (float(sum(statCpuReady[0].value[0].value)) / statInt) / 20000
            output['cpuReady'] = dict()
            # output['cpuReady']['average'] = dict(value=cpuReady * 100, units='percent')
            # output['cpuReady']['max'] = dict(value=(float(100 * max(statCpuReady[0].value[0].value)) / 20000), units='percent')
            cpu_ready = self.get_cpu_ready(vm, vchtime, interval)
            output['cpuReady']['average'] = cpu_ready['average']
            output['cpuReady']['max'] = cpu_ready['max']

            # CPU Usage Average % - NOTE: values are type LONG so needs divided by 100 for percentage
            statCpuUsage = self.build_perf_query(vchtime, (self.stat_check('cpu.usage.average')), "", vm, interval)
            cpuUsage = ((float(sum(statCpuUsage[0].value[0].value)) / statInt) / 100)
            output['cpuUsage'] = cpuUsage
            # Memory Active Average MB
            statMemoryActive = self.build_perf_query(vchtime, (self.stat_check('mem.active.average')), "", vm, interval)
            memoryActive = (float(sum(statMemoryActive[0].value[0].value) / 1024) / statInt)
            output['memoryActiveInMb'] = memoryActive
            output['memoryCapacityInMb'] = summary.config.memorySizeMB
            # Memory Shared
            statMemoryShared = self.build_perf_query(vchtime, (self.stat_check('mem.shared.average')), "", vm, interval)
            memoryShared = (float(sum(statMemoryShared[0].value[0].value) / 1024) / statInt)
            output['memorySharedInMb'] = statMemoryShared
            # Memory Balloon
            statMemoryBalloon = self.build_perf_query(vchtime, (self.stat_check('mem.vmmemctl.average')), "", vm, interval)

            memoryBalloon = (float(sum(statMemoryBalloon[0].value[0].value) / 1024) / statInt)
            output['memoryBalloonInMb'] = memoryBalloon
            # Memory Swapped
            statMemorySwapped = self.build_perf_query(vchtime, (self.stat_check('mem.swapped.average')), "", vm, interval)
            memorySwapped = (float(sum(statMemorySwapped[0].value[0].value) / 1024) / statInt)
            output['memorySwappedInMb'] = memorySwapped
            # Datastore Average IO
            statDatastoreIoRead = self.build_perf_query(vchtime, (self.stat_check('datastore.numberReadAveraged.average')),
                                             "*", vm, interval)
            DatastoreIoRead = (float(sum(statDatastoreIoRead[0].value[0].value)) / statInt)
            output['dataStoreIoReads'] = DatastoreIoRead
            statDatastoreIoWrite = self.build_perf_query(vchtime, (self.stat_check('datastore.numberWriteAveraged.average')),
                                              "*", vm, interval)
            DatastoreIoWrite = (float(sum(statDatastoreIoWrite[0].value[0].value)) / statInt)
            output['dataStoreIoWrites'] = DatastoreIoWrite
            # Datastore Average Latency
            statDatastoreLatRead = self.build_perf_query(vchtime, (self.stat_check('datastore.totalReadLatency.average')),
                                              "*", vm, interval)
            DatastoreLatRead = (float(sum(statDatastoreLatRead[0].value[0].value)) / statInt)
            statDatastoreLatWrite = self.build_perf_query(vchtime, (self.stat_check('datastore.totalWriteLatency.average')),
                                               "*", vm, interval)
            DatastoreLatWrite = (float(sum(statDatastoreLatWrite[0].value[0].value)) / statInt)

            # Network usage (Tx/Rx)
            statNetworkTx = self.build_perf_query(vchtime, (self.stat_check('net.transmitted.average')), "", vm, interval)
            networkTx = (float(sum(statNetworkTx[0].value[0].value) * 8 / 1024) / statInt)
            output['networkTxInMb'] = dict(value=networkTx, units='MB')
            statNetworkRx = self.build_perf_query(vchtime, (self.stat_check('net.received.average')), "", vm, interval)
            networkRx = (float(sum(statNetworkRx[0].value[0].value) * 8 / 1024) / statInt)
            output['networkRxInMb'] = dict(value=networkRx, units='MB')

            output['timeIntervalInSeconds'] = statInt * self.sample_period
            output['name'] = summary.config.name
            output['description'] = summary.config.annotation

            output['guestOS'] = summary.config.guestFullName
            output['numOfCpus'] = summary.config.numCpu
            return output
        except QueryIsEmptyError:
            pass

    def get_avg_cpu_ready(self, vms):
        vals = []
        for vm in vms:
            try:
                cpu_ready = self.get_cpu_ready(vm, interval=1)['average']['value']
                if cpu_ready:
                    vals.append(cpu_ready)
            except (AttributeError, TypeError, QueryIsEmptyError):
                pass
        return mean(vals)

    def get_cpu_ready(self, vm, vchtime=None, interval=1):
        if vchtime is None:
            vchtime = self.db_conn.CurrentTime()

        stat_int = interval * (60 / self.sample_period)
        # CPU Ready Average
        stat_cpu_ready = self.build_perf_query(vchtime, (self.stat_check('cpu.ready.summation')),
                                               "", vm, interval)
        output = dict()
        cpu_ready = (float(sum(stat_cpu_ready[0].value[0].value)) / stat_int) / (self.sample_period * 1000)  # ms
        output['average'] = dict(value=cpu_ready * 100, units='percent')
        output['max'] = dict(
            value=(float(100 * max(stat_cpu_ready[0].value[0].value)) / (self.sample_period * 1000)),
            units='percent'
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
