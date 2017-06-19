from datetime import timedelta, datetime
import atexit
import ssl

from pyVim import connect
from pyVmomi import vmodl
from pyVmomi import vim
import numpy as np

from errors import QueryIsEmptyError, HostConnectionError


class VcenterApi(object):
    def __init__(self, vCenter_host, user, password, port=443, sample_period=20,
                 start=None, time_interval=24*60):
        """
        This class provides an API to interact with VSphere using it's REST API. This is a wrapper class around the
        PyVmomi library primarily to get access to the top level managed entities and query capacity information.

        :param vCenter_host: IP Address or Hostname of the vCenter instance to connect to.
        :param user: vSphere username to login as.
        :param password: vSphere password
        :param port: Port to connect to the vCenter instance on. Defaults to SSL port 443
        :param sample_period: Sampling period in seconds used by vSphere to collect data points.
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
        """
        Class method used to create connection to a vCenter instance over an HTTPS session.
        :param host:  Hostname or IP Address of target vCenter instance
        :param user: Username
        :param password: Password
        :param port: Port to connect to vCenter instance on
        :return: A vSphere connection object
        """
        # form a connection...
        context = ssl.SSLContext(ssl.PROTOCOL_TLSv1)
        context.verify_mode = ssl.CERT_NONE

        conn = connect.SmartConnect(host=host, user=user, pwd=password,
                                    port=port, sslContext=context)

        # Note: some daemons use a shutdown hook to do this, not the atexit
        atexit.register(connect.Disconnect, conn)

        return conn

    def disconnect_vSphere_connection(self):
        """
        Disconnects the connected vCenter instance.
        :return:
        """
        connect.Disconnect(self.db_conn)

    def get_conn_search_index(self):
        return self.db_conn.content.searchIndex

    def set_query_time_window(self, start=None, time_interval=24*60):
        """
        Specify the timewindow to query vCenter for.

        :param DateTime start: Query start time as a python datetime object
        :param time_interval: The time interval to add to the query start time i.e. 4 hours, 1 day etc...
        :return: A tupple with the start and end datetimes.
        """
        if start is None:
            start = self.db_conn.CurrentTime()
        self._start = start - timedelta(minutes=time_interval+1)
        self._end = start - timedelta(minutes=1)
        return self._start, self._end

    def get_view_container(self, recursive=True, view_type=None):
        """
        Builds the container view object that represents the list of vSphere managed entities of the view_type passed in
        from the vCenter instance currently connected.

        :param Boolean recursive: Flag to indicate how to build the container object.
        :param view_type: Managed Entity i.e. VirtualMachine, HostSystem etc... Defaults to VirtualMachine
        :return:
        """
        try:
            content = self.db_conn.RetrieveContent()
            container = content.rootFolder  # starting point to look into
            if view_type is None:
                view_type = [vim.VirtualMachine]
            return content.viewManager.CreateContainerView(container, view_type, recursive)
        except vmodl.MethodFault as error:
            raise error('Problem connecting to a VM')

    def get_all_vms(self):
        """
        Returns all VMs associated with the connected vCenter instance.
        :return: A list of VM Managed Entity objects
        """
        return self.get_view_container().view

    def get_vm_by_uuid(self, uuid):
        """
        Query a VM by its vCenter UUID.
        :param uuid: vCenter Unique Identifier
        :return:
        """
        return self.db_conn.content.searchIndex.FindByUuid(None, uuid, True, True)

    def get_vm_by_ip(self, search_index, ip_address):
        """
        Query a VM by it's IP address.

        :param search_index:
        :param ip_address:
        :return:
        """
        return search_index.FindByIp(None, ip_address, True)

    def get_vm_by_dns(self, dns_name=None):
        try:
            return self.db_conn.content.searchIndex.FindByDnsName(dnsName=dns_name, vmSearch=True)
        except (AttributeError, TypeError):
            pass

    def get_esxi_host_by_name(self, hostname):
        raise NotImplementedError

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
            managed_obj = self.get_view_container()
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

    def get_all_esxi_hosts(self):
        """
        Returns a view container that has the information on the esxi hosts. Access the view property to get the list
        of esix hosts.
        :return:
        """
        # Search for all ESXi hosts
        content = self.db_conn.content
        return content.viewManager.CreateContainerView(content.rootFolder, [vim.HostSystem], True).view

    def get_esxi_hosts_capacity(self, hosts):
        out = []
        for host in hosts:
            out.append(self.get_esxi_host_capacity_details(host))
        return out

    def get_esxi_host_capacity_details(self, host):
        try:
            domain = host.config.network.dnsConfig.searchDomain[0]
            hostname = host.config.network.dnsConfig.hostName
            # Todo: update to remove host.config is None error
        except (IndexError, AttributeError):
            domain = None
            hostname = None
        try:
            return {
                'hostname': hostname,
                'cluster': host.parent.name,
                'vms': host.vm,  # List of associated Virtual Machines
                'domain': domain,  # can be the .domainName property also if not blank
                'datastores': host.datastore,
                'vendor': host.summary.hardware.vendor,
                'model': host.summary.hardware.model,
                'cpuModel': host.summary.hardware.cpuModel,
                'cpuCores': dict(value=host.summary.hardware.numCpuCores, units=None),
                'cpuThreads': dict(value=host.summary.hardware.numNics, units=None),
                'runtime': dict(value=host.RetrieveHardwareUptime() / (60 * 60 * 24), units='days'),
                'idInfo': parse_host_id_info(host.summary.hardware.otherIdentifyingInfo),
                'status': host.summary.overallStatus,
                'cpuUsage': dict(value=host.summary.quickStats.overallCpuUsage, units='MHz'),
                'ramUsage': dict(value=host.summary.quickStats.overallMemoryUsage, units='MB'),  # MB
                'cpuCapacity': dict(value=host.summary.hardware.cpuMhz * host.summary.hardware.numCpuCores,
                                    units='MHz'),
                'ramCapacity': dict(value=float(host.summary.hardware.memorySize) / pow(1024, 3), units='GB'),
                'uptime': dict(value=host.summary.quickStats.uptime, units='seconds'),
                'powerState': host.summary.runtime.powerState,
                'connectionState': host.summary.runtime.connectionState,
                'vDiskMaxCapacity': dict(value=host.summary.runtime.hostMaxVirtualDiskCapacity, units=None),
                'serverTimestamp': self.db_conn.CurrentTime(),
                'performanceStats': self.get_esxi_host_performance_stats(host),
                'raw': host
            }

        except vmodl.fault.HostCommunication:
            raise HostConnectionError('Problem vCenter connection to Host, please investigate!')

    def get_esxi_host_performance_stats(self, esxi_host):
        """
        By default provides average values over the last 60 minutes
        :param esxi_host: pyVmomi HostSystem Managed Entity
        :param interval:
        :param kwargs:
        :return:
        """
        keymap = dict(
            cpuUsageAvg=dict(counter='cpu.usage.average', units='percentage'),
            cpuUsageMax=dict(counter='cpu.usage.maximum', units='percentage'),
            memUsageAvg=dict(counter='mem.usage.average', units='percentage'),
            memUsageMax=dict(counter='mem.usage.maximum', units='percentage'),
            powerUsable=dict(counter='power.capacity.usable.average', units='watts'),
            powerCap=dict(counter='power.powerCap.average', units='watts'),
            powerUsage=dict(counter='power.capacity.usagePct.average', units='percentage'),
            energy=dict(counter='power.energy.summation', units='joules'),
            power=dict(counter='power.power.average', units='watts')
        )
        return self.get_resource_performance_stats(esxi_host, keymap)

    def get_host_by_dns(self, dns_name=None):
        try:
            return self.db_conn.content.searchIndex.FindByDnsName(dnsName=dns_name, vmSearch=False)
        except (AttributeError, TypeError):
            pass

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
                'timestamp': self.db_conn.CurrentTime(),
                'raw': datastore
            }
            output.append(info)
        return output

    def get_compute_cluster_view(self):
        content = self.db_conn.content
        return content.viewManager.CreateContainerView(content.rootFolder, [vim.ClusterComputeResource], True).view

    def get_cluster_capacity_details(self, clusters):
        output = []
        for cluster in clusters:
            try:
                cluster_usage = cluster.GetResourceUsage()
                cluster_info = {
                    'name': cluster.name,
                    'cpuCapacity': dict(value=cluster.summary.totalCpu, units='MHz'),
                    'effectiveCpu': dict(value=cluster.summary.effectiveCpu, units='MHz'),
                    'cpuUsed': dict(value=cluster_usage.cpuUsedMHz, units='MHz'),
                    'memCapacity': dict(value=float(cluster_usage.memCapacityMB) / 1024, units='GB'),
                    'memUsed': dict(value=float(cluster_usage.memUsedMB) / 1024, units='GB'),
                    'numCpuCores': dict(value=cluster.summary.numCpuCores, units=None),
                    'numCpuThreads': dict(value=cluster.summary.numCpuThreads, units=None),
                    'storageUsed': dict(value=float(cluster_usage.storageUsedMB) / 1024, units='GB'),
                    'storageCapacity': dict(value=float(cluster_usage.storageCapacityMB) / 1024, units='GB'),
                    'effectiveMemory': dict(value=float(cluster.summary.effectiveMemory) / 1024, units='GB'),
                    'numHosts': dict(value=cluster.summary.numHosts, units=None),
                    'numEffectiveHosts': dict(value=cluster.summary.numEffectiveHosts, units=None),
                    'overallStatus': cluster.summary.overallStatus,
                    'serverTimestamp': self.db_conn.CurrentTime(),
                    'raw': cluster
                }
                output.append(cluster_info)
            except vmodl.fault.MethodNotFound:
                pass
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

    def get_capacity_details_for_vms(self, vms):
        """
        Returns the capacity performance details for a list of VMs
        :param [vim.VirtualMachines] vms:
        :return:
        """
        output = []
        for resource_entity in vms:
            output.append(self.get_vm_capacity_details(resource_entity))
        return output

    def get_vm_capacity_details(self, resource_entity):
        output = dict()
        summary = resource_entity.summary
        output['vDisks'] = []
        output['vNICs'] = []

        output['uuid'] = resource_entity.config.instanceUuid  # vCenter globally unique ID
        output['template'] = resource_entity.config.template
        output['powerState'] = resource_entity.runtime.powerState
        output['host'] = resource_entity.summary.runtime.host.summary.config.name
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
                    committed=dict(value=(float(datastoreInfo.committed) / pow(1024, 3)), units='GB'),
                    uncommitted=dict(value=(float(datastoreInfo.uncommitted) / pow(1024, 3)), units='GB'),
                    unshared=dict(value=(float(datastoreInfo.unshared) / pow(1024, 3)), units='GB')
                )
            )
        vm_hardware = resource_entity.config.hardware
        for each_vm_hardware in vm_hardware.device:
            if (each_vm_hardware.key >= 2000) and (each_vm_hardware.key < 3000):
                vdisk = {
                    'label': each_vm_hardware.deviceInfo.label,
                    'capacity': dict(value=float(each_vm_hardware.capacityInKB) / 1024 / 1024, units='GB'),
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
        output['raw'] = resource_entity
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
            networkTx=dict(counter='net.transmitted.average', units='kBps', instance='*'),
            networkRx=dict(counter='net.received.average', units='kBps', instance='*'),
            power=dict(counter='power.power.average', units='watts')
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
        out['queryStartTime'] = self._start
        out['queryEndTime'] = self._end

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

    def get_cpu_stat_moments(self, vms, counter_name='cpu.ready.summation'):
        """
        Returns the arithmetic mean and standard deviation of a distribution of values for a cpu counter
        :param vms:
        :param counter_name:
        :return:
        """
        vals = []
        percent_ready_vals = []
        for vm in vms:
            try:
                stats = self.get_cpu_queue_stats(vm, counter_name)['average']
                cpu_ready = stats['stat']['value']
                percent_ready = stats['percentStat']['value']
                if cpu_ready:
                    vals.append(cpu_ready)
                    percent_ready_vals.append(percent_ready)
            except (AttributeError, TypeError, QueryIsEmptyError):
                pass
        return mean(vals), mean(percent_ready_vals), standard_deviation(vals), standard_deviation(percent_ready_vals)

    def get_cpu_queue_stats(self, vm, counter_name='cpu.ready.summation'):
        """
        Design to calculate stats for queuing time
        :param vm:
        :param counter_name:
        :return:
        """
        results = self.build_perf_query((self.stat_check(counter_name)), 'aggregated', vm)
        avg_stat = float(mean(results[0].value[0].value))  # actual ready time in ms
        # Sample period represents the time it takes to capture one sample.
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

    def get_available_host_vcpus(self, host):
        """
        Computes the available estimated vCPUs that the host can support based on current VM usage
        :param HostSystem host:
        :return:
        """
        vms = host.vm
        # Get a lit representing the (not None types) vcpu counts of all vms on the hosts
        vcpus = filter(lambda vcpu: vcpu is not None, [vm.summary.config.numCpu for vm in vms])
        vcpu_count = sum(vcpus)
        #  Check against vSphere scheduling algorithm limits
        if len(vms) > int(0.9 * 512) or vcpu_count > int(0.9 * 1024):
            return 0

        # Calculate statistical moments for VM cpu performance values
        array = []
        for vm in host.vm:
            try:
                cpu_ready = self.get_cpu_queue_stats(vm)['average']
                costop = self.get_cpu_queue_stats(vm, 'cpu.costop.summation')['average']
                vcpu_usage = vm.summary.quickStats.overallCpuUsage  # MHz
                stats = {
                    'cpu_ready': cpu_ready['stat']['value'],
                    'percent_cpu_ready': cpu_ready['percentStat']['value'],
                    'costop': costop['stat']['value'],
                    'percent_costop': costop['percentStat']['value'],
                    'vcpu_usage': vcpu_usage
                }
                array.append(stats)
            except QueryIsEmptyError:
                pass

        out = dict()
        out['cpu_ready'] = {
            'mean': dict(value=mean([x['percent_cpu_ready'] for x in array]), units='%'),
            'stdv': dict(value=standard_deviation([x['percent_cpu_ready'] for x in array]), units='%')
        }
        out['cpu_costop'] = {
            'mean': dict(value=mean([x['percent_cpu_ready'] for x in array]), units='%'),
            'stdv': dict(value=standard_deviation([x['percent_cpu_ready'] for x in array]), units='%')
        }
        # Threshold statistical values
        if out['cpu_ready']['mean']['value'] is None or out['cpu_ready']['stdv']['value'] is None:
            out['cpu_slots'] = dict(value=0, units='vCPU')
            return out
        if out['cpu_costop']['mean']['value'] is None or out['cpu_costop']['stdv']['value'] is None:
            out['cpu_slots'] = dict(value=0, units='vCPU')
            return out

        # print 'avg percent ready is {0}'.format(avg_percent_ready + std_percent_ready)
        if (out['cpu_ready']['mean']['value'] + out['cpu_ready']['stdv']['value']) > 5:
            out['cpu_slots'] = dict(value=0, units='vCPU')
            return out
        if (out['cpu_costop']['mean']['value'] + out['cpu_costop']['stdv']['value']) > 3:
            out['cpu_slots'] = dict(value=0, units='vCPU')
            return out
        # Calculate available vCPU provisioning capacity
        vcpu_usage_vals = [x['vcpu_usage'] for x in array]
        non_zero_vcpu_usage = filter(lambda usage: usage > 0, vcpu_usage_vals)
        # Average vCPU usage in MHz along with the spread...
        out['vm_cpu_usage'] = {
            'mean': dict(value=mean(non_zero_vcpu_usage), units='MHz'),
            'stdv': dict(value=standard_deviation(non_zero_vcpu_usage), units='MHz')
        }
        # Calculate number of slots based on usage
        est_vcpu_usage = out['vm_cpu_usage']['mean']['value'] + out['vm_cpu_usage']['stdv']['value']
        host_cpu_capacity = (host.summary.hardware.cpuMhz * host.summary.hardware.numCpuCores)
        available_host_cpu = host_cpu_capacity - host.summary.quickStats.overallCpuUsage
        try:
            out['cpu_slots'] = dict(
                value=round(available_host_cpu / est_vcpu_usage, 0),
                units='vCPU'
            )
        except ZeroDivisionError:
            # Theoretical Upper bound on vCPus available to provision
            out['cpu_slots'] = dict(value=round(0.9 * 1024, 0), units='vCPU')

        return out

    def get_memory_slots_available(self, host):
        """
        Calculates the available VM provisioning capacity of a host based on memory slots
        :param host:
        :return:
        """
        vms = host.vm
        # Calculate average vm memory usage:
        vms_mem_values_tmp = [x.summary.quickStats.guestMemoryUsage for x in vms]
        vms_mem_values = filter(lambda mem: mem > 0, vms_mem_values_tmp)
        available_host_ram = float(host.summary.hardware.memorySize) - host.summary.quickStats.overallMemoryUsage

        avg_vm_mem_usage = mean(vms_mem_values)  # calculate what a mem slot is
        if avg_vm_mem_usage is None:
            avg_vm_mem_usage = 0
        try:
            slots = round(available_host_ram / avg_vm_mem_usage)
            return dict(value=slots, unit='mem-slots')
        except ZeroDivisionError:
            print('Host {0} has no VMs'.format(host.name))
            return dict(value=0.9*512, unit=None)

    def get_datastore_slots_available(self, clusters):
        """
        Calculates the available VM provisioning ability across all datastores in use by a cluster
        :param clusters:
        :return:
        """
        cluster_datastore_slots = dict()
        for cluster in clusters:
            usage_info = cluster.GetResourceUsage()
            total_capacity = usage_info.storageCapacityMB * 0.95
            used = usage_info.storageUsedMB
            available = total_capacity - used
            # Calculate VM average
            vms = self.get_cluster_vms(cluster)
            vm_storage_vals = []
            for vm in vms:
                used = 0
                for datastore in vm.storage.perDatastoreUsage:
                    if datastore.committed is None or datastore.uncommitted is None:
                        continue  # Move on to next loop iteration
                    used = used + datastore.committed + datastore.uncommitted  # Units - Bytes
                vm_storage_vals.append(used)

            # Calculate average
            avg_vm_datastore_usage = convert_byte_units(mean(vm_storage_vals), unit='mega')
            try:
                slots = available / avg_vm_datastore_usage
                cluster_datastore_slots[cluster.name] = dict(slots=slots, units='cluster-datastore-slots')
            except ZeroDivisionError:
                cluster_datastore_slots[cluster.name] = dict(slots=float('inf'), units='cluster-datastore-slots')
        return cluster_datastore_slots


def get_epoch_value():
    return (datetime.utcnow() - datetime(1970, 1, 1)).seconds


def mean(numbers):
    """
    Calculates the arithmetic mean of an array of numerical values
    :param numbers:  list of number
    """
    if len(numbers) == 0:
        return None
    return np.mean(np.array(numbers))


def standard_deviation(numbers):
    """
    Computes the standard deviation of an array of numerical values
    :param numbers:
    :return:
    """
    if len(numbers) == 0:
        return None
    return np.std(np.array(numbers))


def convert_byte_units(val, unit='mega'):
    """
    Performs units conversion for bits.

    :param val: Numerical value representing the number bits to be converted
    :param unit: The ends state units
    :return: The new value in units of unit
    """
    if unit == 'kilo':
        return float(val) / 1024
    if unit == 'mega':
        return float(val) / 1024 / 1024
    if unit == 'giga':
        return float(val) / 1024 / 1024 / 1024
    return float(val)


def parse_host_id_info(id_info):
    """
    Accepts a vSphere id tag and parses out the Asset and Service Tag numbers.

    :param id_info:
    :return: A dictionary consisting of the Asset and Service Tag numbers {assetTag: 123, serviceTag: 456}
    """
    output = dict()
    for info in id_info:
        if info.identifierType.key == 'AssetTag':
            output['assetTag'] = info.identifierValue
        if info.identifierType.key == 'ServiceTag':
            output['serviceTag'] = info.identifierValue
    return output

