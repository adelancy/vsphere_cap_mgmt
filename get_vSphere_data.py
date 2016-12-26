import atexit
import argparse
import getpass
import ssl
import unittest
from datetime import timedelta

from pyvim import connect
from pyVmomi import vmodl
from pyVmomi import vim

host = '10.12.30.20'
user = 'splab\captest'
password = 'Password123!'
port = 443


class VcenterApi(object):
    def __init__(self, vCenter_host, user, password, port=443):
        self.vCenter_host = vCenter_host
        self.user = user
        self.password = password
        self.port = port

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
        return self.get_all_vms_view(self.db_conn).view

    def get_vm_by_uuid(self, search_index, uuid):
        return search_index.FindByUuid(None, uuid, True, True)

    def get_vm_by_ip(self, search_index, ip_address):
        return search_index.FindByIp(None, ip_address, True)

    def get_vm_perf_stats(self, vm):
        pass

    def build_query(self, vchtime, counterId, instance, vm, interval):
        perfManager = self.db_conn.content.perfManager
        metricId = vim.PerformanceManager.MetricId(counterId=counterId, instance=instance)
        startTime = vchtime - timedelta(minutes=(interval + 1))
        endTime = vchtime - timedelta(minutes=1)

        query = vim.PerformanceManager.QuerySpec(intervalId=20, entity=vm, metricId=[metricId], startTime=startTime,
                                                 endTime=endTime)
        perfResults = perfManager.QueryPerf(querySpec=[query])
        try:
            assert perfResults not in [None, [], '']
            return perfResults
        except AssertionError:
            print('ERROR: Performance results empty.  TIP: Check time drift on source and vCenter server')
            print('Troubleshooting info:')
            print('vCenter/host date and time: {}'.format(vchtime))
            print('Start perf counter time   :  {}'.format(startTime))
            raise AssertionError(query)

    def get_performance_counters(self):
        # Get all the performance counters
        self.perf_list = self.db_conn.content.perfManager.perfCounter
        for counter in self.perf_list:
            counter_full = "{}.{}.{}".format(counter.groupInfo.key, counter.nameInfo.key, counter.rollupType)
            self.perf_dict[counter_full] = counter.key
        return self.perf_dict


    def get_vm_properties(db_conn, managed_obj=None, props=None, property_type=vim.VirtualMachine):
        content = db_conn.content
        if managed_obj is None:
            managed_obj = get_all_vms_view(db_conn)
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
        gpOutput = []
        for eachProp in total_props:
            propDic = dict()
            for prop in eachProp.propSet:
                propDic[prop.name] = prop.val
            propDic['moref'] = eachProp.obj
            gpOutput.append(propDic)
        return gpOutput

    def stat_check(self, perf_dict, counter_name):
        counter_key = perf_dict[counter_name]
        return counter_key

def print_vm_info(virtual_machine):
    """
    Print information for a particular virtual machine or recurse into a
    folder with depth protection
    """
    summary = virtual_machine.summary
    print("Name       : ", summary.config.name)
    print("Template   : ", summary.config.template)
    print("Path       : ", summary.config.vmPathName)
    print("Guest      : ", summary.config.guestFullName)
    print("Instance UUID : ", summary.config.instanceUuid)
    print("Bios UUID     : ", summary.config.uuid)
    annotation = summary.config.annotation
    if annotation:
        print("Annotation : ", annotation)
    print("State      : ", summary.runtime.powerState)
    if summary.guest is not None:
        ip_address = summary.guest.ipAddress
        tools_version = summary.guest.toolsStatus
        if tools_version is not None:
            print("VMware-tools: ", tools_version)
        else:
            print("Vmware-tools: None")
        if ip_address:
            print("IP         : ", ip_address)
        else:
            print("IP         : None")
    if summary.runtime.question is not None:
        print("Question  : ", summary.runtime.question.text)
    print("")


def print_vm_details(vm, db_conn, perf_dict, vchtime=None, interval=1):
    content = db_conn.content
    if vchtime is None:
        vchtime = db_conn.CurrentTime()
    statInt = interval * 3  # There are 3 20s samples in each minute
    summary = vm.summary
    disk_list = []
    network_list = []

    # Convert limit and reservation values from -1 to None
    if vm.resourceConfig.cpuAllocation.limit == -1:
        vmcpulimit = "None"
    else:
        vmcpulimit = "{} Mhz".format(vm.resourceConfig.cpuAllocation.limit)
    if vm.resourceConfig.memoryAllocation.limit == -1:
        vmmemlimit = "None"
    else:
        vmmemlimit = "{} MB".format(vm.resourceConfig.cpuAllocation.limit)

    if vm.resourceConfig.cpuAllocation.reservation == 0:
        vmcpures = "None"
    else:
        vmcpures = "{} Mhz".format(vm.resourceConfig.cpuAllocation.reservation)
    if vm.resourceConfig.memoryAllocation.reservation == 0:
        vmmemres = "None"
    else:
        vmmemres = "{} MB".format(vm.resourceConfig.memoryAllocation.reservation)

    vm_hardware = vm.config.hardware
    for each_vm_hardware in vm_hardware.device:
        if (each_vm_hardware.key >= 2000) and (each_vm_hardware.key < 3000):
            disk_list.append('{} | {:.1f}GB | Thin: {} | {}'.format(each_vm_hardware.deviceInfo.label,
                                                         each_vm_hardware.capacityInKB/1024/1024,
                                                         each_vm_hardware.backing.thinProvisioned,
                                                         each_vm_hardware.backing.fileName))
        elif (each_vm_hardware.key >= 4000) and (each_vm_hardware.key < 5000):
            network_list.append('{} | {} | {}'.format(each_vm_hardware.deviceInfo.label,
                                                         each_vm_hardware.deviceInfo.summary,
                                                         each_vm_hardware.macAddress))

    #CPU Ready Average
    statCpuReady = build_query(content, vchtime, (stat_check(perf_dict, 'cpu.ready.summation')), "", vm, interval)
    cpuReady = (float(sum(statCpuReady[0].value[0].value)) / statInt)
    #CPU Usage Average % - NOTE: values are type LONG so needs divided by 100 for percentage
    statCpuUsage = build_query(content, vchtime, (stat_check(perf_dict, 'cpu.usage.average')), "", vm, interval)
    cpuUsage = ((float(sum(statCpuUsage[0].value[0].value)) / statInt) / 100)
    #Memory Active Average MB
    statMemoryActive = build_query(content, vchtime, (stat_check(perf_dict, 'mem.active.average')), "", vm, interval)
    memoryActive = (float(sum(statMemoryActive[0].value[0].value) / 1024) / statInt)
    #Memory Shared
    statMemoryShared = build_query(content, vchtime, (stat_check(perf_dict, 'mem.shared.average')), "", vm, interval)
    memoryShared = (float(sum(statMemoryShared[0].value[0].value) / 1024) / statInt)
    #Memory Balloon
    statMemoryBalloon = build_query(content, vchtime, (stat_check(perf_dict, 'mem.vmmemctl.average')), "", vm, interval)
    memoryBalloon = (float(sum(statMemoryBalloon[0].value[0].value) / 1024) / statInt)
    #Memory Swapped
    statMemorySwapped = build_query(content, vchtime, (stat_check(perf_dict, 'mem.swapped.average')), "", vm, interval)
    memorySwapped = (float(sum(statMemorySwapped[0].value[0].value) / 1024) / statInt)
    #Datastore Average IO
    statDatastoreIoRead = build_query(content, vchtime, (stat_check(perf_dict, 'datastore.numberReadAveraged.average')),
                                     "*", vm, interval)
    DatastoreIoRead = (float(sum(statDatastoreIoRead[0].value[0].value)) / statInt)
    statDatastoreIoWrite = build_query(content, vchtime, (stat_check(perf_dict, 'datastore.numberWriteAveraged.average')),
                                      "*", vm, interval)
    DatastoreIoWrite = (float(sum(statDatastoreIoWrite[0].value[0].value)) / statInt)
    #Datastore Average Latency
    statDatastoreLatRead = build_query(content, vchtime, (stat_check(perf_dict, 'datastore.totalReadLatency.average')),
                                      "*", vm, interval)
    DatastoreLatRead = (float(sum(statDatastoreLatRead[0].value[0].value)) / statInt)
    statDatastoreLatWrite = build_query(content, vchtime, (stat_check(perf_dict, 'datastore.totalWriteLatency.average')),
                                       "*", vm, interval)
    DatastoreLatWrite = (float(sum(statDatastoreLatWrite[0].value[0].value)) / statInt)

    #Network usage (Tx/Rx)
    statNetworkTx = build_query(content, vchtime, (stat_check(perf_dict, 'net.transmitted.average')), "", vm, interval)
    networkTx = (float(sum(statNetworkTx[0].value[0].value) * 8 / 1024) / statInt)
    statNetworkRx = build_query(content, vchtime, (stat_check(perf_dict, 'net.received.average')), "", vm, interval)
    networkRx = (float(sum(statNetworkRx[0].value[0].value) * 8 / 1024) / statInt)

    print('\nNOTE: Any VM statistics are averages of the last {} minutes\n'.format(statInt / 3))
    print('Server Name                    :', summary.config.name)
    print('Description                    :', summary.config.annotation)
    print('Guest                          :', summary.config.guestFullName)
    if vm.rootSnapshot:
        print('Snapshot Status                : Snapshots present')
    else:
        print('Snapshot Status                : No Snapshots')
    print('VM .vmx Path                   :', summary.config.vmPathName)
    try:
        print('Virtual Disks                  :', disk_list[0])
        if len(disk_list) > 1:
            disk_list.pop(0)
            for each_disk in disk_list:
                print('                                ', each_disk)
    except IndexError:
        pass
    print('Virtual NIC(s)                 :', network_list[0])
    if len(network_list) > 1:
        network_list.pop(0)
        for each_vnic in network_list:
            print('                                ', each_vnic)
    print('[VM] Limits                    : CPU: {}, Memory: {}'.format(vmcpulimit, vmmemlimit))
    print('[VM] Reservations              : CPU: {}, Memory: {}'.format(vmcpures, vmmemres))
    print('[VM] Number of vCPUs           :', summary.config.numCpu)
    print('[VM] CPU Ready                 : Average {:.1f} %, Maximum {:.1f} %'.format((cpuReady / 20000 * 100),
                                                                                       ((float(max(
                                                                                           statCpuReady[0].value[
                                                                                               0].value)) / 20000 * 100))))
    print('[VM] CPU (%)                   : {:.0f} %'.format(cpuUsage))
    print('[VM] Memory                    : {} MB ({:.1f} GB)'.format(summary.config.memorySizeMB, (float(summary.config.memorySizeMB) / 1024)))
    print('[VM] Memory Shared             : {:.0f} %, {:.0f} MB'.format(
        ((memoryShared / summary.config.memorySizeMB) * 100), memoryShared))
    print('[VM] Memory Balloon            : {:.0f} %, {:.0f} MB'.format(
        ((memoryBalloon / summary.config.memorySizeMB) * 100), memoryBalloon))
    print('[VM] Memory Swapped            : {:.0f} %, {:.0f} MB'.format(
        ((memorySwapped / summary.config.memorySizeMB) * 100), memorySwapped))
    print('[VM] Memory Active             : {:.0f} %, {:.0f} MB'.format(
        ((memoryActive / summary.config.memorySizeMB) * 100), memoryActive))
    print('[VM] Datastore Average IO      : Read: {:.0f} IOPS, Write: {:.0f} IOPS'.format(DatastoreIoRead,
                                                                                          DatastoreIoWrite))
    print('[VM] Datastore Average Latency : Read: {:.0f} ms, Write: {:.0f} ms'.format(DatastoreLatRead,
                                                                                      DatastoreLatWrite))
    print('[VM] Overall Network Usage     : Transmitted {:.3f} Mbps, Received {:.3f} Mbps'.format(networkTx, networkRx))
    print('[Host] Name                    : {}'.format(summary.runtime.host.name))
    print('[Host] CPU Detail              : Processor Sockets: {}, Cores per Socket {}'.format(
        summary.runtime.host.summary.hardware.numCpuPkgs,
        (summary.runtime.host.summary.hardware.numCpuCores / summary.runtime.host.summary.hardware.numCpuPkgs)))
    print('[Host] CPU Type                : {}'.format(summary.runtime.host.summary.hardware.cpuModel))
    print('[Host] CPU Usage               : Used: {} Mhz, Total: {} Mhz'.format(
        summary.runtime.host.summary.quickStats.overallCpuUsage,
        (summary.runtime.host.summary.hardware.cpuMhz * summary.runtime.host.summary.hardware.numCpuCores)))
    print('[Host] Memory Usage            : Used: {:.0f} GB, Total: {:.0f} GB\n'.format(
        (float(summary.runtime.host.summary.quickStats.overallMemoryUsage) / 1024),
        (float(summary.runtime.host.summary.hardware.memorySize) / 1024 / 1024 / 1024)))

    print


def main():
    """
    Main module implementing the business logic.
    :return:
    """

    si = create_vSphere_connection()
    vms = get_all_vms_view(si)  # Returns an array of VMs
    assert vms.view[0] is not None


if __name__ == '__main__':
    main()
