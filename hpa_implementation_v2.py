import sys
import subprocess
from pyVmomi import vim, vmodl
from pyVim.connect import SmartConnectNoSSL, Disconnect
from pyVim.task import WaitForTask
from tools import cli
import pandas as pd
import numpy as np


def setup_args():
    parser = cli.build_arg_parser()
    parser.add_argument('-n', '--property', default='runtime.powerState',
                        help='Name of the property to filter by')
    parser.add_argument('-v', '--value', default='poweredOn',
                        help='Value to filter with')
    return cli.prompt_for_password(parser.parse_args())


def get_obj(si, root, vim_type):
    container = si.content.viewManager.CreateContainerView(root, vim_type,
                                                           True)
    view = container.view
    container.Destroy()
    return view


def create_filter_spec(pc, vms, prop):
    objSpecs = []
    for vm in vms:
        objSpec = vmodl.query.PropertyCollector.ObjectSpec(obj=vm)
        objSpecs.append(objSpec)
    filterSpec = vmodl.query.PropertyCollector.FilterSpec()
    filterSpec.objectSet = objSpecs
    propSet = vmodl.query.PropertyCollector.PropertySpec(all=False)
    propSet.type = vim.VirtualMachine
    propSet.pathSet = [prop]
    filterSpec.propSet = [propSet]
    return filterSpec


def hpa_algo(vms, memory_threshold_utilization):
    arr =[]
    for vm in vms:
        summary= vm.summary
        utilizationrate = summary.quickStats.guestMemoryUsage/summary.config.memorySizeMB
        if ("harbor" not in summary.config.name ):
            d = {"pod_name":summary.config.name.rsplit('-', 2)[0],"mem_utilization":utilizationrate}
        arr.append(d)
    df = pd.DataFrame(arr)
    dftemp= df.groupby('pod_name', as_index=False).sum()
    dftemp['desired_mem_utilization']=float(memory_threshold_utilization) 
    dftemp['desired_replicas'] = (dftemp['mem_utilization']/dftemp['desired_mem_utilization']).apply(np.ceil)
    dftemp['desired_replicas'] = np.round(dftemp['desired_replicas']).astype('Int64')
    d = dftemp.to_dict('records')
    return d

def filter_results(result, value):
    vms = []
    for o in result.objects:
        if o.propSet[0].val == value:
            vms.append(o.obj)
    return vms


def main():
    args = setup_args()
    filter_key = "summary.config.guestId"
    value = "crxPod1Guest" 
    memory_threshold_utilization = 0.5
    si = SmartConnectNoSSL(host=args.host,
                           user=args.user,
                           pwd=args.password,
                           port=args.port)
    # Start with all the VMs from container, which is easier to write than
    # PropertyCollector to retrieve them.
    vms = get_obj(si, si.content.rootFolder, [vim.VirtualMachine])

    pc = si.content.propertyCollector
    filter_spec = create_filter_spec(pc, vms, filter_key)
    options = vmodl.query.PropertyCollector.RetrieveOptions()
    result = pc.RetrievePropertiesEx([filter_spec], options)
    vms = filter_results(result, value)
    hpa_vms = hpa_algo(vms,memory_threshold_utilization)
    for vm in hpa_vms:
        #print(vm['pod_name'])
        deployment = vm['pod_name']
        replica_count = "--replicas="+str(vm['desired_replicas'])
        subprocess.call(["kubectl","scale","deployments",deployment,replica_count])
    

    Disconnect(si)


if __name__ == '__main__':
    main()
