#!/usr/bin/env python
"""
Python program for flat text listing the VMs on an
ESX / vCenter, host one per line.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""
from __future__ import print_function
import atexit
from pyVim.connect import SmartConnectNoSSL, Disconnect
from pyVmomi import vim
from tools import cli
import pandas as pd
import numpy as np

MAX_DEPTH = 10


arr=[]

def setup_args():

    """
    Get standard connection arguments
    """
    parser = cli.build_arg_parser()
    my_args = parser.parse_args()

    return cli.prompt_for_password(my_args)


def printvminfo(vm, depth=1):

    # if this is a group it will have children. if it does, recurse into them
    # and then return
    if hasattr(vm, 'childEntity'):
        if depth > MAX_DEPTH:
            return
        vmlist = vm.childEntity
        for child in vmlist:
             printvminfo(child, depth+1)
        return

    summary = vm.summary
    if (summary.config.guestId=="crxPod1Guest" and "harbor" not in summary.config.name ):
        utilizationrate = summary.quickStats.guestMemoryUsage/summary.config.memorySizeMB
        
        d = {"pod_name":summary.config.name.rsplit('-', 2)[0],"mem_utilization":utilizationrate}
        arr.append(d)


    #if (summary.config.guestId=="crxPod1Guest"):
        #print("VM Name:" + summary.config.name)
        #print(summary.config.name.rsplit('-', 2)[0])
        #print("GUEST MemoryUsedMB:" + str(summary.quickStats.guestMemoryUsage)+" MB")
        #print("HOST MemoryUsedMB:" + str(vm.config)+" MB")
        #print(summary.config.memorySizeMB)
        #print (summary.config.guestId)
    
    


def main():
    """
    Simple command-line program for listing the virtual machines on a host.
    """
    args = setup_args()
    si = None
    try:
        si = SmartConnectNoSSL(host=args.host,
                               user=args.user,
                               pwd=args.password,
                               port=int(args.port))
        atexit.register(Disconnect, si)
    except vim.fault.InvalidLogin:
        raise SystemExit("Unable to connect to host "
                         "with supplied credentials.")
   
    
    list=[]
    content = si.RetrieveContent()
    for child in content.rootFolder.childEntity:
        if hasattr(child, 'vmFolder'):
            datacenter = child
            vmfolder = datacenter.vmFolder
            vmlist = vmfolder.childEntity
            for vm in vmlist:

                summary=printvminfo(vm)
                #print(summary)
                #list.append(printvminfo(vm))
    print(arr)
    print("--------------------------------")
    
    
    
    """
    group by key for array
    the memory utilization has been hardcoded for now in line 113. Please adjust to your desired value for example 0.3 which is 30%. 
    """
    df = pd.DataFrame(arr)
    dftemp= df.groupby('pod_name', as_index=False).sum()
    dftemp['desired_mem_utilization']=float(0.5) 
    dftemp['desired_replicas'] = (dftemp['mem_utilization']/dftemp['desired_mem_utilization']).apply(np.ceil)
    d = dftemp.to_dict('records')
    print(d)

# Start program
if __name__ == "__main__":
    main()
