require 'rbvmomi'
require 'vSphere/util/vim_helpers'

module VagrantPlugins
  module VSphere
    module Action
      class ProvisionVSphere

        def initialize(app, _env)
          @app = app
        end


        def call(env)

        	vmdk_spec = RbVmomi::VIM::FileBackedVirtualDiskSpec(
	            capacityKb: 1024 * 1024,
        	    adapterType: 'lsiLogic',
	            diskType: 'thin'
	        )

		device = RbVmomi::VIM::VirtualDisk(
		    capacityInKB: 1024 * 1024,
                )

		device_config_spec = RbVmomi::VIM::VirtualDeviceConfigSpec(
		    device: device,
		    operation: RbVmomi::VIM::VirtualDeviceConfigSpecOperation('add')
	        )
	        
	        vm_config_spec = RbVmomi::VIM::VirtualMachineConfigSpec (
	            deviceChange: [device_config_spec]
                )

	        create_disk_task = RbVmomi::VIM.VirtualDiskManager.CreateVirtualDisk_Task ( 
			datacenter: @connection.serviceInstance.find_datacenter
	        	name => "[TRUST-01] jakub-test-with-parted/jakub-test-with-parted-2.vmdk"
	        	spec: vmdk_spec
	        ).wait_for_completion
	        
	        return if env[:machine].state.id == :not_created
	        vm = get_vm_by_uuid env[:vSphere_connection], env[:machine]
	        return if vm.nil?
	        
	        vm.ReconfigVM_Task(spec: vm_config_spec).wait_for_completion
        end
      end
    end
  end
end