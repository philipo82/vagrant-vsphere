require 'rbvmomi'
require 'vSphere/util/vim_helpers'

module VagrantPlugins
  module VSphere
    module Action
      class AddVMDK
		include Util::VimHelpers

		def initialize(app, _env)
			@app = app
        end

		def call(env)
			vim = vim_connection
			vdm = vim.serviceContent.virtualDiskManager

			machine = env[:machine]
		
			return if machine.state.id == :not_created
			
			vm = get_vm_by_uuid connection, machine
	        
			return if vm.nil?

            dc = get_datacenter connection, machine
			datastore = get_datastore dc, machine
		
			size = @name_args[1]
			if size.nil?
			  ui.fatal 'You need a VMDK size!'
			  show_usage
			  exit 1
			end

			fatal_exit "Could not find #{vmname}" unless vm

			target_lun = get_config(:target_lun) unless get_config(:target_lun).nil?
			vmdk_size_kb = size.to_i * 1024 * 1024

			if target_lun.nil?
			  vmdk_datastore = choose_datastore(vm.datastore, size)
			  exit(-1) if vmdk_datastore.nil?
			else
			  vmdk_datastores = find_datastores_regex(target_lun)
			  vmdk_datastore = choose_datastore(vmdk_datastores, size)
			  exit(-1) if vmdk_datastore.nil?
			  vmdk_dir = "[#{vmdk_datastore.name}] #{vmname}"
			  # create the vm folder on the LUN or subsequent operations will fail.
			  unless vmdk_datastore.exists? vmname
				dc = datacenter
				begin
				  dc._connection.serviceContent.fileManager.MakeDirectory name: vmdk_dir, datacenter: dc, createParentDirectories: false
				rescue RbVmomi::Fault => e
				  ui.warn "Ignoring a fault when trying to create #{vmdk_dir}. This may be related to issue #213."
				  if log_verbose?
					puts "Chose #{vmdk_datastore.name} out of #{vmdk_datastores.map(&:name).join(', ')}"
					puts e
				  end
				end
			  end
			end

			puts "Choosing: #{vmdk_datastore.name}"

			# now we need to inspect the files in this datastore to get our next file name
			next_vmdk = 1
			pc = vmdk_datastore._connection.serviceContent.propertyCollector
			vms = vmdk_datastore.vm
			vm_files = pc.collectMultiple vms, 'layoutEx.file'
			vm_files.keys.each do |vmFile|
			  vm_files[vmFile]['layoutEx.file'].each do |layout|
				if layout.name.match(/^\[#{vmdk_datastore.name}\] #{vmname}\/#{vmname}_([0-9]+).vmdk/)
				  num = Regexp.last_match(1)
				  next_vmdk = num.to_i + 1 if next_vmdk <= num.to_i
				end
			  end
			end
			vmdk_file_name = "#{vmname}/#{vmname}_#{next_vmdk}.vmdk"
			vmdk_name = "[#{vmdk_datastore.name}] #{vmdk_file_name}"
			vmdk_type = get_config(:vmdk_type)
			vmdk_type = 'preallocated' if vmdk_type == 'thick'
			puts "Next vmdk name is => #{vmdk_name}"

			# create the disk
			unless vmdk_datastore.exists? vmdk_file_name
			  vmdk_spec = RbVmomi::VIM::FileBackedVirtualDiskSpec(
				adapterType: 'lsiLogic',
				capacityKb: vmdk_size_kb,
				diskType: vmdk_type
			  )
			  ui.info 'Creating VMDK'
			  ui.info "#{ui.color 'Capacity:', :cyan} #{size} GB"
			  ui.info "#{ui.color 'Disk:', :cyan} #{vmdk_name}"

			  if get_config(:noop)
				ui.info "#{ui.color 'Skipping disk creation process because --noop specified.', :red}"
			  else
				vdm.CreateVirtualDisk_Task(
				  datacenter: datacenter,
				  name: vmdk_name,
				  spec: vmdk_spec
				).wait_for_completion
			  end
			end
			ui.info "Attaching VMDK to #{vmname}"

			# now we run through the SCSI controllers to see if there's an available one
			available_controllers = []
			use_controller = nil
			scsi_tree = {}
			vm.config.hardware.device.each do |device|
			  if device.is_a? RbVmomi::VIM::VirtualSCSIController
				if scsi_tree[device.controllerKey].nil?
				  scsi_tree[device.key] = {}
				  scsi_tree[device.key]['children'] = []
				end
				scsi_tree[device.key]['device'] = device
			  end
			  next unless device.class == RbVmomi::VIM::VirtualDisk
			  if scsi_tree[device.controllerKey].nil?
				scsi_tree[device.controllerKey] = {}
				scsi_tree[device.controllerKey]['children'] = []
			  end
			  scsi_tree[device.controllerKey]['children'].push(device)
			end
			scsi_tree.keys.sort.each do |controller|
			  if scsi_tree[controller]['children'].length < 15
				available_controllers.push(scsi_tree[controller]['device'].deviceInfo.label)
			  end
			end

			if available_controllers.length > 0
			  use_controller = available_controllers[0]
			  puts "using #{use_controller}"
			else

			  if scsi_tree.keys.length < 4

				# Add a controller if none are available
				puts 'no controllers available. Will attempt to create'
				new_scsi_key = scsi_tree.keys.sort[scsi_tree.length - 1] + 1
				new_scsi_bus_number = scsi_tree[scsi_tree.keys.sort[scsi_tree.length - 1]]['device'].busNumber + 1

				controller_device = RbVmomi::VIM::VirtualLsiLogicController(
				  key: new_scsi_key,
				  busNumber: new_scsi_bus_number,
				  sharedBus: :noSharing
				)

				device_config_spec = RbVmomi::VIM::VirtualDeviceConfigSpec(
				  device: controller_device,
				  operation: RbVmomi::VIM::VirtualDeviceConfigSpecOperation('add')
				)

				vm_config_spec = RbVmomi::VIM::VirtualMachineConfigSpec(
				  deviceChange: [device_config_spec]
				)

				if get_config(:noop)
				  ui.info "#{ui.color 'Skipping controller creation process because --noop specified.', :red}"
				else
				  vm.ReconfigVM_Task(spec: vm_config_spec).wait_for_completion
				end
			  else
				ui.info 'Controllers maxed out at 4.'
				exit(-1)
			  end
			end

			# now go back and get the new device's name
			vm.config.hardware.device.each do |device|
			  if device.class == RbVmomi::VIM::VirtualLsiLogicController
				use_controller = device.deviceInfo.label if device.key == new_scsi_key
			  end
			end

			# add the disk
			controller = find_device(vm, use_controller)

			used_unit_numbers = []
			scsi_tree.keys.sort.each do |c|
			  next unless controller.key == scsi_tree[c]['device'].key
			  used_unit_numbers.push(scsi_tree[c]['device'].scsiCtlrUnitNumber)
			  scsi_tree[c]['children'].each do |disk|
				used_unit_numbers.push(disk.unitNumber)
			  end
			end

			available_unit_numbers = []
			(0..15).each do |scsi_id|
			  if used_unit_numbers.grep(scsi_id).length > 0
			  else
				available_unit_numbers.push(scsi_id)
			  end
			end

			# ensure we don't try to add the controllers SCSI ID
			new_unit_number = available_unit_numbers.sort[0]
			puts "using SCSI ID #{new_unit_number}"

			vmdk_backing = RbVmomi::VIM::VirtualDiskFlatVer2BackingInfo(
			  datastore: vmdk_datastore,
			  diskMode: 'persistent',
			  fileName: vmdk_name
			)

			device = RbVmomi::VIM::VirtualDisk(
			  backing: vmdk_backing,
			  capacityInKB: vmdk_size_kb,
			  controllerKey: controller.key,
			  key: -1,
			  unitNumber: new_unit_number
			)

			device_config_spec = RbVmomi::VIM::VirtualDeviceConfigSpec(
			  device: device,
			  operation: RbVmomi::VIM::VirtualDeviceConfigSpecOperation('add')
			)

			vm_config_spec = RbVmomi::VIM::VirtualMachineConfigSpec(
			  deviceChange: [device_config_spec]
			)

			if get_config(:noop)
			  ui.info "#{ui.color 'Skipping disk attaching process because --noop specified.', :red}"
			else
			  vm.ReconfigVM_Task(spec: vm_config_spec).wait_for_completion
			end
		  end

        def call(env)


		env[:ui].info "Provisioning vsphere"
		
		connection = env[:vSphere_connection]
		machine = env[:machine]
		
		return if machine.state.id == :not_created
	        vm = get_vm_by_uuid connection, machine
	        return if vm.nil?

                dc = get_datacenter connection, machine

		env[:ui].info "1"

		datastore = get_datastore dc, machine
		
		env[:ui].info "2"

        	vmdk_spec = RbVmomi::VIM::FileBackedVirtualDiskSpec(
			capacityKb: 1024 * 1024,
        	    	adapterType: 'lsiLogic',
	        	diskType: 'thin'
	        )
	        
	        env[:ui].info "3"

		vmdk_backing = RbVmomi::VIM::VirtualDiskFlatVer2BackingInfo(
			datastore: datastore,
		        diskMode: 'persistent',
		        fileName: "[TRUST-01] jakub_14/jakub-2.vmdk",
		)
		
		env[:ui].info "4"
		
		# now we run through the SCSI controllers to see if there's an available one
    available_controllers = []
    use_controller = nil
    scsi_tree = {}
    vm.config.hardware.device.each do |device|
      if device.is_a? RbVmomi::VIM::VirtualSCSIController
        if scsi_tree[device.controllerKey].nil?
          scsi_tree[device.key] = {}
          scsi_tree[device.key]['children'] = []
        end
        scsi_tree[device.key]['device'] = device
      end
      next unless device.class == RbVmomi::VIM::VirtualDisk
      if scsi_tree[device.controllerKey].nil?
        scsi_tree[device.controllerKey] = {}
        scsi_tree[device.controllerKey]['children'] = []
      end
      scsi_tree[device.controllerKey]['children'].push(device)
    end
    scsi_tree.keys.sort.each do |controller|
      if scsi_tree[controller]['children'].length < 15
        available_controllers.push(scsi_tree[controller]['device'].deviceInfo.label)
      end
    end

    if available_controllers.length > 0
      use_controller = available_controllers[0]
      puts "using #{use_controller}"
    else

      if scsi_tree.keys.length < 4

        # Add a controller if none are available
        puts 'no controllers available. Will attempt to create'
        new_scsi_key = scsi_tree.keys.sort[scsi_tree.length - 1] + 1
        new_scsi_bus_number = scsi_tree[scsi_tree.keys.sort[scsi_tree.length - 1]]['device'].busNumber + 1

        controller_device = RbVmomi::VIM::VirtualLsiLogicController(
          key: new_scsi_key,
          busNumber: new_scsi_bus_number,
          sharedBus: :noSharing
        )

        device_config_spec = RbVmomi::VIM::VirtualDeviceConfigSpec(
          device: controller_device,
          operation: RbVmomi::VIM::VirtualDeviceConfigSpecOperation('add')
        )

        vm_config_spec = RbVmomi::VIM::VirtualMachineConfigSpec(
          deviceChange: [device_config_spec]
        )

        if get_config(:noop)
          ui.info "#{ui.color 'Skipping controller creation process because --noop specified.', :red}"
        else
          vm.ReconfigVM_Task(spec: vm_config_spec).wait_for_completion
        end
      else
        ui.info 'Controllers maxed out at 4.'
        exit(-1)
      end
    end
		
		vm.config.hardware.device.each do |device|
		      puts device.class
		      
		      if device.class == RbVmomi::VIM::VirtualLsiLogicController
		              use_controller = device.deviceInfo.label if device.key == new_scsi_key
                      end
                end
                
                controller = find_device(vm, use_controller)
		
		device = RbVmomi::VIM::VirtualDisk(
			backing: vmdk_backing,
			controllerKey: controller.key,
			key: -1,
			unitNumber:3,
			capacityInKB: 1024 * 1024,
                )
                
                env[:ui].info "5"

		device_config_spec = RbVmomi::VIM::VirtualDeviceConfigSpec(
			device: device,
			operation: RbVmomi::VIM::VirtualDeviceConfigSpecOperation('add')
	        )
	        
	        env[:ui].info "6"
	        
	        vm_config_spec = RbVmomi::VIM::VirtualMachineConfigSpec(
		        deviceChange: [device_config_spec]
                )
                
                env[:ui].info "7"
                
	        create_disk_task = connection.serviceContent.virtualDiskManager.CreateVirtualDisk_Task( 
			datacenter: dc,
	        	name: "[TRUST-01] jakub_14/jakub-2.vmdk",
	        	spec: vmdk_spec
	        ).wait_for_completion
	        
	        env[:ui].info "8"
	        
	        
	        
	        
	        env[:ui].info "9"
	        
	        vm.ReconfigVM_Task(spec: vm_config_spec).wait_for_completion
	        
	        env[:ui].info "10"
        end
      end
    end
  end
end