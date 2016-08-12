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

        def find_device(vm, deviceName)
          vm.config.hardware.device.each do |device|
            return device if device.deviceInfo.label == deviceName
          end
          nil
        end

        def validate_config(disks)
          puts "disks: #{disks}"

          disks.each do |disk|
            create = disk['create']
            type = disk['type']

            if create.nil?
              puts "Missing required attribute 'create' for disk: #{disk}"
              exit(-1)
            end

            if create == true && type.nil?
              puts "'create' attribute was provided, but 'type' attribute missing for disk: #{disk}"
              exit(-1)
            end

            size = disk['size']

            if create == true && size.nil?
              puts "'create' attribute was provided, but 'size' attribute missing for #{disk}"
              exit(-1)
            end

            path = disk['path']

            if create == false && path.nil?
              puts "'create' attribute was not provided and 'path' attribute missing for #{disk}"
              exit(-2)
            end
          end
        end

        def call(env)
          machine = env[:machine]
          return if machine.state.id == :not_created

          config = machine.provider_config
          disks = config.disks

          validate_config disks

          vim = env[:vSphere_connection]
          vdm = vim.serviceContent.virtualDiskManager
          vm = get_vm_by_uuid vim, machine

          return if vm.nil?

          vmname = vm.summary.config.name

          puts "VM name #{vmname}"

          datacenter = get_datacenter vim, machine

          disks.each do |disk|
            create_disk = disk['create']
            data_store_name = disk['data_store_name']

            puts "Disk: #{disk}"

            if data_store_name.nil?
              vmdk_datastore = get_datastore datacenter, machine
            else
              vmdk_datastore = get_datastore_by_name datacenter, data_store_name
            end

            puts "Choosing: #{vmdk_datastore.name}"

            if create_disk == true
              size = disk['size']
              vmdk_size_kb = size.to_i * 1024
              vmdk_type = disk['type']

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

              vmdk_type = 'preallocated' if vmdk_type == 'thick'
              puts "Next vmdk name is => #{vmdk_name}"

              # create the disk
              unless vmdk_datastore.exists? vmdk_file_name
                vmdk_spec = RbVmomi::VIM::FileBackedVirtualDiskSpec(
                    adapterType: 'lsiLogic',
                    capacityKb: vmdk_size_kb,
                    diskType: vmdk_type
                )

                puts "Creating VMDK #{vmdk_name} #{size} GB"

                vdm.CreateVirtualDisk_Task(
                    datacenter: datacenter,
                    name: vmdk_name,
                    spec: vmdk_spec
                ).wait_for_completion
              end
            else
              path = disk['path']
              disk_exists = false

              pc = vmdk_datastore._connection.serviceContent.propertyCollector
              vms = vmdk_datastore.vm
              vm_files = pc.collectMultiple vms, 'layoutEx.file'
              vm_files.keys.each do |vmFile|
                vm_files[vmFile]['layoutEx.file'].each do |layout|
                  if layout.name.match(/^\[#{vmdk_datastore.name}\] #{path}/)
                    disk_exists = true
                    break
                  end
                end
              end

              if disk_exists == true
                puts "Trying to attach disk '[#{vmdk_datastore.name}] #{path}' but it is already attached to this VM. Exiting..."
                exit(-2)
              else
                vmdk_name = "[#{vmdk_datastore.name}] #{path}"
              end
            end

            puts "Attaching VMDK to #{vmname}"

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
              if scsi_tree[controller]['children'].length < 15 # Virtual SCSI targets per virtual SCSI adapters
                available_controllers.push(scsi_tree[controller]['device'].deviceInfo.label)
              end
            end

            if available_controllers.length > 0
              use_controller = available_controllers[0]
              puts "using #{use_controller}"
            else

              if scsi_tree.keys.length < 4 # Virtual SCSI adapters per virtual machine

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

                vm.ReconfigVM_Task(spec: vm_config_spec).wait_for_completion
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

            vm.ReconfigVM_Task(spec: vm_config_spec).wait_for_completion
          end
        end
      end
    end
  end
end