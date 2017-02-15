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

        # Validates that the 'disks' attribute configuration is correct:
        # 'create' and 'path' attributes are required
        # If 'create' attribute is provided, 'size' and 'type' attributes
        # are also required.
        def validate_config(disks)
          disks.each do |disk|
            create = disk['create']
            type = disk['type']
            path = disk['path']

            if create.nil?
              puts "Missing required attribute 'create' for disk: #{disk}"
              exit(-1)
            end

            if path.nil?
              puts "Missing required attribute 'path' for disk: #{disk}"
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
          end
        end

        # Checks whether the virtual disk specified by the
        # path already exists in the datastore.
        def find_virtual_disk_in_datastore(datastore, path)
          split_path = path.split(/\//)

          if split_path.empty? || split_path.length < 2
            puts "Incorrect path format. Expected format: path/to/folder/vmdk_name"
            exit(-1)
          end

          vmdk_file_name = split_path[split_path.length - 1]
          split_path.delete_at(split_path.length - 1)
          vmdk_folder = split_path.join("/")

          search_details = RbVmomi::VIM::FileQueryFlags(
            fileOwner: true,
            fileSize: true,
            fileType: true,
            modification: true
          )

          file_query_details = RbVmomi::VIM::VmDiskFileQueryFlags(
            capacityKb: true,
            controllerType: false,
            diskExtents: true,
            diskType: true,
            hardwareVersion: false,
            thin: true
          )

          file_query = RbVmomi::VIM::VmDiskFileQuery(
            details: file_query_details
          )

          search_spec_obj = RbVmomi::VIM::HostDatastoreBrowserSearchSpec(
            details: search_details,
            query: [file_query],
            matchPattern: ["#{vmdk_file_name}"]
          )

          existing_datastore_path = "[#{datastore.name}] #{vmdk_folder}"

          search_task = datastore.browser.SearchDatastoreSubFolders_Task(
            datastorePath: existing_datastore_path,
            searchSpec: search_spec_obj
          )

          search_task.wait_for_completion
          search_result = search_task.info.result

          return nil if search_result.empty?

          files = search_result[0].file

          return nil if files.empty?

          if files.length > 1
            puts "Found more than 1 virtual disks with a given path #{path}. This should not have happened. Exiting..."
            exit(-1)
          end

          # There should be only 1 file matching the search criteria
          files[0]
        end

        # Checks whether the virtual disk specified by the datastore and path
        # is already attached to any VM. If it's attached to other VM,
        # the program displays an error and exits with -1 error code.
        # If it's attached to this VM, it function returns true and lets
        # the caller handle.
        def disk_attached?(datastore, vmdk_path, my_vm)
          pc = datastore._connection.serviceContent.propertyCollector
          vms = datastore.vm

          attached_to_my_vm = false
          other_vm_name = nil

          vms.each do |vm|
            vm_files = pc.collectMultiple [vm], 'layoutEx.file'

            vm_files.keys.each do |vmFile|
              vm_files[vmFile]['layoutEx.file'].each do |layout|
                next unless layout.name.match(/^\[#{datastore.name}\] #{vmdk_path}/)

                if vm == my_vm
                  attached_to_my_vm = true
                else
                  other_vm_name = vm.config.name
                end
              end
            end
          end

          unless other_vm_name.nil?
            puts "The virtual disk #{vmdk_path} is already attached to VM #{other_vm_name}. Exiting..."
            exit(-1)
          end

          attached_to_my_vm
        end

        # Creates new virtual disk in the datastore
        def create_new_disk_in_datastore(datastore, vdm, path, vmdk_size_kb, vmdk_type, datacenter)
          vmdk_full_name = "[#{datastore.name}] #{path}"

          # create the disk if the file doesn't exist
          return if datastore.exists? path

          vmdk_spec = RbVmomi::VIM::FileBackedVirtualDiskSpec(
            adapterType: 'lsiLogic',
            capacityKb: vmdk_size_kb,
            diskType: vmdk_type
          )

          vdm.CreateVirtualDisk_Task(
            datacenter: datacenter,
            name: vmdk_full_name,
            spec: vmdk_spec
          ).wait_for_completion
        end

        def find_scsi_controller_tree(vm)
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

          scsi_tree
        end

        # Finds available SCSI controller. If it doesn't find any controllers
        # it creates one first.
        def find_scsi_controller(vm, scsi_tree)
          # now we run through the SCSI controllers to see if there's an available one
          available_controllers = []
          use_controller = nil

          scsi_tree.keys.sort.each do |controller|
            if scsi_tree[controller]['children'].length < 15 # Virtual SCSI targets per virtual SCSI adapters
              available_controllers.push(scsi_tree[controller]['device'].deviceInfo.label)
            end
          end

          if available_controllers.length > 0
            use_controller = available_controllers[0]
          else
            if scsi_tree.keys.length < 4 # Virtual SCSI adapters per virtual machine

              # Add a controller if none are available
              puts 'No controllers available. Will attempt to create'
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
              puts 'Controllers maxed out at 4.'
              exit(-1)
            end
          end

          # now go back and get the new device's name
          vm.config.hardware.device.each do |device|
            if device.class == RbVmomi::VIM::VirtualLsiLogicController
              use_controller = device.deviceInfo.label if device.key == new_scsi_key
            end
          end

          find_device(vm, use_controller)
        end

        # Finds next available unit number in the provided SCSI controller
        def find_new_unit_number(scsi_tree, ctrl)
          used_unit_numbers = []
          scsi_tree.keys.sort.each do |c|
            next unless ctrl.key == scsi_tree[c]['device'].key
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

          available_unit_numbers.sort[0]
        end

        # Adds new virtual disk to the VM. If this disk is already attached
        # to any other VM, the error is thrown and program exists with errno=-1
        def attach_virtual_disk_to_vm(vm, datastore, vmdk_full_name, vmdk_path, vmdk_size_kb, ctrl_key, unit_number)
          disk_attched_to_vm = disk_attached? datastore, vmdk_path, vm

          if disk_attched_to_vm == true
            # Trying to attach disk but it is already attached to this VM. Skipping...
            return
          end

          vmdk_backing = RbVmomi::VIM::VirtualDiskFlatVer2BackingInfo(
              datastore: datastore,
              diskMode: 'persistent',
              fileName: vmdk_full_name
          )

          device = RbVmomi::VIM::VirtualDisk(
              backing: vmdk_backing,
              capacityInKB: vmdk_size_kb,
              controllerKey: ctrl_key,
              key: -1,
              unitNumber: unit_number
          )

          already_attached_disks = nil

          if !vm.config.nil? && !vm.config.extraConfig.nil?
            vm.config.extraConfig.each do |extraOption|
              if extraOption.key == "AttachedDisks"
                already_attached_disks = extraOption
                break
              end
            end
          end

          if already_attached_disks.nil?
            already_attached_disks = RbVmomi::VIM::OptionValue(
              key: "AttachedDisks",
              value: vmdk_backing.fileName
            )
          else
            already_attached_disks.value = already_attached_disks.value + "," + vmdk_backing.fileName
          end

          device_config_spec = RbVmomi::VIM::VirtualDeviceConfigSpec(
              device: device,
              operation: RbVmomi::VIM::VirtualDeviceConfigSpecOperation('add')
          )

          vm_config_spec = RbVmomi::VIM::VirtualMachineConfigSpec(
              deviceChange: [device_config_spec],
              extraConfig: [already_attached_disks]
          )

          vm.ReconfigVM_Task(spec: vm_config_spec).wait_for_completion
        end

        # Verifies that the folder specified by the path exists
        # in the datastore. If it doesn't, it is created
        def verify_vmdir_exists(datastore, datacenter, path)
          split_path = path.split(/\//)

          if split_path.empty? || split_path.length < 2
            puts "Incorrect path format. Expected format: path/to/folder/vmdk_name"
            exit(-1)
          end

          split_path.delete_at(split_path.length - 1)
          vmdk_folder = split_path.join("/")

          return if datastore.exists? vmdk_folder

          dc = datacenter
          vmdk_dir = "[#{datastore.name}] #{vmdk_folder}"
          begin
            dc._connection.serviceContent.fileManager.MakeDirectory name: vmdk_dir, datacenter: dc, createParentDirectories: true
          rescue RbVmomi::Fault
            puts "Error when creating directory #{vmdk_dir}."
            exit(-1)
          end
        end

        def call(env)
          machine = env[:machine]

          if machine.state.id == :not_created
            puts 'VM is not created. Exiting...'
            return
          end

          config = machine.provider_config
          disks = config.disks

          return if disks.nil?

          validate_config disks

          vim = env[:vSphere_connection]
          vm = get_vm_by_uuid vim, machine

          if vm.nil?
            puts 'Did not find the specified VM. Exiting...'
            return
          end

          datacenter = get_datacenter vim, machine
          vmdk_datastore = get_datastore datacenter, machine

          disks.each do |disk|
            create_disk = disk['create']
            path = disk['path']

            verify_vmdir_exists vmdk_datastore, datacenter, path

            virtual_disk = find_virtual_disk_in_datastore vmdk_datastore, path
            vmdk_full_name = "[#{vmdk_datastore.name}] #{path}"

            if create_disk == true
              # If create flag is true, we need to grab the provisioning type and size
              size = disk['size']
              vmdk_type = disk['type']
              vmdk_size_kb = size.to_i * 1024

              if !virtual_disk.nil?
                puts "Virtual disk #{path} already created - using this one."
              else
                create_new_disk_in_datastore vmdk_datastore, vim.serviceContent.virtualDiskManager, path, vmdk_size_kb, vmdk_type, datacenter
              end
            else
              if virtual_disk.nil?
                puts "Couldn't find virtual disk specified at #{path} in datastore [#{vmdk_datastore.name}]. Exiting..."
                exit(-1)
              end

              vmdk_size_kb = virtual_disk.capacityKb
            end

            scsi_tree = find_scsi_controller_tree vm
            ctrl = find_scsi_controller vm, scsi_tree

            if ctrl.nil?
              puts "Didn't find any SCSI controllers. Exiting..."
              exit(-1)
            end

            new_unit_number = find_new_unit_number scsi_tree, ctrl

            begin
              attach_virtual_disk_to_vm vm, vmdk_datastore, vmdk_full_name, path, vmdk_size_kb, ctrl.key, new_unit_number
            rescue RbVmomi::Fault => e
              puts "Error when attaching disk #{path}: #{e}."

              exit(-1)
            end
          end

          @app.call env
        end
      end
    end
  end
end
