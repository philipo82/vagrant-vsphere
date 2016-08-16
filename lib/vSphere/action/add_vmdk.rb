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

        def find_virtual_disk_in_datastore(datastore, path)

          split_path = path.split(/\//)

          if split_path.empty? || split_path.length < 2
            puts "Incorrect path format. Expected format: path/to/folder/vmdk_name"
            exit(-1)
          end

          vmdk_file_name = split_path[split_path.length - 1]
          split_path.delete_at(split_path.length - 1)
          vmdk_folder = split_path.join("/")

          searchDetails = RbVmomi::VIM::FileQueryFlags(
              fileOwner: true,
              fileSize: true,
              fileType: true,
              modification: true)

          fileQueryDetails = RbVmomi::VIM::VmDiskFileQueryFlags(
              capacityKb: true,
              controllerType: false,
              diskExtents: true,
              diskType: true,
              hardwareVersion: false,
              thin: true
          )

          fileQuery = RbVmomi::VIM::VmDiskFileQuery(
              details: fileQueryDetails
          )

          searchSpecObj = RbVmomi::VIM::HostDatastoreBrowserSearchSpec(
              details: searchDetails,
              query: [fileQuery],
              matchPattern: ["#{vmdk_file_name}"]
          )

          existingDataStorePath = "[#{datastore.name}] #{vmdk_folder}"

          search_task = datastore.browser.SearchDatastoreSubFolders_Task(
              datastorePath: existingDataStorePath,
              searchSpec: searchSpecObj
          )

          search_task.wait_for_completion
          search_result = search_task.info.result

          if search_result.empty?
            return nil
          end

          files = search_result[0].file

          if files.empty?
            return nil
          end

          if files.length > 1
            puts "Found more than 1 virtual disks with a given path #{path}. This should not have happened. Exiting..."
            exit (-1)
          end

          # There should be only 1 file matching the search criteria
          return files[0]
        end

        # Checks whether the virtual disk specified by the datastore and path
        # is already attached to any VM
        def is_disk_attached (datastore, vmdk_path)
          pc = datastore._connection.serviceContent.propertyCollector
          vms = datastore.vm
          vm_files = pc.collectMultiple vms, 'layoutEx.file'
          vm_files.keys.each do |vmFile|
            vm_files[vmFile]['layoutEx.file'].each do |layout|
              if layout.name.match(/^\[#{datastore.name}\] #{vmdk_path}/)
                return true
              end
            end
          end

          return false
        end

        # Creates new virtual disk in the datastore
        def create_new_disk_in_datastore(datastore, vdm, path, vmdk_size_kb, vmdk_type, datacenter)

          # TODO - thick, preallocated?
          vmdk_type = 'preallocated' if vmdk_type == 'thick'
          vmdk_full_name = "[#{datastore.name}] #{path}"

          # create the disk
          unless datastore.exists? path
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
        end

        def find_scsi_controller_tree (vm)
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

          return scsi_tree
        end

        # Finds available SCSI controller. If it doesn't find any controllers
        # it creates one first.
        def find_scsi_controller (vm, scsi_tree)
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

          return find_device(vm, use_controller)
        end

        def find_new_unit_number (scsi_tree, ctrl)
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

          return available_unit_numbers.sort[0]
        end

        def attach_virtual_disk_to_vm (vm, datastore, vmdk_full_name, vmdk_path, vmdk_size_kb, ctrl_key, unit_number)
          disk_attched_to_vm = is_disk_attached datastore, vmdk_path

          puts "Trying to attach: #{vmdk_full_name}"

          if disk_attched_to_vm == true
            puts "Trying to attach disk '#{vmdk_full_name}' but it is already attached to a VM. Exiting..."
            exit(-1)
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

          device_config_spec = RbVmomi::VIM::VirtualDeviceConfigSpec(
              device: device,
              operation: RbVmomi::VIM::VirtualDeviceConfigSpecOperation('add')
          )

          vm_config_spec = RbVmomi::VIM::VirtualMachineConfigSpec(
              deviceChange: [device_config_spec]
          )

          vm.ReconfigVM_Task(spec: vm_config_spec).wait_for_completion
        end

        def verify_vmdir_exists (datastore, datacenter, path)

          split_path = path.split(/\//)

          if split_path.empty? || split_path.length < 2
            puts "Incorrect path format. Expected format: path/to/folder/vmdk_name"
            exit(-1)
          end

          split_path.delete_at(split_path.length - 1)
          vmdk_folder = split_path.join("/")

          unless datastore.exists? vmdk_folder
            dc = datacenter
            vmdk_dir = "[#{datastore.name}] #{vmdk_folder}"
            begin
              dc._connection.serviceContent.fileManager.MakeDirectory name: vmdk_dir, datacenter: dc, createParentDirectories: true
            rescue RbVmomi::Fault => e
              puts "Error when creating directory #{vmdk_dir}."

              exit (-1)
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
          vm = get_vm_by_uuid vim, machine

          return if vm.nil?

          datacenter = get_datacenter vim, machine
          vmdk_datastore = get_datastore datacenter, machine

          disks.each do |disk|
            create_disk = disk['create']
            path = disk['path']

            verify_vmdir_exists vmdk_datastore, datacenter, path

            puts "Choosing: #{vmdk_datastore.name}"

            virtualDisk = find_virtual_disk_in_datastore vmdk_datastore, path
            vmdk_full_name = "[#{vmdk_datastore.name}] #{path}"

            if create_disk == true
              size = disk['size']
              vmdk_type = disk['type']
              vmdk_size_kb = size.to_i * 1024

              if !virtualDisk.nil?
                puts "Virtual disk #{path} already created - using this one."
              else
                create_new_disk_in_datastore vmdk_datastore, vim.serviceContent.virtualDiskManager, path, vmdk_size_kb, vmdk_type, datacenter
              end
            else
              if virtualDisk.nil?
                puts "Couldn't find virtual disk specified at #{path} in datastore [#{vmdk_datastore.name}]. Exiting..."
                exit (-1)
              end

              vmdk_size_kb = virtualDisk.capacityKb
            end

            scsi_tree = find_scsi_controller_tree vm
            ctrl = find_scsi_controller vm, scsi_tree
            new_unit_number = find_new_unit_number scsi_tree, ctrl

            begin
              attach_virtual_disk_to_vm vm, vmdk_datastore, vmdk_full_name, path, vmdk_size_kb, ctrl, new_unit_number
            rescue RbVmomi::Fault => e
              puts "Error when attaching disk #{path}: #{e}."

              exit (-1)
            end
          end
        end
      end
    end
  end
end