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

        def find_virtual_disk_in_datastore(datastore, path)

          if path.nil?
            return nil
          end

          split_path = path.split(/\//)

          if split_path.empty? || split_path.length < 2
            puts "Incorrect path format. Expected format: <path_to_folder>/vmdk_name"
            exit(-4)
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

          puts "Returning existing disk"
          return files[0]
        end

        def is_disk_attached (datastore, vmdk_path)
          puts "Checking if disk attached"

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

        def get_next_available_number (datastore, vmname)
          # now we need to inspect the files in this datastore to get our next file name
          next_vmdk = 1
          pc = datastore._connection.serviceContent.propertyCollector
          vms = datastore.vm

          vm_files = pc.collectMultiple vms, 'layoutEx.file'
          vm_files.keys.each do |vmFile|
            vm_files[vmFile]['layoutEx.file'].each do |layout|
              if layout.name.match(/^\[#{datastore.name}\] #{vmname}\/#{vmname}_([0-9]+).vmdk/)
                num = Regexp.last_match(1)
                next_vmdk = num.to_i + 1 if next_vmdk <= num.to_i
              end
            end
          end

          next_vmdk
        end

        def create_new_disk_in_datastore(datastore, vdm, path, vmdk_size_kb, vmdk_type, datacenter)

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

          return vmdk_full_name
        end

        def find_scsi_controller_and_unit_number (vm)
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

          ctrl = find_device(vm, use_controller)

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

          # ensure we don't try to add the controllers SCSI ID
          new_unit_number = available_unit_numbers.sort[0]

          return {
              "controller" => ctrl,
              "unit_number" => new_unit_number
          }
        end

        def attach_virtual_disk_to_vm (vm, datastore, vmdk_full_name, vmdk_path, vmdk_size_kb)
          disk_attched_to_vm = is_disk_attached datastore, vmdk_path

          if disk_attched_to_vm == true
            puts "Trying to attach disk '#{vmdk_full_name}' but it is already attached to this VM. Exiting..."
            exit(-2)
          end

          newDiskControllerInfo = find_scsi_controller_and_unit_number vm

          puts "info: #{newDiskControllerInfo}"

          vmdk_backing = RbVmomi::VIM::VirtualDiskFlatVer2BackingInfo(
              datastore: datastore,
              diskMode: 'persistent',
              fileName: vmdk_full_name
          )

          device = RbVmomi::VIM::VirtualDisk(
              backing: vmdk_backing,
              capacityInKB: vmdk_size_kb,
              controllerKey: newDiskControllerInfo["controller"].key,
              key: -1,
              unitNumber: newDiskControllerInfo["unit_number"]
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

          disks.each do |disk|
            create_disk = disk['create']
            data_store_name = disk['data_store_name']

            if data_store_name.nil?
              vmdk_datastore = get_datastore datacenter, machine
            else
              vmdk_datastore = get_datastore_by_name datacenter, data_store_name
            end

            puts "Choosing: #{vmdk_datastore.name}"

            if create_disk == true
              path = disk['path']
              size = disk['size']
              vmdk_type = disk['type']

              virtualDisk = find_virtual_disk_in_datastore vmdk_datastore, path

              if !virtualDisk.nil?
                puts "Trying to create a disk #{path}, but it already exists in datatore: #{vmdk_datastore.name}. Exiting..."
                exit (-6)
              end

              vmname = vm.summary.config.name
              next_vmdk = get_next_available_number vmdk_datastore, vmname

              if path.nil?
                path = "#{vmname}/#{vmname}_#{next_vmdk}.vmdk"
              end

              vmdk_size_kb = size.to_i * 1024
              # TODO - thick, preallocated?
              vmdk_type = 'preallocated' if vmdk_type == 'thick'
              vmdk_full_name = create_new_disk_in_datastore vmdk_datastore, vim.serviceContent.virtualDiskManager, path, vmdk_size_kb, vmdk_type, datacenter
            else
              path = disk['path']

              virtualDisk = find_virtual_disk_in_datastore vmdk_datastore, path

              if virtualDisk.nil?
                puts "Couldn't find virtual disk specified at #{path} in datastore [#{vmdk_datastore.name}]. Exiting..."
                exit (-3)
              end

              vmdk_full_name = "[#{vmdk_datastore.name}] #{path}"
              vmdk_size_kb = virtualDisk.capacityKb
            end

            attach_virtual_disk_to_vm vm, vmdk_datastore, vmdk_full_name, path, vmdk_size_kb
          end
        end
      end
    end
  end
end