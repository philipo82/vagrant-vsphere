require 'rbvmomi'
require 'vSphere/util/vim_helpers'

module VagrantPlugins
  module VSphere
    module Action
      class RemoveVMDK
        include Util::VimHelpers

        def initialize(app, _env)
          @app = app
        end

        # Finds the VMDK attached to the VM by the disk file name,
        # which is the path to the disk in the datastore
        def find_disk_by_file_name(vm, diskFileName)
          vm.disks.each do |disk|
            if !disk.backing.fileName.nil? && disk.backing.fileName == diskFileName
              return disk
            end
          end
        end

        def call(env)
          machine = env[:machine]

          if machine.state.id == :not_created
            puts 'VM is not created. Exiting...'
            return
          end

          vim = env[:vSphere_connection]
          vm = get_vm_by_uuid vim, machine

          if vm.nil?
            puts 'VM was not found. Exiting...'
            return
          end

          attached_disks_names = nil

          # Get all attached disks' names
          if !vm.config.nil? && !vm.config.extraConfig.nil?
            vm.config.extraConfig.each do |configOption|
              if configOption.key == "AttachedDisks"
                attached_disks_names = configOption.value
                break
              end
            end
          end

          unless attached_disks_names.nil?
            attached_disks_names = attached_disks_names.split(',')

            attached_disks_names.each do |attachedDiskName|
              attached_disk = find_disk_by_file_name vm, attachedDiskName

              if attached_disk.nil?
                puts "WARN: Attached disk with name #{attachedDiskName} was not found!"
                next
              end

              device_config_spec = RbVmomi::VIM::VirtualDeviceConfigSpec(
                device: attached_disk,
                operation: RbVmomi::VIM::VirtualDeviceConfigSpecOperation('remove')
              )

              vm_config_spec = RbVmomi::VIM::VirtualMachineConfigSpec(
                deviceChange: [device_config_spec]
              )

              vm.ReconfigVM_Task(spec: vm_config_spec).wait_for_completion
            end
          end

          @app.call env
        end
      end
    end
  end
end
