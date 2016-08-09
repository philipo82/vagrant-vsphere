require 'rbvmomi'

module VagrantPlugins
  module VSphere
    module Action
      class ProvisionVSphere

        def initialize(app, _env)
          @app = app
        end

        def call(env)
          @app.call env
        end

        private

        def provision_vsphere()
	    puts 'running provisioneerrrrr!'
        end
      end
    end
  end
end