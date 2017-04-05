# -*- mode: ruby -*-
# vi: set ft=ruby :

Vagrant.configure("2") do |config|

  config.vm.box = "ubuntu/trusty64"

  config.vm.synced_folder "build/install", "/opt/pravega"
  config.vm.synced_folder ".", "/code"

  config.hostmanager.enabled = true
  config.hostmanager.manage_host = true
  config.hostmanager.include_offline = false

  # Provider-specific configuration so you can fine-tune various
  # backing providers for Vagrant. These expose provider-specific options.
  # Example for VirtualBox:
  #
 config.vm.provider "virtualbox" do |vb, override|
   # Display the VirtualBox GUI when booting the machine
   #   vb.gui = true

   # Customize the amount of memory on the VM:
   vb.memory = "1024"
   if Vagrant.has_plugin?("vagrant-cachier")
      override.cache.scope = :box
      config.cache.enable :apt
      config.cache.enable :apt_lists
      # Besides the defaults, we use a custom cache to handle the Oracle JDK
      # download, which downloads via wget during an apt install. Because of the
      # way the installer ends up using its cache directory, we need to jump
      # through some hoops instead of just specifying a cache directly -- we
      # share to a temporary location and the provisioning scripts symlink data
      # to the right location.
      override.cache.enable :generic, {
        "java" => { cache_dir: "/tmp/java-cache" },
      }
    end
 end


  #
  # View the documentation for the provider you are using for more
  # information on available options.

  # Define a Vagrant Push strategy for pushing to Atlas. Other push strategies
  # such as FTP and Heroku are also available. See the documentation at
  # https://docs.vagrantup.com/v2/push/atlas.html for more information.
  # config.push.define "atlas" do |push|
  #   push.app = "YOUR_ATLAS_USERNAME/YOUR_APPLICATION_NAME"
  # end

  # Enable provisioning with a shell script. Additional provisioners such as
  # Puppet, Chef, Ansible, Salt, and Docker are also available. Please see the
  # documentation for more information about their specific syntax and use.

  config.vm.define "controlnode" do |controlnode|
	controlnode.vm.hostname = "controlnode"
	controlnode.vm.provider :virtualbox do |vb,override|
         override.vm.network :private_network, ip: "192.168.10.10"
        end
	controlnode.vm.provision "shell", path:"vagrant/scripts/common.sh"
	controlnode.vm.provision "shell", path:"vagrant/scripts/start_first_machine.sh"
  end
  config.vm.define "datanode" do |datanode|
        datanode.vm.hostname = "datanode"
        datanode.vm.provider :virtualbox do |vb,override|
         override.vm.network :private_network, ip: "192.168.10.20"
        end

        datanode.vm.provision "shell", path:"vagrant/scripts/common.sh"
        datanode.vm.provision "shell", path:"vagrant/scripts/start_other_machine.sh"
  end
end
