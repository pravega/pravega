# -*- mode: ruby -*-
# vi: set ft=ruby :

Vagrant.configure("2") do |config|
  config.vm.box = "ubuntu/trusty64"
  config.vm.network "private_network", ip: "192.168.33.10"

  config.vm.synced_folder "build/install", "/opt/pravega"
  config.vm.synced_folder ".", "/code"

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
  config.vm.provision "shell", inline: <<-SHELL
    apt-get -y update
    apt-get install -y software-properties-common python-software-properties
    add-apt-repository -y ppa:webupd8team/java
    apt-get -y update
  
    mkdir -p /var/cache/oracle-jdk7-installer
    if [ -e "/tmp/java-cache/" ]; then
        find /tmp/java-cache/ -not -empty -exec cp '{}' /var/cache/oracle-jdk7-installer/ \;
    fi 
      
    /bin/echo debconf shared/accepted-oracle-license-v1-1 select true | /usr/bin/debconf-set-selections
    apt-get -y install oracle-java7-installer oracle-java7-set-default
      
    if [ -e "/tmp/java-cache/" ]; then 
        cp -R /var/cache/oracle-jdk7-installer/* /tmp/java-cache/
    fi
    /opt/pravega/pravega-installer/scripts/start_namenode.sh 
   SHELL
end
