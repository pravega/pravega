
set -x

cd ~/temp
controller_ip=$1
./integrationtests/bin/integrationtests -controller http://$controller_ip:9090 -producers 1 -eventspersec 100000 -runtime 1 -size 100 -stream aae -blocking false -zipkin false >  100000_events_size_100_allatonce_raw_latencies.txt
./integrationtests/bin/integrationtests -controller http://$controller_ip:9090 -producers 1 -eventspersec 100000 -runtime 1 -size 1000 -stream aae -blocking false -zipkin false >  100000_events_size_1k_allatonce_raw_latencies.txt
./integrationtests/bin/integrationtests -controller http://$controller_ip:9090 -producers 1 -eventspersec 100000 -runtime 1 -size 10000 -stream aae -blocking false -zipkin false >  100000_events_size_10k_allatonce_raw_latencies.txt
cd -
