set -x

cd ~/temp
controller_ip=$1
for i in 100 1000 2000 5000 8000 10000 15000 20000
 do 
	for j in 1 5 10 50 100 1000 5000 10000 20000 25000 30000 40000 50000 100000
	do 
		echo "packetsize $i packets $j"
./integrationtests/bin/integrationtests -controller http://$controller_ip:9090 -producers 1 -eventspersec $j -runtime 1 -size $i -stream w -blocking false -zipkin false | tee   {$j}_events_{$i}_packetsize.txt
	 done
 done

cd -
