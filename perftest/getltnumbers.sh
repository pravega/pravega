set -x

cd ~/temp
controller_ip=$1
	for j in 20000 50000 100000 200000 300000 500000 1000000 2000000 3000000 4000000 5000000 6000000 7000000 8000000 10000000 11000000 15000000 18000000 20000000 22000000 25000000 27000000 30000000 35000000 40000000 45000000 50000000 100000000 200000000 300000000 400000000 500000000 1000000000 5000000000
 do 
	for i in 1000 5000 8000 10000 15000 20000
	do 
		packets=$(( $j/$i))
		echo "packetsize $i packets $packets"
		./integrationtests/bin/integrationtests -controller http://$controller_ip:9090 -producers 4 -eventspersec $packets -runtime 1 -size $i -stream ww -blocking false -zipkin false -reporting 200 | tee   {$j}_thoroughput_{$i}_eventsize.txt
	 done
 done

cd -
