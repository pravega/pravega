set -x

cd ~/temp
controller_ip=$1

echo "producing to single segment"

        for j in 20000 50000 100000 200000 300000 500000 1000000 2000000 3000000 4000000 5000000 6000000 7000000 8000000 10000000 11000000 15000000 18000000 20000000 22000000 25000000 27000000 30000000 35000000 40000000 45000000 50000000 100000000 200000000 300000000 400000000 500000000 1000000000 5000000000
 do
        for i in 10000 
        do
                packets=$(( $j/$i))
                echo "packetsize $i packets $packets"
                ./integrationtests/bin/integrationtests -controller http://$controller_ip:9090 -producers 1 -eventspersec $packets -runtime 1 -size $i -stream ww -blocking false -zipkin false -reporting 200 | tee   single_segment_{$j}_thoroughput_{$i}_eventsize.txt
         done
 done




echo "producing to multiple segment (20) in multiple containers (4)"

      
        for j in 20000 50000 100000 200000 300000 500000 1000000 2000000 3000000 4000000 5000000 6000000 7000000 8000000 10000000 11000000 15000000 18000000 20000000 22000000 25000000 27000000 30000000 
 do
        for i in 10000 
        do
                packets=$(( $j/$i))
                echo "packetsize $i packets $packets"
                ./integrationtests/bin/integrationtests -controller http://$controller_ip:9090 -producers 20 -eventspersec $packets -runtime 1 -size $i -stream ww -blocking false -zipkin false -reporting 200 | tee   20_segment_{$j}_thoroughput_{$i}_eventsize.txt
         done
 done


# TODO: change config to 1 container and create the deployment again

echo "producing to multiple segment (20) in single container 


        for j in 20000 50000 100000 200000 300000 500000 1000000 2000000 3000000 4000000 5000000 6000000 7000000 8000000 10000000 11000000 15000000 18000000 20000000 22000000 25000000 27000000 30000000
 do
        for i in 10000
        do
                packets=$(( $j/$i))
                echo "packetsize $i packets $packets"
                ./integrationtests/bin/integrationtests -controller http://$controller_ip:9090 -producers 20 -eventspersec $packets -runtime 1 -size $i -stream ww -blocking false -zipkin false -reporting 200 | tee   20_segment_1_container_{$j}_thoroughput_{$i}_eventsize.txt
         done
 done




cd -

