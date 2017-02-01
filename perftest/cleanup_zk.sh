cat << EOF | ./distributedlog-service/bin/dlog zkshell 10.249.250.151
deleteall /cluster
deleteall /messaging
deleteall /streams
ls /
EOF

