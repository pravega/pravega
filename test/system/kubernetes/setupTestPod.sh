#!/usr/bin/env bash
#
# Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#

function program_is_installed {
  # set to 1 initially
  local return_=1
  # set to 0 if not found
  type $1 >/dev/null 2>&1 || { local return_=0; }
  # return value
  echo "$return_"
}

# verify if kubectl is present.
if [ $(program_is_installed kubectl) == 0 ]; then
  echo "kubectl is not present"
  exit 1
else
  echo "kubectl is present"
fi

echo "Logging the details of Kubernetes cluster"
kubectl cluster-info

if [ $? -ne 0 ]; then
   echo "Kubectl is not configured correctly, please configure kubectl before starting tests."
   exit 1
fi

#create a dynamic PVC
cat <<EOF | kubectl create -f -
kind: PersistentVolumeClaim
apiVersion: v1
metadata:
  name: task-pv-claim
spec:
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: 1Gi
EOF

#create a pod.
cat <<EOF | kubectl create -f -
kind: Pod
apiVersion: v1
metadata:
  name: task-pv-pod
spec:
  volumes:
    - name: task-pv-storage
      persistentVolumeClaim:
       claimName: task-pv-claim
  containers:
    - name: task-pv-container
      image: openjdk:8-jre-alpine
      command: ["/bin/sh"]
      args: ["-c", "sleep 60000"]
      volumeMounts:
        - mountPath: "/data"
          name: task-pv-storage
EOF
kubectl wait --for=condition=Ready pod/task-pv-pod

checksum="$(kubectl exec task-pv-pod md5sum '/data/test-collection.jar' | awk '{ print $1 }')"
echo "Checksum of test artifact on the pod $checksum"

expectedCheckSum="$(md5sum './build/libs/test-collection.jar' | awk '{ print $1 }' )"
echo "Checksum of the local test artifact $expectedCheckSum"

if [ "$checksum" == "$expectedCheckSum" ]; then
  echo "Checksum match, no need to copy the test jar to Kubernetes cluster"
else
  echo "Copying test artifact to cluster, (this will take a couple of minutes)..."
  kubectl cp ./build/libs/test-collection.jar task-pv-pod:/data
fi

#delete the pod that was created.
echo "Deleting pod task-pv-pod that was used to copy the test artifacts"
kubectl delete po task-pv-pod --now
