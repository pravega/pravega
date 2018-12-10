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

# Constants
minKubectlVersion="v1.13.0"

# Helper functions.
function program_is_installed {
  # set to 1 initially
  local return_=1
  # set to 0 if not found
  type $1 >/dev/null 2>&1 || { local return_=0; }
  # return value
  echo "$return_"
}

# Step 1: verify if kubectl is present.
if [ $(program_is_installed kubectl) == 0 ]; then
  echo "kubectl is not installed, please ensure kubectl with version >=v1.13.0"
  exit 1
else
  echo "kubectl is installed."
fi

# Step 2: Verify the version of kubectl present. Minimum required version is v1.13.0 as this supports the wait command.
kubectlVersion="$(kubectl version --client=true --short=true | awk '{print $3}')"
echo "Version of installed kubectl: $kubectlVersion"

if [ "$(printf '%s\n' "$minKubectlVersion" "$kubectlVersion" | sort -V | head -n1)" = "$minKubectlVersion" ]; then
    echo "Valid version of kubectl present."
else
    echo "Older version of kubectl present, please upgrade"
    exit 1
fi

# Step 3: Verify if kubectl is able to talk to the Kubernetes cluster.
echo "Logging the details of Kubernetes cluster"
kubectl cluster-info

if [ $? -ne 0 ]; then
   echo "Kubectl is not configured correctly, please configure kubectl before starting tests."
   exit 1
fi

# Step 4: Create a dynamic PVC, if already created the error is ignored.
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

# Step 5: Create an init pod and wait until pod is running.
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

#Step 6: Compute the checksum of the local test artifact and the artifact on the persistent volume. Copy test artifact only if required.
checksum="$(kubectl exec task-pv-pod md5sum '/data/test-collection.jar' | awk '{ print $1 }')"
echo "Checksum of test artifact on the pod $checksum"

expectedCheckSum="$(md5sum './build/libs/test-collection.jar' | awk '{ print $1 }' )"
echo "Checksum of the local test artifact $expectedCheckSum"

if [ "$checksum" == "$expectedCheckSum" ]; then
  echo "Checksum match, no need to copy the test jar to Kubernetes cluster"
else
  echo "Copying test artifact to cluster, (this will take a couple of minutes)..."
  copyStatus="$(kubectl cp ./build/libs/test-collection.jar task-pv-pod:/data)"
fi

#delete the pod that was created.
echo "Deleting pod task-pv-pod that was used to copy the test artifacts"
kubectl delete po task-pv-pod --now

exit $copyStatus
