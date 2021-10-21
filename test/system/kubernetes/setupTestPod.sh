#!/usr/bin/env bash
#
# Copyright Pravega Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

set -euxo pipefail

# Constants
minKubectlVersion="v1.13.0"
minHelmVersion="v3.1.2"
skipServiceInstallation=${skipServiceInstallation:-false}

if [ $skipServiceInstallation = false ]; then

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

  # Step 2: Verify if helm is present.
  if [ $(program_is_installed helm) == 0 ]; then
    echo "helm is not installed, please ensure helm with version >=v3.2.1"
    exit 1
  else
    echo "helm is installed."
  fi

  # Step 3: Verify the version of kubectl present. Minimum required version is v1.13.0 as this supports the wait command.
  kubectlVersion="$(kubectl version --client=true --short=true | awk '{print $3}')"
  echo "Version of installed kubectl: $kubectlVersion"

  if [ "$(printf '%s\n' "$minKubectlVersion" "$kubectlVersion" | sort -V | head -n1)" = "$minKubectlVersion" ]; then
      echo "Valid version of kubectl present."
  else
      echo "Older version of kubectl present, please upgrade"
      exit 1
  fi

  # Step 4: Verify the version of helm present. Minimum required version is v3.2.1 as this is needed to deploy charts.
  helmVersion="$(helm version | cut -f2 -d "\"")"
  echo "Version of installed helm: $helmVersion"

  if [ "$(printf '%s\n' "$minHelmVersion" "$helmVersion=" | sort -V | head -n1)" = "$minHelmVersion" ]; then
      echo "Valid version of helm present."
  else
      echo "Older version of helm present, please upgrade"
      exit 1
  fi

  # Step 5: Verify if kubectl is able to talk to the Kubernetes cluster.
  echo "Logging the details of Kubernetes cluster"
  kubectl cluster-info  # any error here will cause the script to terminate.

  #Step  : Adding Charts repo to helm
  echo "Adding Charts repo to helm"
  helm repo add $publishedChartName $helmRepository
  helm repo update

  #Step 6: Creating ZK-OP
  echo "Creating ZK Operator"
  echo "helm install zkop $publishedChartName/zookeeper-operator --version=$zookeeperOperatorChartVersion --set image.repository=$dockerRegistryUrl/$imagePrefix/$zookeeperOperatorImageName --set image.tag=$zookeeperOperatorVersion --set hooks.image.repository=$helmHookImageName"
  helm install zkop $publishedChartName/zookeeper-operator --version=$zookeeperOperatorChartVersion --set image.repository=$dockerRegistryUrl/$imagePrefix/$zookeeperOperatorImageName --set image.tag=$zookeeperOperatorVersion --set hooks.image.repository=$helmHookImageName
  zkOpName="$(kubectl get pod | grep "zookeeper-operator" | awk '{print $1}')"
  #kubectl wait --timeout=1m --for=condition=Ready pod/$zkOpName
  readyValueZk="$(kubectl get deploy | awk '$1 == "zkop-zookeeper-operator" { print $2 }')"
  if [ "$readyValueZk" != "1/1" ];then
          echo "Zookeeper operator is not running. Please check"
          exit 1
    else
      echo "Zookeeper operator is running $readyValueZk"
    fi

  #Step 7: Creating BK-OP
  echo "Creating BK Operator"
  echo "helm install bkop $publishedChartName/bookkeeper-operator --version=$bookkeeperOperatorChartVersion --set testmode.enabled=true --set image.repository=$dockerRegistryUrl/$imagePrefix/$bookkeeperOperatorImageName --set image.tag=$bookkeeperOperatorVersion --set hooks.image.repository=$helmHookImageName"
  helm install bkop $publishedChartName/bookkeeper-operator --version=$bookkeeperOperatorChartVersion --set testmode.enabled=true --set image.repository=$dockerRegistryUrl/$imagePrefix/$bookkeeperOperatorImageName --set image.tag=$bookkeeperOperatorVersion --set hooks.image.repository=$helmHookImageName --wait

  bkOpName="$(kubectl get pod | grep "bookkeeper-operator" | awk '{print $1}')"
  #kubectl wait --timeout=1m --for=condition=Ready pod/$bkOpName
  readyValueBk="$(kubectl get deploy | awk '$1 == "bkop-bookkeeper-operator" { print $2 }')"
  if [ "$readyValueBk" != "1/1" ];then
          echo "Bookkeeper operator is not running. Please check"
          exit 1
    else
      echo "Bookkeeper operator is running $readyValueBk"
    fi

  #Step 8: Creating Pravega-OP
  echo "Creating Pravega Operator"
  CERT="$(kubectl get secret selfsigned-cert-tls -o yaml | grep tls.crt | head -1 | awk '{print $2}')"
  echo "helm install prop $publishedChartName/pravega-operator  --version=$pravegaOperatorChartVersion --set webhookCert.crt=$CERT --set testmode.enabled=true --set image.repository=$dockerRegistryUrl/$imagePrefix/$pravegaOperatorImageName --set image.tag=$pravegaOperatorVersion --set hooks.image.repository=$helmHookImageName"
  helm install prop $publishedChartName/pravega-operator  --version=$pravegaOperatorChartVersion --set webhookCert.crt=$CERT --set testmode.enabled=true --set image.repository=$dockerRegistryUrl/$imagePrefix/$pravegaOperatorImageName --set image.tag=$pravegaOperatorVersion --set hooks.image.repository=$helmHookImageName --wait
  prOpName="$(kubectl get pod | grep "pravega-operator" | awk '{print $1}')"
  #kubectl wait --timeout=1m --for=condition=Ready pod/$prOpName
  readyValuePr="$(kubectl get deploy | awk '$1 == "prop-pravega-operator" { print $2 }')"
  if [ "$readyValuePr" != "1/1" ];then
          echo "Pravega operator is not running. Please check"
          exit 1
    else
      echo "Pravega operator is running $readyValuePr"
    fi

  # Step 9: Verify if tier2 PVC has been created for NFS.
  if [ $tier2Type = "nfs" ]; then
    tier2Size="$(kubectl get pvc -o jsonpath='{.items[?(@.metadata.name == "pravega-tier2")].status.capacity.storage}')"
    if [ -z "$tier2Size" ];then
          echo "Tier2 PVC pravega-tier2 is not present. Please create it before running the tests."
          exit 1
    else
      echo "Size of Tier2 is $tier2Size"
    fi
  fi

fi

# Step 10: Create a dynamic PVC, if already created the error is ignored.
cat <<EOF | kubectl create -f - || true
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

# Step 11: Create an init pod and wait until pod is running.
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
      image: $testPodImage
      command: ["/bin/sh"]
      args: ["-c", "sleep 60000"]
      volumeMounts:
        - mountPath: "/data"
          name: task-pv-storage
EOF
kubectl wait --timeout=5m --for=condition=Ready pod/task-pv-pod

#Step 12: Compute the checksum of the local test artifact and the artifact on the persistent volume. Copy test artifact only if required.
checksum="$(kubectl exec task-pv-pod md5sum '/data/test-collection.jar' | awk '{ print $1 }' || true)"

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
