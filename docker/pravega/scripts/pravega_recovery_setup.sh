IFS='
'

is_recovery_pod_installed() {
  recovery_pod=$(kubectl get pod | grep recovery)
  for line in $recovery_pod; do
    str="$(cut -d' ' -f1 <<<"$line")";
    if [ $str == $1 ]
    then
      return 1;
    fi
  done
  return 0
}

is_installed() {
  helm_list=$(helm list)
  for line in $helm_list; do
    str="$(cut -d' ' -f1 <<<"$line")";
    if [ $str == $1 ]
    then
      return 1;
    fi
  done
  return 0
}

generate_recovery_yaml() {
  echo "Generating yaml for recovery pod"
  output=$(kubectl get deployment -o wide | grep pravega-pravega-controller | tr -s ' ')
  image="$(cut -d' ' -f7 <<<"$output")"
  echo $image
  cat << FILE > recovery.yaml
apiVersion: v1
kind: Pod
metadata:
  labels:
    run: recovery
  name: recovery
  namespace: default
spec:
  containers:
  - command: [ "sh", "-c", "tail -f /dev/null" ]
    image: $image
    imagePullPolicy: IfNotPresent
    name: testss
    volumeMounts:
    - mountPath: /mnt/tier2
      name: tier2
  volumes:
    - name: tier2
      persistentVolumeClaim:
        claimName: pravega-tier2
FILE
}

remove_services() {
  pod_array=("pravega" "pravega-operator" "bookkeeper" "bookkeeper-operator" "zookeeper" "zookeeper-operator")
  for pod in ${pod_array[@]}; do
    is_installed $pod
    installed=$?
    retry=0
    
    if [ $installed -eq 0 ]
    then
      echo $pod "is not installed"
    fi

    while [ $installed -eq 1 ] && [ $retry -lt 4 ];
    do
      if [ $retry -eq 3 ]
      then
        echo "Unable to uninstall "$pod
        exit 1
      fi
      echo "Uninstalling " $pod
      helm uninstall $pod
      retry=$((retry + 1))
      is_installed $pod
      installed=$?
    done
  done
}

if [ $1 == "--install" ]
then
    is_installed "pravega"
    installed=$?
    is_recovery_pod_installed "recovery"
    is_recovery_installed=$?
    if [ $installed -eq 1 ]
    then
      generate_recovery_yaml
      kubectl apply -f recovery.yaml
      rm recovery.yaml
    elif [ $is_recovery_installed -eq 0 ]
    then
        echo "Can not create recovery pod. Pravega not installed"
        exit 1
    fi
    remove_services
    
elif [ $1 == "--remove" ]
then
    kubectl delete pod recovery
elif [ $1 == "--help" ]
then
    echo "--install : If pravega is deployed then it installs the recovery pods and destroys all services of pravega except tier2."
    echo "--remove  : Deletes the recovery pod."
elif [[ $1 == --* ]]
then
    echo "recovery: invalid option"
    echo "Try 'recovery --help' for more information."
fi