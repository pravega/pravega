export JAVA_OPTS="$JAVA_OPTS -Ddlog.hostname=controlnode -Dpravegaservice.zkURL=controlnode:2181 -DpravegaService.controllerUri=tcp://controlnode:9090 -Dpravegaservice.listeningIPAddress=`hostname`"
export ZK_URL=controlnode:2181
