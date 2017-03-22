#!/bin/bash
/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */


PRAVEGA_CLASSPATH=clients/streaming/build/libs/streaming.jar

java -cp $PRAVEGA_CLASSPATH com.emc.pravega.console.ConsoleWriter $LINE_OPTIONS 
