/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.local;

import com.twitter.distributedlog.LocalDLMEmulator;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;

@Slf4j
@Builder
public class InProcPravegaCluster {
    /*Controller related variables*/
    private boolean isInprocController;
    private int controllerCount;
    private int controllerPorts[];
    private String controllerURI;

    /*Host related variables*/
    private boolean isInprocHost;
    private int hostCount;
    private int hostPorts[];

    /*Distributed log related variables*/
    private boolean isInProcDL;
    private int bookieCount;
    private int initialBookiePort;


    /*ZK related variables*/
    private boolean isInProcZK;
    private int zkPort;
    private String zkHost;

    /*HDFS related variables*/
    private boolean isInProcHDFS;
    private String hdfsUrl;

     /* TBD: Add more config variables like:
     1. container count
     etc
     */

    private LocalHDFSEmulator localHdfs;
    private LocalDLMEmulator localDlm;

    public void start() throws Exception {
        /*Start the ZK*/
        if(isInProcZK) {
            zkHost = "localhost";
        } else {
            assert(zkHost!=null);
        }

        if(isInProcHDFS) {
            startLocalHDFS();
            hdfsUrl = String.format("hdfs://localhost:%d/", localHdfs.getNameNodePort());
        } else {
            assert(hdfsUrl!=null);
        }

        if(isInProcDL) {
            startLocalDL();
        }
        if(isInprocHost) {
            startLocalHosts();
        }

        if(isInprocController) {
            startLocalControllers();
        } else {
            assert(controllerURI!= null);
        }

    }

    private void startLocalDL() throws Exception {
        if(isInProcZK) {
            localDlm = LocalDLMEmulator.newBuilder().shouldStartZK(true).
                    zkPort(zkPort).numBookies(this.bookieCount).build();
        } else {
            localDlm = LocalDLMEmulator.newBuilder().shouldStartZK(false).zkHost(zkHost).
                    zkPort(zkPort).numBookies(this.bookieCount).build();
        }
        localDlm.start();
    }


    private void startLocalHDFS() throws IOException {
        localHdfs = LocalHDFSEmulator.newBuilder().baseDirName("temp").build();
        localHdfs.start();
    }

    private void startLocalHosts() {


    }

    private void startLocalControllers() {

    }

}
