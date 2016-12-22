/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.emc.pravega.local;

import com.twitter.distributedlog.LocalDLMEmulator;
import org.apache.bookkeeper.util.IOUtils;
import org.apache.commons.io.FileUtils;

import java.io.File;

public class LocalPravegaEmulator {



    public static void main(String[] args) {
        try {
            if (args.length < 3) {
                System.out.println("Usage: LocalPravegaEmulator <zk_port> <controller_port> <host_port>");
                System.exit(-1);
            }

            final int zkPort = Integer.parseInt(args[0]);
            final int controllerPort = Integer.parseInt(args[1]);
            final int hostPort = Integer.parseInt(args[2]);

            final File zkDir = IOUtils.createTempDir("distrlog", "zookeeper");
            final LocalDLMEmulator localDlm = LocalDLMEmulator.newBuilder()
                    .zkPort(zkPort)
                    .numBookies(1)
                    .build();

             final LocalHDFSEmulator localHdfs = LocalHDFSEmulator.newBuilder()
                                                .baseDirName("temp")
                                                .build();

            final LocalPravegaEmulator localPravega = LocalPravegaEmulator.newBuilder()
                    .controllerPort(controllerPort)
                    .hostPort(hostPort)
                    .build();

            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    try {
                        localPravega.teardown();
                        localDlm.teardown();
                        localHdfs.teardown();
                        FileUtils.deleteDirectory(zkDir);
                        System.out.println("ByeBye!");
                    } catch (Exception e) {
                        // do nothing
                    }
                }
            });

            localHdfs.start();
            localDlm.start();
            localPravega.start();

            System.out.println(String.format(
                    "DistributedLog Sandbox is running now. You could access %s",
                    localDlm.getUri()));
        } catch (Exception ex) {
            System.out.println("Exception occurred running emulator " + ex);
        }
    }

    /**
     * Stop controller and host.
     * */
    private void teardown() {
    }

    /**
     * Start controller and host.
     * */
    private void start() {
    }

    private static Builder newBuilder() {
        return new Builder();
    }


    private static class Builder {
        private int controllerPort;
        private int hostPort;

        public Builder controllerPort(int controllerPort) {
            this.controllerPort = controllerPort;
            return this;
        }

        public Builder hostPort(int hostPort) {
            this.hostPort = hostPort;
            return this;
        }

        public LocalPravegaEmulator build() {
            return null;
        }
    }
}
