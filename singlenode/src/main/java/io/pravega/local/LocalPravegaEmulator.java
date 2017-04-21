/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package io.pravega.local;


import lombok.Builder;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LocalPravegaEmulator implements AutoCloseable {

    private final InProcPravegaCluster inProcPravegaCluster;

    @Builder
    private LocalPravegaEmulator(int zkPort, int controllerPort, int hostPort) {
        inProcPravegaCluster = InProcPravegaCluster
                .builder()
                .isInProcZK(true)
                .zkUrl("localhost:" + zkPort)
                .zkPort(zkPort)
                .isInMemStorage(true)
                .isInProcController(true)
                .controllerCount(1)
                .isInProcSegmentStore(true)
                .segmentStoreCount(1)
                .containerCount(4)
                .build();
        inProcPravegaCluster.setControllerPorts(new int[] {controllerPort});
        inProcPravegaCluster.setSegmentStorePorts(new int[] {hostPort});
    }

    /**
     * Gets an integer argument from the args array, or returns the default value if the argument was not provided.
     *
     * @param args the arguments.
     * @param pos the position of the argument to retrieve.
     * @param defaultValue the default value if the argument was not provided.
     * @return the integer value of the argument, or the default value if the argument was not provided.
     *
     * @throws NumberFormatException if the argument is provided and is not a valid integer.
     */
    private static int intArg(String[] args, int pos, int defaultValue) {
        if (args.length > pos) {
            return Integer.parseInt(args[pos]);
        } else {
            return defaultValue;
        }
    }

    public static void main(String[] args) {
        try {
            final int zkPort = intArg(args, 0, 4000);
            final int controllerPort = intArg(args, 1, 9090);
            final int hostPort = intArg(args, 2, 6000);

            final LocalPravegaEmulator localPravega = LocalPravegaEmulator.builder().controllerPort(
                    controllerPort).hostPort(hostPort).zkPort(zkPort).build();
            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    try {
                        localPravega.close();
                        System.out.println("ByeBye!");
                    } catch (Exception e) {
                        // do nothing
                        log.warn("Exception running local Pravega emulator: " + e.getMessage());
                    }
                }
            });

            localPravega.start();

            System.out.println(
                    String.format("Pravega Sandbox is running locally now. You could access it at %s:%d", "127.0.0.1",
                            controllerPort));
        } catch (Exception ex) {
            System.out.println("Exception occurred running emulator " + ex);
            ex.printStackTrace();
            System.exit(1);
        }
    }

    /**
     * Stop controller and host.
     */
    @Override
    public void close() {
       inProcPravegaCluster.close();
    }

    /**
     * Start controller and host.
     */
    private void start() throws Exception {
        inProcPravegaCluster.start();
    }

}
