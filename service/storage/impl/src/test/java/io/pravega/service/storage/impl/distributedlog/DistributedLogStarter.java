/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package io.pravega.service.storage.impl.distributedlog;

import ch.qos.logback.classic.LoggerContext;
import com.twitter.distributedlog.LocalDLMEmulator;
import com.twitter.distributedlog.admin.DistributedLogAdmin;
import com.twitter.distributedlog.tools.Tool;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.util.ReflectionUtils;
import org.junit.Assert;
import org.slf4j.LoggerFactory;

/**
 * Helper class that starts DistributedLog in-process and creates a namespace.
 */
@Slf4j
public final class DistributedLogStarter {
    public static final String DLOG_HOST = "127.0.0.1";

    public static void main(String[] args) throws Exception {
        // We don't need logging here at all.
        LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
        context.reset();

        final String host = args[0];
        final int port = Integer.parseInt(args[1]);
        log.info("Starting LocalDLMEmulator with host='{}', port={}.", host, port);

        // Start DistributedLog in-process.
        ServerConfiguration sc = new ServerConfiguration()
                .setJournalAdaptiveGroupWrites(false)
                .setJournalMaxGroupWaitMSec(0);
        val dlm = LocalDLMEmulator.newBuilder()
                                  .zkPort(port)
                                  .serverConf(sc)
                                  .build();
        dlm.start();

        // Wait forever. This process will be killed externally.
        Thread.sleep(Long.MAX_VALUE);
    }

    /**
     * Starts DistributedLog out of process.
     *
     * @param port The port that DistributedLog is running on.
     * @return A Process describing the remote process.
     * @throws IOException If an Exception occurred.
     */
    public static Process startOutOfProcess(int port) throws IOException {
        // Pick a random port to reduce chances of collisions during concurrent test executions.
        String classPath = getClassPath();
        val pb = new ProcessBuilder(
                "java",
                "-cp", String.join(":", classPath),
                DistributedLogStarter.class.getCanonicalName(),
                DLOG_HOST,
                Integer.toString(port));
        pb.inheritIO(); // For some reason we need to inherit IO, otherwise this process is not responsive and we can't use it.
        return pb.start();
    }

    /**
     * Creates the given namespace in a DistributedLog instance running at the given port.
     *
     * @param namespace The namespace to create.
     * @param port      The port where DistributedLog is running at.
     * @throws Exception If an Exception occurred.
     */
    public static void createNamespace(String namespace, int port) throws Exception {
        Tool tool = ReflectionUtils.newInstance(DistributedLogAdmin.class.getName(), Tool.class);
        tool.run(new String[]{
                "bind",
                "-l", "/ledgers",
                "-s", String.format("%s:%s", DLOG_HOST, port),
                "-c", String.format("distributedlog://%s:%s/%s", DLOG_HOST, port, namespace) });
    }

    /**
     * Gets the current class path and updates the path to Guava to point to version 16.0 of it.
     */
    private static String getClassPath() {
        String[] classPath = System.getProperty("java.class.path").split(":");
        String guava16Path = getGuava16PathFromSystemProperties();
        if (guava16Path == null) {
            guava16Path = inferGuava16PathFromClassPath(classPath);
        }

        Assert.assertTrue("Unable to determine Guava 16 path.", guava16Path != null && guava16Path.length() > 0);
        for (int i = 0; i < classPath.length; i++) {
            if (classPath[i].contains("guava")) {
                classPath[i] = guava16Path;
            }
        }
        return String.join(":", classPath);
    }

    private static String getGuava16PathFromSystemProperties() {
        String guava16Path = System.getProperty("user.guava16");
        if (guava16Path != null && guava16Path.length() > 2 && guava16Path.startsWith("[") && guava16Path.endsWith("]")) {
            guava16Path = guava16Path.substring(1, guava16Path.length() - 1);
        }

        return guava16Path;
    }

    @SneakyThrows(IOException.class)
    private static String inferGuava16PathFromClassPath(String[] classPath) {
        // Example path: /home/username/.gradle/caches/modules-2/files-2.1/com.google.guava/guava/16.0/aca09d2e5e8416bf91550e72281958e35460be52/guava-16.0.jar
        final String searchString = "com.google.guava/guava";
        final Path jarPath = Paths.get("guava-16.0.jar");

        for (String path : classPath) {
            if (path.contains("guava")) {
                int dirNameStartPos = path.indexOf(searchString);
                if (dirNameStartPos >= 0) {
                    String dirName = path.substring(0, dirNameStartPos + searchString.length());
                    Path f = Files.find(Paths.get(dirName), 3, (p, a) -> p.endsWith(jarPath))
                                  .findFirst().orElse(null);
                    if (f != null) {
                        return f.toString();
                    }
                }
            }
        }

        return null;
    }
}
