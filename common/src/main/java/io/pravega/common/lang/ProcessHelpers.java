package io.pravega.common.lang;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import lombok.val;

/**
 * Helper methods related to java.lang.process.
 */
public final class ProcessHelpers {
    /**
     * Executes the given Class in a new process.
     *
     * @param c    The Class to execute. This class must have a public static main() method.
     * @param args An array of arguments to pass to the Class' main() method. Each of these arguments will be converted
     *             to a String by invoking toString() on them.
     * @return A Process pointing to the new process that was started.
     * @throws IOException If an Exception occurred.
     */
    public static Process exec(Class c, Object... args) throws IOException {
        return exec(c, null, null, args);
    }

    /**
     * Executes the given Class in a new process.
     *
     * @param c        The Class to execute. This class must have a public static main() method.
     * @param envVars  A Map of Environment Variables to set (in addition to the inherited ones).
     * @param sysProps A Map of System Properties to set (in addition to the inherited ones).
     * @param args     An array of arguments to pass to the Class' main() method. Each of these arguments will be converted
     *                 to a String by invoking toString() on them.
     * @return A Process pointing to the new process that was started.
     * @throws IOException If an Exception occurred.
     */
    public static Process exec(Class c, Map<String, String> envVars, Map<String, String> sysProps, Object... args) throws IOException {
        envVars = envVars == null ? Collections.emptyMap() : envVars;
        sysProps = sysProps == null ? Collections.emptyMap() : sysProps;
        String[] params = new String[args.length + sysProps.size() + 4];
        int index = 0;

        // Invoke Java and setup classpath.
        params[index++] = System.getProperty("java.home") + File.separator + "bin" + File.separator + "java";
        params[index++] = "-cp";
        params[index++] = System.getProperty("java.class.path");

        // Set System Properties (if any).
        for (val e : sysProps.entrySet()) {
            params[index++] = String.format("-D%s=%s", e.getKey(), e.getValue());
        }

        // Set class & class arguments.
        params[index++] = c.getCanonicalName();
        for (Object arg : args) {
            params[index++] = arg.toString();
        }

        // Start the process, but not before setting new environment variables.
        ProcessBuilder builder = new ProcessBuilder(params);
        builder.environment().putAll(envVars);
        return builder.start();
    }
}
