/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.common.lang;

import io.pravega.common.Exceptions;
import java.io.File;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.stream.Collectors;
import lombok.val;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for the ProcessStarter class.
 */
public class ProcessStarterTests {
    private static final String PREFIX_OUT = "out";
    private static final String PREFIX_ERR = "err";
    private static final Object[] ARGS = new Object[]{ 1, true, "hello" };
    private static final Map.Entry<String, String> SYS_PROP = new AbstractMap.SimpleImmutableEntry<>("sp", "spv");
    private static final Map.Entry<String, String> ENV_VAR = new AbstractMap.SimpleImmutableEntry<>("ev", "evvv");

    private File stdOutFile;
    private File stdErrFile;

    @Before
    public void setUp() throws IOException {
        this.stdOutFile = File.createTempFile("processStarter", "stdout");
        this.stdErrFile = File.createTempFile("processStarter", "stderr");
    }

    @After
    public void tearDown() {
        if (this.stdOutFile != null) {
            this.stdOutFile.delete();
            this.stdOutFile = null;
        }

        if (this.stdErrFile != null) {
            this.stdErrFile.delete();
            this.stdErrFile = null;
        }
    }

    /**
     * Tests the ProcessBuilder functionality.
     */
    @Test
    public void testStart() throws Exception {
        val p = ProcessStarter
                .forClass(ProcessStarterTests.class)
                .args(ARGS)
                .sysProp(SYS_PROP.getKey(), SYS_PROP.getValue())
                .env(ENV_VAR.getKey(), ENV_VAR.getValue())
                .stdOut(ProcessBuilder.Redirect.to(this.stdOutFile))
                .stdErr(ProcessBuilder.Redirect.to(this.stdErrFile))
                .start();
        int r = Exceptions.<Exception, Integer>handleInterruptedCall(p::waitFor);
        Assert.assertEquals("Unexpected response code.", 0, r);

        List<String> stdOut = readFile(this.stdOutFile);
        Assert.assertEquals("Unexpected args output to StdOut.", formatArgs(PREFIX_OUT, ARGS), stdOut.get(0));
        Assert.assertEquals("Unexpected sysProp output to StdOut.", formatSysProp(PREFIX_OUT, SYS_PROP.getKey(), SYS_PROP.getValue()), stdOut.get(1));
        Assert.assertEquals("Unexpected envVar output to StdOut.", formatEnvVar(PREFIX_OUT, ENV_VAR.getKey(), ENV_VAR.getValue()), stdOut.get(2));

        List<String> stdErr = readFile(this.stdErrFile);
        Assert.assertEquals("Unexpected args output to StdErr.", formatArgs(PREFIX_ERR, ARGS), stdErr.get(0));
        Assert.assertEquals("Unexpected sysProp output to StdErr.", formatSysProp(PREFIX_ERR, SYS_PROP.getKey(), SYS_PROP.getValue()), stdErr.get(1));
        Assert.assertEquals("Unexpected envVar output to StdErr.", formatEnvVar(PREFIX_ERR, ENV_VAR.getKey(), ENV_VAR.getValue()), stdErr.get(2));
    }

    private List<String> readFile(File f) throws IOException {
        return Arrays.stream(new Scanner(f).useDelimiter("\\Z'").next().split(System.lineSeparator()))
                .filter(s -> s.startsWith(PREFIX_OUT) || s.startsWith(PREFIX_ERR))
                .collect(Collectors.toList());
    }

    private static String formatSysProp(String prefix, String key, String value) {
        return String.format("%s: sysprop: %s=%s", prefix, key, value);
    }

    private static String formatEnvVar(String prefix, String key, String value) {
        return String.format("%s: envvar: %s=%s", prefix, key, value);
    }

    private static <T> String formatArgs(String prefix, T[] arg) {
        return String.format("%s: arg: %s",
                prefix,
                String.join(",", Arrays.stream(arg).map(Object::toString).collect(Collectors.toList())));
    }

    /**
     * Main method is required for proper testing in this class. Do not delete.
     */
    public static void main(String[] args) {
        // Print out some dummy text. This ensures that our testing code ignores whatever else the environment throws at us.
        System.out.println("This line to be ignored");
        System.err.println("This line to be ignored");

        System.out.println(formatArgs(PREFIX_OUT, args));
        System.err.println(formatArgs(PREFIX_ERR, args));

        System.out.println(formatSysProp(PREFIX_OUT, SYS_PROP.getKey(), System.getProperty(SYS_PROP.getKey())));
        System.err.println(formatSysProp(PREFIX_ERR, SYS_PROP.getKey(), System.getProperty(SYS_PROP.getKey())));

        System.out.println(formatEnvVar(PREFIX_OUT, ENV_VAR.getKey(), System.getenv(ENV_VAR.getKey())));
        System.err.println(formatEnvVar(PREFIX_ERR, ENV_VAR.getKey(), System.getenv(ENV_VAR.getKey())));
    }
}
