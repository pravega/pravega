/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common.lang;

import io.pravega.common.Exceptions;
import java.io.File;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.Arrays;
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
        int r = Exceptions.<Exception, Integer>handleInterrupted(p::waitFor);
        Assert.assertEquals("Unexpected response code.", 0, r);

        String[] stdOut = readFile(this.stdOutFile);
        Assert.assertEquals("Unexpected args output to StdOut.", formatArgs("out", ARGS), stdOut[0]);
        Assert.assertEquals("Unexpected sysProp output to StdOut.", formatSysProp("out", SYS_PROP.getKey(), SYS_PROP.getValue()), stdOut[1]);
        Assert.assertEquals("Unexpected envVar output to StdOut.", formatEnvVar("out", ENV_VAR.getKey(), ENV_VAR.getValue()), stdOut[2]);

        String[] stdErr = readFile(this.stdErrFile);
        Assert.assertEquals("Unexpected args output to StdErr.", formatArgs("err", ARGS), stdErr[0]);
        Assert.assertEquals("Unexpected sysProp output to StdErr.", formatSysProp("err", SYS_PROP.getKey(), SYS_PROP.getValue()), stdErr[1]);
        Assert.assertEquals("Unexpected envVar output to StdErr.", formatEnvVar("err", ENV_VAR.getKey(), ENV_VAR.getValue()), stdErr[2]);
    }

    private String[] readFile(File f) throws IOException {
        return new Scanner(f).useDelimiter("\\Z'").next().split(System.lineSeparator());
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
        System.out.println(formatArgs("out", args));
        System.err.println(formatArgs("err", args));

        System.out.println(formatSysProp("out", SYS_PROP.getKey(), System.getProperty(SYS_PROP.getKey())));
        System.err.println(formatSysProp("err", SYS_PROP.getKey(), System.getProperty(SYS_PROP.getKey())));

        System.out.println(formatEnvVar("out", ENV_VAR.getKey(), System.getenv(ENV_VAR.getKey())));
        System.err.println(formatEnvVar("err", ENV_VAR.getKey(), System.getenv(ENV_VAR.getKey())));
    }
}
