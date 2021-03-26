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
package io.pravega.test.common;

import io.netty.util.ResourceLeakDetector;
import io.netty.util.internal.logging.InternalLogLevel;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.Slf4JLoggerFactory;
import lombok.RequiredArgsConstructor;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

/**
 * Base class for a unit test that wants to check for Netty (ByteBuf) resource leaks. Automatically fails any tests
 * for which {@link ResourceLeakDetector} reports a leak.
 * <p>
 * This works by attaching a specialized {@link Slf4JLoggerFactory} to the {@link ResourceLeakDetector} and invokes
 * {@link Assert#fail} when {@link InternalLogger#error} is invoked.
 * <p>
 * NOTE:
 * Ensure that Log4j is properly configured in the (Test) Project where this is used. If you get a `WARN` when running
 * your test saying that Log4j is not properly configured (no appenders set up), the Leak Detector WILL NOT WORK. At the
 * very least, verify the following are included as dependencies in your Test Project:
 * <pre><code>
 * testCompile group: 'org.slf4j', name: 'log4j-over-slf4j', version: slf4jApiVersion
 * testCompile group: 'ch.qos.logback', name: 'logback-classic', version: qosLogbackVersion
 * </code></pre>
 */
public abstract class LeakDetectorTestSuite extends ThreadPooledTestSuite {
    private ResourceLeakDetector.Level originalLevel;

    @Override
    @Before
    public void before() throws Exception {
        super.before();
        InternalLoggerFactory.setDefaultFactory(new ResourceLeakLoggerFactory());
        this.originalLevel = ResourceLeakDetector.getLevel();
        ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
    }

    @Override
    @After
    public void after() throws Exception {
        super.after();
        ResourceLeakDetector.setLevel(this.originalLevel);
    }

    @SuppressWarnings("deprecation")
    private static class ResourceLeakLoggerFactory extends Slf4JLoggerFactory {
        @Override
        public InternalLogger newInstance(String name) {
            InternalLogger baseLogger = ((Slf4JLoggerFactory) Slf4JLoggerFactory.INSTANCE).newInstance(name);
            if (name.equals(ResourceLeakDetector.class.getName())) {
                return new ResourceLeakAssertionLogger(baseLogger);
            } else {
                return baseLogger;
            }
        }

        @RequiredArgsConstructor
        private static class ResourceLeakAssertionLogger implements InternalLogger {
            private final InternalLogger wrappedLogger;

            @Override
            public String name() {
                return this.wrappedLogger.name();
            }

            @Override
            public boolean isTraceEnabled() {
                return this.wrappedLogger.isTraceEnabled();
            }

            @Override
            public void trace(String msg) {
                this.wrappedLogger.trace(msg);
            }

            @Override
            public void trace(String format, Object arg) {
                this.wrappedLogger.trace(format, arg);
            }

            @Override
            public void trace(String format, Object argA, Object argB) {
                this.wrappedLogger.trace(format, argA, argB);
            }

            @Override
            public void trace(String format, Object... arguments) {
                this.wrappedLogger.trace(format, arguments);
            }

            @Override
            public void trace(String msg, Throwable t) {
                this.wrappedLogger.trace(msg, t);
            }

            @Override
            public void trace(Throwable t) {
                this.wrappedLogger.trace(t);
            }

            @Override
            public boolean isDebugEnabled() {
                return this.wrappedLogger.isDebugEnabled();
            }

            @Override
            public void debug(String msg) {
                this.wrappedLogger.debug(msg);
            }

            @Override
            public void debug(String format, Object arg) {
                this.wrappedLogger.debug(format, arg);
            }

            @Override
            public void debug(String format, Object argA, Object argB) {
                this.wrappedLogger.debug(format, argA, argB);
            }

            @Override
            public void debug(String format, Object... arguments) {
                this.wrappedLogger.debug(format, arguments);
            }

            @Override
            public void debug(String msg, Throwable t) {
                this.wrappedLogger.debug(msg, t);
            }

            @Override
            public void debug(Throwable t) {
                this.wrappedLogger.debug(t);
            }

            @Override
            public boolean isInfoEnabled() {
                return this.wrappedLogger.isInfoEnabled();
            }

            @Override
            public void info(String msg) {
                this.wrappedLogger.info(msg);
            }

            @Override
            public void info(String format, Object arg) {
                this.wrappedLogger.info(format, arg);
            }

            @Override
            public void info(String format, Object argA, Object argB) {
                this.wrappedLogger.info(format, argA, argB);
            }

            @Override
            public void info(String format, Object... arguments) {
                this.wrappedLogger.info(format, arguments);
            }

            @Override
            public void info(String msg, Throwable t) {
                this.wrappedLogger.info(msg, t);
            }

            @Override
            public void info(Throwable t) {
                this.wrappedLogger.info(t);
            }

            @Override
            public boolean isWarnEnabled() {
                return this.wrappedLogger.isWarnEnabled();
            }

            @Override
            public void warn(String msg) {
                this.wrappedLogger.warn(msg);
            }

            @Override
            public void warn(String format, Object arg) {
                this.wrappedLogger.warn(format, arg);
            }

            @Override
            public void warn(String format, Object... arguments) {
                this.wrappedLogger.warn(format, arguments);
            }

            @Override
            public void warn(String format, Object argA, Object argB) {
                this.wrappedLogger.warn(format, argA, argB);
            }

            @Override
            public void warn(String msg, Throwable t) {
                this.wrappedLogger.warn(msg, t);
            }

            @Override
            public void warn(Throwable t) {
                this.wrappedLogger.warn(t);
            }

            @Override
            public boolean isErrorEnabled() {
                return this.wrappedLogger.isErrorEnabled();
            }

            @Override
            public void error(String msg) {
                error(msg, new Object[0]);
            }

            @Override
            public void error(String format, Object arg) {
                error(format, new Object[]{arg});
            }

            @Override
            public void error(String format, Object argA, Object argB) {
                error(format, new Object[]{argA, argB});
            }

            @Override
            public void error(String format, Object... arguments) {
                this.wrappedLogger.error(format, arguments);
                Assert.fail("RESOURCE LEAK: " + String.format(format, arguments));
            }

            @Override
            public void error(String msg, Throwable t) {
                this.wrappedLogger.error(msg, t);
                Assert.fail(String.format("RESOURCE LEAK: %s (%s)", msg, t));
            }

            @Override
            public void error(Throwable t) {
                error("", t);
            }

            @Override
            public boolean isEnabled(InternalLogLevel level) {
                return this.wrappedLogger.isEnabled(level);
            }

            @Override
            public void log(InternalLogLevel level, String msg) {
                log(level, msg, new Object[0]);
            }

            @Override
            public void log(InternalLogLevel level, String format, Object arg) {
                log(level, format, new Object[]{arg});
            }

            @Override
            public void log(InternalLogLevel level, String format, Object argA, Object argB) {
                log(level, format, new Object[]{argA, argB});
            }

            @Override
            public void log(InternalLogLevel level, String format, Object... arguments) {
                this.wrappedLogger.log(level, format, arguments);
                if (level == InternalLogLevel.ERROR) {
                    Assert.fail("RESOURCE LEAK: " + String.format(format, arguments));
                }
            }

            @Override
            public void log(InternalLogLevel level, String msg, Throwable t) {
                this.wrappedLogger.log(level, msg, t);
                if (level == InternalLogLevel.ERROR) {
                    Assert.fail(String.format("RESOURCE LEAK: %s (%s)", msg, t));
                }
            }

            @Override
            public void log(InternalLogLevel level, Throwable t) {
                log(level, "", t);
            }
        }
    }

}
