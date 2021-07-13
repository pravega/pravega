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

import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import javax.annotation.concurrent.NotThreadSafe;
import lombok.val;

/**
 * Helps start a Class out of Process.
 */
@NotThreadSafe
public class ProcessStarter {
    //region Members

    private final Class<?> target;
    private final ProcessBuilder builder;
    private final HashMap<String, String> systemProps;
    private Object[] args;

    //endregion

    // region Constructor

    /**
     * Creates a new instance of the ProcessStarter class.
     *
     * @param target The Class to start. This class must have a static main() method defined in it.
     */
    private ProcessStarter(Class<?> target) {
        this.target = Preconditions.checkNotNull(target, "target");
        this.builder = new ProcessBuilder().inheritIO();
        this.systemProps = new HashMap<>();
    }

    /**
     * Creates a new instance of the ProcessStarter class.
     *
     * @param target The Class to start. This class must have a static main() method defined in it.
     *
     * @return new ProcessStarter instance
     *
     */
    public static ProcessStarter forClass(Class<?> target) {
        return new ProcessStarter(target);
    }

    //endregion

    //region Configuration and Execution

    /**
     * Includes the given System Property as part of the start.
     *
     * @param name  The System Property Name.
     * @param value The System Property Value. This will have toString() invoked on it.
     * @return This object instance.
     */
    public ProcessStarter sysProp(String name, Object value) {
        this.systemProps.put(name, value.toString());
        return this;
    }

    /**
     * Includes the given Environment Variable as part of the start.
     *
     * @param name  The Environment Variable Name.
     * @param value The Environment Variable Value. This will have toString() invoked on it.
     * @return This object instance.
     */
    public ProcessStarter env(String name, Object value) {
        this.builder.environment().put(name, value.toString());
        return this;
    }

    /**
     * Redirects the Standard Out of the Process to the given Redirect.
     *
     * @param redirect The ProcessBuilder.Redirect to use.
     * @return This object instance.
     */
    public ProcessStarter stdOut(ProcessBuilder.Redirect redirect) {
        this.builder.redirectOutput(redirect);
        return this;
    }

    /**
     * Redirects the Standard Err of the Process to the given Redirect.
     *
     * @param redirect The ProcessBuilder.Redirect to use.
     * @return This object instance.
     */
    public ProcessStarter stdErr(ProcessBuilder.Redirect redirect) {
        this.builder.redirectError(redirect);
        return this;
    }

    /**
     * Redirects the Standard In of the Process to the given Redirect.
     *
     * @param redirect The ProcessBuilder.Redirect to use.
     * @return This object instance.
     */
    public ProcessStarter stdIn(ProcessBuilder.Redirect redirect) {
        this.builder.redirectInput(redirect);
        return this;
    }

    /**
     * Starts the Process using the given command-line arguments.
     *
     * @param args The arguments to use.
     * @return This object instance.
     */
    public ProcessStarter args(Object... args) {
        this.args = args;
        return this;
    }

    /**
     * Executes the Class using the accumulated options.
     *
     * @return The Process reference.
     * @throws IOException If an error occurred.
     */
    public Process start() throws IOException {
        ArrayList<String> cmd = new ArrayList<>();

        // Invoke Java and setup classpath.
        cmd.add(System.getProperty("java.home") + File.separator + "bin" + File.separator + "java");
        cmd.add("-cp");
        cmd.add(System.getProperty("java.class.path"));

        // Set System Properties (if any).
        for (val e : this.systemProps.entrySet()) {
            cmd.add(String.format("-D%s=%s", e.getKey(), e.getValue()));
        }
        // Set class & class arguments.
        cmd.add(this.target.getCanonicalName());
        if (this.args != null) {
            for (Object arg : this.args) {
                cmd.add(arg.toString());
            }
        }

        this.builder.command(cmd);
        return this.builder.start();
    }
    //endregion
}