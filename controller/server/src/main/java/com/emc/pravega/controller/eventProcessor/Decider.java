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
package com.emc.pravega.controller.eventProcessor;

/**
 * Functional interface that provides a Directive, which is either Restart, Resume,
 * or Stop, on encountering an exception while executing event procssors's user-defined methods.
 */
@FunctionalInterface
public interface Decider {

    enum Directive {
        Restart,
        Resume,
        Stop
    }

    /**
     * The default decider that Stops the event processor on pre-defined event processor exceptions,
     * and Restarts it otherwise.
     */
    Decider DEFAULT_DECIDER = (Throwable y) -> {
        if (y instanceof EventProcessorInitException ||
                y instanceof EventProcessorReinitException) {
            return Decider.Directive.Stop;
        } else {
            return Decider.Directive.Restart;
        }
    };

    /**
     * The decider method that returns a Directive.
     *
     * @param throwable Exception
     * @return Directive
     */
    Directive run(Throwable throwable);
}
