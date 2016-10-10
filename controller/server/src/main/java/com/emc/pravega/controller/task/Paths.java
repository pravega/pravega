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
package com.emc.pravega.controller.task;

/**
 * Static list of path prefixes
 */
public class Paths {

    // Task data per stream. A stream can have at most one task under execution at any given point of time.
    public static final String streamTasks = "/tasks/streams/%s/";

    // Locks held by hosts on streams
    public static final String streamLocks = "/locks/streams/%s/";

    // List of tasks being executed by a host.
    // This list is kind of an index for quick access to tasks being executed by a failed host.
    public static final String hostTasks = "/hosts/%s";
}
