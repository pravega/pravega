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

import java.io.Serializable;
import java.util.Arrays;

import org.apache.commons.lang.SerializationUtils;

import lombok.Data;

/**
 * Task data: task name and its parameters.
 */
@Data
public class TaskData implements Serializable {
    private final String methodName;
    private final String methodVersion;
    private final Serializable[] parameters;

    TaskData(String methodName, String methodVersion, Serializable[] parameters) {
        this.methodName = methodName;
        this.methodVersion = methodVersion;
        this.parameters = Arrays.copyOf(parameters, parameters.length);
    }

    public byte[] serialize() {
        return SerializationUtils.serialize(this);
    }

    public static TaskData deserialize(final byte[] bytes) {
        return (TaskData) SerializationUtils.deserialize(bytes);
    }
    
    public Serializable[] getParameters() {
        return Arrays.copyOf(parameters, parameters.length);
    }
}
