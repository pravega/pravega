/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.task;

import java.io.Serializable;
import java.util.Arrays;

import org.apache.commons.lang3.SerializationUtils;

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
