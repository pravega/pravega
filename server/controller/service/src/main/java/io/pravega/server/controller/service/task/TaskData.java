/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.server.controller.service.task;

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
