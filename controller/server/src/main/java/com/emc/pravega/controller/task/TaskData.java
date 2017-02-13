/**
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.emc.pravega.controller.task;

import lombok.Data;
import org.apache.commons.lang.SerializationUtils;

import java.io.Serializable;

/**
 * Task data: task name and its parameters.
 */
@Data
public class TaskData implements Serializable {
    private String methodName;
    private String methodVersion;
    private Serializable[] parameters;

    public byte[] serialize() {
        return SerializationUtils.serialize(this);
    }

    public static TaskData deserialize(final byte[] bytes) {
        return (TaskData) SerializationUtils.deserialize(bytes);
    }
}
