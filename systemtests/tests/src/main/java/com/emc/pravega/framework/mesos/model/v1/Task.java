/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package com.emc.pravega.framework.mesos.model.v1;

import com.google.gson.annotations.SerializedName;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class Task {
    private String id;
    private String name;
    @SerializedName("framework_id")
    private String frameworkId;
    @SerializedName("slave_id")
    private String slaveId;
}
