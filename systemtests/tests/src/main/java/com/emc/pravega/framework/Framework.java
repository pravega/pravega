/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package com.emc.pravega.framework;

import com.google.gson.annotations.SerializedName;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

@Getter
@Setter
public class Framework {
    private String id;
    private String name;
    private String hostname;
    private List<Executor> executors;
    @SerializedName("completed_executors")
    private List<Executor> completedExecutors;
}
