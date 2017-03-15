/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package com.emc.pravega.framework.mesos.model.v1;

import com.google.gson.annotations.SerializedName;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

@Getter
@Setter
public class SlaveState {

    private String id;
    private String hostname;
    private List<Framework> frameworks;
    @SerializedName("completed_frameworks")
    private List<Framework> completedFrameworks;
}
