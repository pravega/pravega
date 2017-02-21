/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */

package com.emc.pravega.framework.metronome.model.v1;

import com.emc.pravega.framework.metronome.ModelUtils;
import lombok.Getter;
import lombok.Setter;

import java.util.List;
import java.util.Map;

@Getter
@Setter
public class Run {
    private List<Artifact> artifacts;
    private String cmd;
    private double cpus;
    private double mem;
    private double disk;
    private Map<String, String> env;
    private int maxLaunchDelay;
    private Restart restart;
    private String user;

    @Override
    public String toString() {
        return ModelUtils.toString(this);
    }
}
