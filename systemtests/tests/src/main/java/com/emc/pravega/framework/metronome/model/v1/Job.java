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
public class Job {
    private String id;
    private String description;
    private Map<String, String> labels;
    private Run run;
    private History history;
    private List<ActiveRun> activeRuns;

    @Override
    public String toString() {
        return ModelUtils.toString(this);
    }
}
