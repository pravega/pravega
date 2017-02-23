/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.framework.metronome.model.v1;

import com.emc.pravega.framework.metronome.ModelUtils;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class Artifact {
    private String uri;
    private boolean extract;
    private boolean executable;
    private boolean cache;

    @Override
    public String toString() {
        return ModelUtils.toString(this);
    }
}
