/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.framework.metronome.model.v1;

import io.pravega.framework.metronome.ModelUtils;
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
