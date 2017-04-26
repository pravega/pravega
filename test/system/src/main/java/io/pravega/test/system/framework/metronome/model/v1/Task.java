/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package io.pravega.test.system.framework.metronome.model.v1;

import io.pravega.test.system.framework.metronome.ModelUtils;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class Task {
    private String id;
    private String startedAt;
    private String status;

    @Override
    public String toString() {
        return ModelUtils.toString(this);
    }
}
