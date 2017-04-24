/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.framework.metronome.model.v1;

import io.pravega.framework.metronome.ModelUtils;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

@Getter
@Setter
public class ActiveRun {
    private String id;
    private String jobId;
    private String status;
    private String createdAt;
    private String completedAt;
    private List<Task> tasks;

    @Override
    public String toString() {
        return ModelUtils.toString(this);
    }
}
