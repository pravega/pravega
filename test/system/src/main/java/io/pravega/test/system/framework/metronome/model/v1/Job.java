/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.system.framework.metronome.model.v1;

import io.pravega.test.system.framework.metronome.ModelUtils;
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
