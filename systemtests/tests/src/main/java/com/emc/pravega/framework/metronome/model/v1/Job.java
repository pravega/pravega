/**
 *  Copyright (c) 2016 Dell Inc. or its subsidiaries. All Rights Reserved
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 *
 */

package com.emc.pravega.framework.metronome.model.v1;

import com.emc.pravega.framework.metronome.ModelUtils;

import java.util.List;
import java.util.Map;

public class Job {
    private String id;
    private String description;
    private Map<String, String> labels;
    private Run run;
    private History history;
    private List<ActiveRun> activeRuns;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public Map<String, String> getLabels() {
        return labels;
    }

    public void setLabels(Map<String, String> labels) {
        this.labels = labels;
    }

    public Run getRun() {
        return run;
    }

    public void setRun(Run run) {
        this.run = run;
    }

    public History getHistory() {
        return history;
    }

    public void setHistory(History history) {
        this.history = history;
    }

    public List<ActiveRun> getActiveRuns() {
        return activeRuns;
    }

    public void setActiveRuns(List<ActiveRun> activeRuns) {
        this.activeRuns = activeRuns;
    }

    @Override
    public String toString() {
        return ModelUtils.toString(this);
    }
}
