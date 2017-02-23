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
public class History {
    private int successCount;
    private int failureCount;
    private String lastSuccessAt;
    private String lastFailureAt;

    @Override
    public String toString() {
        return ModelUtils.toString(this);
    }
}
