/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common.util;

import java.util.Date;

import lombok.Data;

@Data
public class ImmutableDate {
    private final long time;
    
    public ImmutableDate() {
        time = System.currentTimeMillis();
    }
    
    public ImmutableDate(long time) {
        this.time = time;
    }

    public Date asDate() {
        return new Date(time);
    }
}
