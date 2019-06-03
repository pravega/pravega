/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.store.stream;

import lombok.Data;

@Data
/**
 * Wrapper data class that wraps a generic object with a version. 
 */
public class VersionedMetadata<OBJECT> {
    private final OBJECT object;
    private final Version version;
}
