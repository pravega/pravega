/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.storage.extendeds3;

import com.emc.object.s3.bean.AccessControlList;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.experimental.Wither;

/**
 * ACL and size representation for the extended S3 simulator wrappers.
 */
@Getter
@AllArgsConstructor
public class AclSize {
    @Wither
    private final AccessControlList acl;
    @Wither
    private final long size;
}
