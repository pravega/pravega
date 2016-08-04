/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.emc.pravega.service.server.reading;

import com.emc.pravega.service.storage.Cache;
import com.emc.pravega.service.server.ContainerMetadata;
import com.emc.pravega.service.server.ReadIndex;
import com.emc.pravega.service.server.ReadIndexFactory;
import com.google.common.base.Preconditions;

/**
 * Default implementation for ReadIndexFactory.
 */
public class ContainerReadIndexFactory implements ReadIndexFactory {
    private final Cache<CacheKey> cache;

    /**
     * Creates a new instance of the ContainerReadIndexFactory class.
     *
     * @param cache The cache to use.
     */
    public ContainerReadIndexFactory(Cache<CacheKey> cache) {
        Preconditions.checkNotNull(cache, "cache");
        this.cache = cache;
    }

    @Override
    public ReadIndex createReadIndex(ContainerMetadata containerMetadata) {
        return new ContainerReadIndex(containerMetadata, this.cache);
    }
}
