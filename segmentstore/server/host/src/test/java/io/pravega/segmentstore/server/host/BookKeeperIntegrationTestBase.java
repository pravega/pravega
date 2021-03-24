/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.segmentstore.server.host;

import io.pravega.common.io.FileHelpers;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.segmentstore.server.store.ServiceConfig;
import io.pravega.segmentstore.server.store.StreamSegmentStoreTestBase;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperConfig;
import java.io.File;
import java.nio.file.Files;
import lombok.AccessLevel;
import lombok.Getter;

/**
 * Base class for any StreamSegmentStore Integration Test that uses BookKeeper.
 */
public abstract class BookKeeperIntegrationTestBase extends StreamSegmentStoreTestBase {
    private static final int BOOKIE_COUNT = 1;
    @Getter(AccessLevel.PROTECTED)
    private File baseDir = null;
    @Getter(AccessLevel.PROTECTED)
    private BookKeeperRunner bookkeeper = null;

    /**
     * Starts BookKeeper.
     *
     * NOTE: this (and tearDown()) cannot be annotated with @Before and @After since JUnit doesn't pick these up from
     * super classes, at least not in the order in which we expect it to)
     * @throws Exception if an error occurred.
     */
    protected void setUp() throws Exception {
        bookkeeper = new BookKeeperRunner(this.configBuilder, BOOKIE_COUNT);
        bookkeeper.initialize();

        this.baseDir = Files.createTempDirectory("IntegrationTest").toFile().getAbsoluteFile();
    }

    /**
     * Shuts down BookKeeper and cleans up file system directory.
     * @throws Exception if an error occurred.
     */
    protected void tearDown() throws Exception {
        bookkeeper.close();
        if (baseDir != null) {
            FileHelpers.deleteFileOrDirectory(this.baseDir);
        }

        this.baseDir = null;
    }

    /**
     * Creates a ServiceBuilderConfig based on the given builder.
     *
     * @param configBuilder The ServiceBuilderConfig.Builder to base from (this builder will not be touched).
     * @param instanceId    The instance id of the Service to build (for least interference, different instances should have
     *                      different Ids so that shared resources can be setup appropriately).
     * @return A ServiceBuilderConfig instance.
     */
    protected ServiceBuilderConfig getBuilderConfig(ServiceBuilderConfig.Builder configBuilder, int instanceId) {
        String id = Integer.toString(instanceId);
        return configBuilder
                .makeCopy()
                .include(BookKeeperConfig.builder().with(BookKeeperConfig.BK_ENFORCE_MIN_NUM_RACKS_PER_WRITE, false)
                                                   .with(BookKeeperConfig.BK_MIN_NUM_RACKS_PER_WRITE_QUORUM, 1))                    
                .include(ServiceConfig.builder().with(ServiceConfig.INSTANCE_ID, id))
                .build();
    }

    @Override
    protected double getFencingTestOperationMultiplier() {
        return 0.3; // Adding operations one-by-one using BookKeeper is much slower than bulk-adding them.
    }
}
