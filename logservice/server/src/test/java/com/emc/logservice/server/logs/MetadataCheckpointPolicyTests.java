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

package com.emc.logservice.server.logs;

import com.emc.logservice.server.service.ServiceBuilderConfig;
import org.junit.Assert;
import org.junit.Test;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Unit tests for MetadataCheckpointPolicy.
 */
public class MetadataCheckpointPolicyTests {
    /**
     * Tests invoking the callback upon calls to commit.
     */
    @Test
    public void testInvokeCallback() throws Exception {
        final int recordCount = 10000;
        final int recordLength = 100;

        ExecutorService executor = Executors.newScheduledThreadPool(2);
        try {

            //1. MinCommit Count: Triggering is delayed until min number of recordings happen.
            DurableLogConfig config = createConfig(10, recordCount + 1, 1); // If no minCount, this would trigger every time (due to length).
            AtomicInteger callbackCount = new AtomicInteger();
            MetadataCheckpointPolicy p = new MetadataCheckpointPolicy(config, callbackCount::incrementAndGet, executor);
            for (int i = 0; i < recordCount; i++) {
                p.recordCommit(recordLength);
            }

            int expectedCallCount = recordCount / config.getCheckpointMinCommitCount();
            Thread.sleep(20); // The callback may not have been invoked yet.
            Assert.assertEquals("Unexpected number of calls when MinCount > CommitCount.", expectedCallCount, callbackCount.get());

            //2. Triggered by count threshold.
            config = createConfig(1, 10, Integer.MAX_VALUE);
            callbackCount.set(0);
            p = new MetadataCheckpointPolicy(config, callbackCount::incrementAndGet, executor);
            for (int i = 0; i < recordCount; i++) {
                p.recordCommit(recordLength);
            }

            expectedCallCount = recordCount / config.getCheckpointCommitCountThreshold();
            Thread.sleep(20); // The callback may not have been invoked yet.
            Assert.assertEquals("Unexpected number of calls when MinCount > CommitCount.", expectedCallCount, callbackCount.get());

            //3. Triggered by length threshold.
            config = createConfig(1, Integer.MAX_VALUE, recordLength * 10);
            callbackCount.set(0);
            p = new MetadataCheckpointPolicy(config, callbackCount::incrementAndGet, executor);
            for (int i = 0; i < recordCount; i++) {
                p.recordCommit(recordLength);
            }

            expectedCallCount = (int) (recordCount * recordLength / config.getCheckpointTotalCommitLengthThreshold());
            Thread.sleep(20); // The callback may not have been invoked yet.
            Assert.assertEquals("Unexpected number of calls when MinCount > CommitCount.", expectedCallCount, callbackCount.get());
        } finally {
            executor.shutdown();
        }
    }

    private DurableLogConfig createConfig(int minCount, int commitCount, int length) {
        Properties p = new Properties();
        ServiceBuilderConfig.set(p, DurableLogConfig.COMPONENT_CODE, DurableLogConfig.PROPERTY_CHECKPOINT_MIN_COMMIT_COUNT, Integer.toString(minCount));
        ServiceBuilderConfig.set(p, DurableLogConfig.COMPONENT_CODE, DurableLogConfig.PROPERTY_CHECKPOINT_COMMIT_COUNT, Integer.toString(commitCount));
        ServiceBuilderConfig.set(p, DurableLogConfig.COMPONENT_CODE, DurableLogConfig.PROPERTY_CHECKPOINT_TOTAL_COMMIT_LENGTH, Integer.toString(length));
        return new DurableLogConfig(p);
    }
}
