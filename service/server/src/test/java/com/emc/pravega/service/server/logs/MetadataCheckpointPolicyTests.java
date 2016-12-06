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

package com.emc.pravega.service.server.logs;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Test;

import com.emc.pravega.common.concurrent.InlineExecutor;
import com.emc.pravega.common.util.PropertyBag;
import com.emc.pravega.service.server.ConfigHelpers;

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


        InlineExecutor executor = new InlineExecutor();
        //1. MinCommit Count: Triggering is delayed until min number of recordings happen.
        DurableLogConfig config = ConfigHelpers.createDurableLogConfig(
                PropertyBag.create()
                           .with(DurableLogConfig.PROPERTY_CHECKPOINT_MIN_COMMIT_COUNT, 10)
                           .with(DurableLogConfig.PROPERTY_CHECKPOINT_COMMIT_COUNT, recordCount + 1)
                           .with(DurableLogConfig.PROPERTY_CHECKPOINT_TOTAL_COMMIT_LENGTH, 1)); // If no minCount, this would trigger every time (due to length).
        AtomicInteger callbackCount = new AtomicInteger();
        MetadataCheckpointPolicy p = new MetadataCheckpointPolicy(config, callbackCount::incrementAndGet, executor);
        for (int i = 0; i < recordCount; i++) {
            p.recordCommit(recordLength);
        }
        int expectedCallCount = recordCount / config.getCheckpointMinCommitCount();
        Assert.assertEquals("Unexpected number of calls when MinCount > CommitCount.", expectedCallCount, callbackCount.get());

        //2. Triggered by count threshold.
        config = ConfigHelpers.createDurableLogConfig(
                PropertyBag.create()
                           .with(DurableLogConfig.PROPERTY_CHECKPOINT_MIN_COMMIT_COUNT, 1)
                           .with(DurableLogConfig.PROPERTY_CHECKPOINT_COMMIT_COUNT, 10)
                           .with(DurableLogConfig.PROPERTY_CHECKPOINT_TOTAL_COMMIT_LENGTH, Integer.MAX_VALUE));

        callbackCount.set(0);
        p = new MetadataCheckpointPolicy(config, callbackCount::incrementAndGet, executor);
        for (int i = 0; i < recordCount; i++) {
            p.recordCommit(recordLength);
        }

        expectedCallCount = recordCount / config.getCheckpointCommitCountThreshold();
        Assert.assertEquals("Unexpected number of calls when MinCou      nt > CommitCount.", expectedCallCount, callbackCount.get());

        //3. Triggered by length threshold.
        config = ConfigHelpers.createDurableLogConfig(
                PropertyBag.create()
                           .with(DurableLogConfig.PROPERTY_CHECKPOINT_MIN_COMMIT_COUNT, 1)
                           .with(DurableLogConfig.PROPERTY_CHECKPOINT_COMMIT_COUNT, Integer.MAX_VALUE)
                           .with(DurableLogConfig.PROPERTY_CHECKPOINT_TOTAL_COMMIT_LENGTH, recordLength * 10));

        callbackCount.set(0);
        p = new MetadataCheckpointPolicy(config, callbackCount::incrementAndGet, executor);
        for (int i = 0; i < recordCount; i++) {
            p.recordCommit(recordLength);
        }

        expectedCallCount = (int) (recordCount * recordLength / config.getCheckpointTotalCommitLengthThreshold());
        Assert.assertEquals("Unexpected number of calls when MinCount > CommitCount.", expectedCallCount, callbackCount.get());
    }
}
