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
package io.pravega.segmentstore.server.logs;

import io.pravega.test.common.ThreadPooledTestSuite;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

/**
 * Unit tests for MetadataCheckpointPolicy.
 */
public class MetadataCheckpointPolicyTests extends ThreadPooledTestSuite {
    @Rule
    public Timeout globalTimeout = Timeout.seconds(10);

    /**
     * Tests invoking the callback upon calls to commit.
     */
    @Test
    public void testInvokeCallback() throws Exception {
        final int recordCount = 10000;
        final int recordLength = 100;

        //1. MinCommit Count: Triggering is delayed until min number of recordings happen.
        DurableLogConfig config = DurableLogConfig.builder()
                                                  .with(DurableLogConfig.CHECKPOINT_MIN_COMMIT_COUNT, 10)
                                                  .with(DurableLogConfig.CHECKPOINT_COMMIT_COUNT, recordCount + 1)
                                                  // If no minCount, this would trigger every time (due to length).
                                                  .with(DurableLogConfig.CHECKPOINT_TOTAL_COMMIT_LENGTH, 1L)
                                                  .build();
        AtomicInteger callbackCount = new AtomicInteger();
        MetadataCheckpointPolicy p = new MetadataCheckpointPolicy(config, callbackCount::incrementAndGet, executorService());
        for (int i = 0; i < recordCount; i++) {
            p.recordCommit(recordLength);
        }
        int expectedCallCount = recordCount / config.getCheckpointMinCommitCount();
        Assert.assertEquals("Unexpected number of calls when MinCount > CommitCount.", expectedCallCount, callbackCount.get());

        //2. Triggered by count threshold.
        config = DurableLogConfig.builder()
                                 .with(DurableLogConfig.CHECKPOINT_MIN_COMMIT_COUNT, 1)
                                 .with(DurableLogConfig.CHECKPOINT_COMMIT_COUNT, 10)
                                 .with(DurableLogConfig.CHECKPOINT_TOTAL_COMMIT_LENGTH, (long) Integer.MAX_VALUE)
                                 .build();

        callbackCount.set(0);
        p = new MetadataCheckpointPolicy(config, callbackCount::incrementAndGet, executorService());
        for (int i = 0; i < recordCount; i++) {
            p.recordCommit(recordLength);
        }

        expectedCallCount = recordCount / config.getCheckpointCommitCountThreshold();
        Assert.assertEquals("Unexpected number of calls when MinCou      nt > CommitCount.", expectedCallCount, callbackCount.get());

        //3. Triggered by length threshold.
        config = DurableLogConfig.builder()
                                 .with(DurableLogConfig.CHECKPOINT_MIN_COMMIT_COUNT, 1)
                                 .with(DurableLogConfig.CHECKPOINT_COMMIT_COUNT, Integer.MAX_VALUE)
                                 .with(DurableLogConfig.CHECKPOINT_TOTAL_COMMIT_LENGTH, recordLength * 10L)
                                 .build();

        callbackCount.set(0);
        p = new MetadataCheckpointPolicy(config, callbackCount::incrementAndGet, executorService());
        for (int i = 0; i < recordCount; i++) {
            p.recordCommit(recordLength);
        }

        expectedCallCount = (int) (recordCount * recordLength / config.getCheckpointTotalCommitLengthThreshold());
        Assert.assertEquals("Unexpected number of calls when MinCount > CommitCount.", expectedCallCount, callbackCount.get());
    }
}
