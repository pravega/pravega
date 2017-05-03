/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
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
package io.pravega.service.storage.impl.hdfs;

import io.pravega.common.LoggerHelpers;
import java.util.concurrent.Callable;

import lombok.extern.slf4j.Slf4j;
import lombok.val;

/**
 * FileSystemOperation that determines whether a Segment exists.
 */
@Slf4j
public class ExistsOperation extends FileSystemOperation<String> implements Callable<Boolean> {
    /**
     * Creates a new instance of the ExistsOperation class.
     *
     * @param segmentName The name of the Segment to check existence for.
     * @param context     Context for the operation.
     */
    ExistsOperation(String segmentName, OperationContext context) {
        super(segmentName, context);
    }

    @Override
    public Boolean call() throws Exception {
        String segmentName = getTarget();
        long traceId = LoggerHelpers.traceEnter(log, "exists", segmentName);
        val files = findAll(segmentName, false);
        boolean exists = files.size() > 0;
        LoggerHelpers.traceLeave(log, "exists", traceId, segmentName, exists);
        return exists;
    }
}
