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
package io.pravega.segmentstore.storage.chunklayer;

import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.util.ArrayList;

/**
 * Utility class that helps with clean up of list of objects.
 * Potentially can be moved to common.
 */
@Slf4j
public class CleanupHelper implements AutoCloseable {
    /**
     * List of objects to close.
     */
    private final ArrayList<AutoCloseable> cleanupList = new ArrayList<>();

    /**
     * Adds given {@link AutoCloseable} instance to list of objects to clean up.
     *
     * @param toCleanup {@link AutoCloseable} to clean up.
     */
    synchronized public void add(AutoCloseable toCleanup) {
        cleanupList.add(toCleanup);
    }

    @Override
    synchronized public void close() throws Exception {
        for (val toCleanUp : cleanupList) {
            close("CleanupHelper", toCleanUp);
        }
    }

    /**
     * Closes given instance.
     *
     * @param message Message.
     * @param toClose {@link AutoCloseable}
     */
    static void close(String message, AutoCloseable toClose) {
        try {
            if (toClose != null) {
                toClose.close();
            }
        } catch (Exception ex) {
            log.error(message, ex);
        }
    }
}
