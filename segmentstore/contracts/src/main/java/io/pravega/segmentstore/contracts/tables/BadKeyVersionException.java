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
package io.pravega.segmentstore.contracts.tables;

import java.util.Map;
import java.util.stream.Collectors;
import lombok.Getter;

/**
 * Exception that is thrown whenever a Conditional Update to a Table failed due to an existing Key that has a different
 * version than provided.
 */
public class BadKeyVersionException extends ConditionalTableUpdateException {
    private static final long serialVersionUID = 1L;
    @Getter
    private final Map<TableKey, Long> expectedVersions;

    /**
     * Creates a new instance of the BadKeyVersionException class.
     *
     * @param segmentName     The name of the affected Table Segment.
     * @param expectedVersions A Map Of {@link TableKey} to Longs representing the keys to expected versions.
     */
    public BadKeyVersionException(String segmentName, Map<TableKey, Long> expectedVersions) {
        super(segmentName, String.format("Version mismatch for %s key(s): %s.", expectedVersions.size(), getMessage(expectedVersions)));
        this.expectedVersions = expectedVersions;
    }

    private static String getMessage(Map<TableKey, Long> expectedVersions) {
        return expectedVersions.entrySet().stream()
                               .map(e -> String.format("%s (%s)", e.getValue(), e.getKey().getVersion()))
                               .collect(Collectors.joining(", "));
    }
}
