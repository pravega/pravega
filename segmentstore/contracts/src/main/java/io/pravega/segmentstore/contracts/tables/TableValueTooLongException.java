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

/**
 * Exception that is thrown whenever a Table Entry's value exceeds the maximum allowed length.
 */
public class TableValueTooLongException extends IllegalArgumentException {
    private static final long serialVersionUID = 1L;

    /**
     * Creates a new instance of the TableValueTooLongException class.
     *
     * @param entry         The TableEntry.
     * @param maximumLength The maximum allowed value length.
     */
    public TableValueTooLongException(TableEntry entry, int maximumLength) {
        super(String.format("Table Value too long. Maximum length: %s, given: %s.", entry.getValue().getLength(), maximumLength));
    }
}
