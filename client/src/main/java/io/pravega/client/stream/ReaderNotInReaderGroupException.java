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
package io.pravega.client.stream;

import lombok.Getter;

/**
 * Read was invoked on a reader that the reader group does not consider a member.
 * This likely means the reader was declared dead by invoking {@link ReaderGroup#readerOffline(String, Position)}
 */
public class ReaderNotInReaderGroupException extends Exception {

    private static final long serialVersionUID = 1L;

    @Getter
    private final String readerId;
 
    public ReaderNotInReaderGroupException(String readerId) {
        super("Reader: " + readerId);
        this.readerId = readerId;
    }
}
