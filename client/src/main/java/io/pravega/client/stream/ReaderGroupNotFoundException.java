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

/**
 * ReaderGroupNotFoundException is thrown by {@link io.pravega.client.admin.ReaderGroupManager#getReaderGroup(String)} API
 * when the provided reader group does not exist.
 */
public class ReaderGroupNotFoundException extends IllegalArgumentException {
    private static final long serialVersionUID = 1L;
    public ReaderGroupNotFoundException(String readerGroupScopedName) {
        super(String.format("Reader Group %s not found.", readerGroupScopedName));
    }

    public ReaderGroupNotFoundException(String readerGroupScopedName, Throwable e) {
        super(String.format("Reader Group %s not found.", readerGroupScopedName), e);
    }
}
