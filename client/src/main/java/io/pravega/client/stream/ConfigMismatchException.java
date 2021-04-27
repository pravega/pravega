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
 * ConfigMismatchException is thrown by the {@link io.pravega.client.admin.ReaderGroupManager#createReaderGroup(String, ReaderGroupConfig)} API
 * when the reader group already exists with a different configuration.
 */
public class ConfigMismatchException extends IllegalArgumentException {
    private static final long serialVersionUID = 1L;

    public ConfigMismatchException(String readerGroupScopedName, ReaderGroupConfig currentConfig) {
        super(String.format("Reader Group %s already exists with a different configuration %s.", readerGroupScopedName, currentConfig));
    }
}
