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
package io.pravega.client.tables;

import com.google.common.annotations.Beta;
import com.google.common.base.Preconditions;
import java.io.Serializable;
import lombok.Builder;
import lombok.Data;

/**
 * Configuration for the {@link KeyValueTable} client.
 */
@Beta
@Data
@Builder
public class KeyValueTableClientConfiguration implements Serializable {
    private static final long serialVersionUID = 1L;
    private final int initialBackoffMillis;
    private final int maxBackoffMillis;
    private final int retryAttempts;
    private final int backoffMultiple;

    public static final class KeyValueTableClientConfigurationBuilder {
        private int initialBackoffMillis = 10;
        private int maxBackoffMillis = 30000;
        private int retryAttempts = 10;
        private int backoffMultiple = 4;

        public KeyValueTableClientConfiguration build() {
            Preconditions.checkArgument(this.initialBackoffMillis >= 0, "Initial backoff must be non-negative number.");
            Preconditions.checkArgument(this.backoffMultiple >= 0, "Backoff multiple must be a non-negative number.");
            Preconditions.checkArgument(this.maxBackoffMillis >= 0, "Max backoff time must be non-negative number.");
            Preconditions.checkArgument(this.retryAttempts > 0, "Retry attempts must be a positive number.");
            return new KeyValueTableClientConfiguration(this.initialBackoffMillis, this.maxBackoffMillis, this.retryAttempts, this.backoffMultiple);
        }
    }
}
