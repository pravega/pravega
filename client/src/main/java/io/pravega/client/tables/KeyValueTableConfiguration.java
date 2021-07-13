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
 * The configuration of a Key-Value Table.
 */
@Beta
@Data
@Builder
public class KeyValueTableConfiguration implements Serializable {
    private static final long serialVersionUID = 1L;
    /**
     * The number of Partitions for a Key-Value Table. This value cannot be adjusted after the Key-Value Table has been
     * created.
     *
     * @param partitionCount The number of Partitions for a Key-Value Table.
     * @return The number of Partitions for a Key-Value Table.
     */
    private final int partitionCount;

    /**
     * The number of bytes for the Primary Key. This value cannot be changed after the Key-Value Table has been created.
     *
     * @param primaryKeyLength The number of bytes for the Primary Key.
     * @return The number of bytes for the Primary Key.
     */
    private final int primaryKeyLength;

    /**
     * The number of bytes for the Secondary Key. This value cannot be changed after the Key-Value Table has been created.
     *
     * @param secondaryKeyLength The number of bytes for the Secondary Key.
     * @return The number of bytes for the Primary Key.
     */
    private final int secondaryKeyLength;

    /**
     * The total number of bytes for the key (includes Primary and Secondary).
     *
     * @return The total key size, in bytes.
     */
    public int getTotalKeyLength() {
        return this.primaryKeyLength + this.secondaryKeyLength;
    }

    @Override
    public String toString() {
        return String.format("Partitions = %s, KeyLength = %s:%s", this.partitionCount, this.primaryKeyLength, this.secondaryKeyLength);
    }

    public static final class KeyValueTableConfigurationBuilder {
        public KeyValueTableConfiguration build() {
            Preconditions.checkArgument(this.partitionCount > 0, "partitionCount must be a positive integer. Given %s.", this.partitionCount);
            Preconditions.checkArgument(this.primaryKeyLength > 0, "primaryKeyLength must be a positive integer. Given %s.", this.primaryKeyLength);
            Preconditions.checkArgument(this.secondaryKeyLength >= 0, "secondaryKeyLength must be a non-negative integer. Given %s.", this.secondaryKeyLength);
            return new KeyValueTableConfiguration(this.partitionCount, this.primaryKeyLength, this.secondaryKeyLength);
        }
    }
}
