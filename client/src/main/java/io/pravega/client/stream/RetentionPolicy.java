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

import java.io.Serializable;
import java.time.Duration;

import lombok.AccessLevel;
import lombok.Builder;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

@Data
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
@Builder
@ToString
public class RetentionPolicy implements Serializable {
    private static final long serialVersionUID = 1L;

    public enum RetentionType {
        /**
         * Set retention based on how long data has been in the stream in milliseconds.
         */
        TIME,

        /**
         * Set retention based on the total size of the data in the stream in bytes.
         */
        SIZE
    }

    private final RetentionType retentionType;
    private final long retentionParam;
    private final long retentionMax;

    /**
     * Create a retention policy to configure a stream to be periodically truncated according to the specified duration.
     *
     * @param duration Period to retain data in a stream.
     * @return Retention policy object.
     */
    public static RetentionPolicy byTime(Duration duration) {
        return RetentionPolicy.builder().retentionType(RetentionType.TIME)
                .retentionParam(duration.toMillis()).retentionMax(Long.MAX_VALUE).build();
    }

    /**
     * Create a retention policy to configure a stream to periodically truncated
     * according to the specified duration.
     *
     * @param durationMin Minimum period for which data would be retained in the stream.
     * @param durationMax Maximum period for which data would be retained in the stream.
     * @return Retention policy object.
     */
    public static RetentionPolicy byTime(Duration durationMin, Duration durationMax) {
        return RetentionPolicy.builder().retentionType(RetentionType.TIME)
                .retentionParam(durationMin.toMillis()).retentionMax(durationMax.toMillis()).build();
    }

    /**
     * Create a retention policy to configure a stream to truncate a stream
     * according to the amount of data currently stored.
     *
     * @param size Amount of data to retain in a stream.
     * @return Retention policy object.
     */
    public static RetentionPolicy bySizeBytes(long size) {
        return RetentionPolicy.builder().retentionType(RetentionType.SIZE)
                .retentionParam(size).retentionMax(Long.MAX_VALUE).build();
    }

    /**
     * Create a retention policy to configure a stream to truncate a stream
     * according to the amount of data currently stored.
     *
     * @param sizeMin Minimum amount of data to retain in a Stream.
     * @param sizeMax Maximum amount of data to retain in a Stream.
     * @return Retention policy object.
     */
    public static RetentionPolicy bySizeBytes(long sizeMin, long sizeMax) {
        return RetentionPolicy.builder().retentionType(RetentionType.SIZE)
                .retentionParam(sizeMin).retentionMax(sizeMax).build();
    }
}
