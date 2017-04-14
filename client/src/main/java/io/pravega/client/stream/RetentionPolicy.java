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
package io.pravega.client.stream;

import java.io.Serializable;
import java.time.Duration;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Data;
import lombok.RequiredArgsConstructor;

@Data
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
@Builder
public class RetentionPolicy implements Serializable {
    private static final long serialVersionUID = 1L;

    public enum Type {
        /**
         * Set retention based on how long data has been in the stream.
         */
        LIMITED_DAYS,
        /**
         * Set retention based on the total size of the data in the stream.
         */
        LIMITED_SIZE_MB,
        /**
         * Set retention to infinite.
         */
        INFINITE
    }

    private final Type type;
    private final long value;

    public static RetentionPolicy byDays(Long days) {
        return new RetentionPolicy(Type.LIMITED_DAYS, days);
    }

    public static RetentionPolicy bySizeMB(long size) {
        return new RetentionPolicy(Type.LIMITED_SIZE_MB, size);
    }

    public static RetentionPolicy infinte() {
        return new RetentionPolicy(Type.INFINITE, 0);
    }
}
