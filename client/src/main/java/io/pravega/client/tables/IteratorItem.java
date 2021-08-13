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
import io.pravega.common.util.AsyncIterator;
import java.util.List;
import lombok.Data;

/**
 * An iteration result item returned by {@link AsyncIterator} when invoking {@link KeyValueTable#iterator()}.
 *
 * @param <T> Iterator Item type.
 */
@Data
@Beta
public class IteratorItem<T> {
    /**
     * A List of items that are contained in this instance.
     *
     * @param items List of items that are contained in this instance.
     * @return List of items that are contained in this instance.
     */
    private final List<T> items;
}
