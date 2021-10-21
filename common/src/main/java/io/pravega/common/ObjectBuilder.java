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
package io.pravega.common;

/**
 * Defines an object that can build other objects. Usually this is a shadow object that accumulates all the data for an
 * immutable object, and it will instantiate such object with the accumulated data.
 *
 * @param <T> Type of the object to build.
 */
public interface ObjectBuilder<T> {
    /**
     * Creates a new instance of the T type (usually with the accumulated data so far).
     *
     * @return A new instance.
     */
    T build();
}
