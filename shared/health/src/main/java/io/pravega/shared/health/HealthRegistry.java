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
package io.pravega.shared.health;

/**
 * The {@link HealthRegistry} interface defines the necessary operations required to store and retrieve objects that are
 * uniquely identified by some id. While conceptually similar to a {@link java.util.Map}, the interface allows us to
 * implement pre/post processing logic of entries.
 *
 * @param <T> The type of objects held.
 */
public interface HealthRegistry<T> {
    /**
     * Register some object of type 'T' to the registry.
     *
     * @param object The object to register.
     * @return The object registered.
     */
    T register(T object);

    /**
     * Unregister said object from this {@link HealthRegistry}.
     *
     * @param object The object to remove.
     * @return The object removed.
     */
    T unregister(T object);

    /**
     * Clears all registered entries from the underlying store.
     */
    void clear();

    /**
     * Returns the object associated with the identifier. A key of type {@link String} is used, therefore we should
     * protect against the case where an no item of type *T* mapped by 'id' exists.
     * @param id The identifier used to query the underlying store.
     * @return The object of type *T* associated with  'id'.
     */
    T get(String id);
}
