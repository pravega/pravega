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
package io.pravega.segmentstore.contracts;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterators;
import java.util.AbstractCollection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import lombok.val;

/**
 * A Collection of {@link AttributeUpdate} instances.
 *
 * Implements a unified {@link Collection} with all added {@link AttributeUpdate}, but also separates by {@link AttributeId} type.
 */
public class AttributeUpdateCollection extends AbstractCollection<AttributeUpdate> implements Collection<AttributeUpdate> {
    private final ArrayList<AttributeUpdate> uuidAttributes;
    private final ArrayList<AttributeUpdate> variableAttributes;

    /**
     * Creates a new, empty instance of the {@link AttributeUpdateCollection} class.
     */
    public AttributeUpdateCollection() {
        this.uuidAttributes = new ArrayList<>();
        this.variableAttributes = new ArrayList<>();
    }

    /**
     * Creates a new {@link AttributeUpdateCollection} instance with the given {@link AttributeUpdate}s.
     *
     * @param attributeUpdates The {@link AttributeUpdate}s to include.
     * @return A new {@link AttributeUpdateCollection}.
     */
    public static AttributeUpdateCollection from(Iterable<AttributeUpdate> attributeUpdates) {
        val c = new AttributeUpdateCollection();
        attributeUpdates.forEach(c::add);
        return c;
    }

    /**
     * Creates a new {@link AttributeUpdateCollection} instance with the given {@link AttributeUpdate}s.
     *
     * @param attributeUpdates The {@link AttributeUpdate}s to include.
     * @return A new {@link AttributeUpdateCollection}.
     */
    public static AttributeUpdateCollection from(AttributeUpdate... attributeUpdates) {
        val c = new AttributeUpdateCollection();
        for (val au : attributeUpdates) {
            c.add(au);
        }
        return c;
    }

    /**
     * Gets a value indicating whether this {@link AttributeUpdateCollection} has any {@link AttributeUpdate}s that
     * have {@link AttributeId}s of type {@link AttributeId.Variable}.
     *
     * @return True of this instance contains any variable Attribute Ids, false otherwise.
     */
    public boolean hasVariableAttributeIds() {
        return this.variableAttributes.size() > 0;
    }

    /**
     * Gets an unmodifiable collection of {@link AttributeUpdate}s that have have {@link AttributeId}s of type
     * {@link AttributeId.Variable}.
     *
     * @return The {@link AttributeUpdate}s with variable Attribute Ids.
     */
    public Collection<AttributeUpdate> getVariableAttributeUpdates() {
        return Collections.unmodifiableCollection(this.variableAttributes);
    }

    /**
     * Gets an unmodifiable collection of {@link AttributeUpdate}s that have have {@link AttributeId}s of type
     * {@link AttributeId.UUID}.
     *
     * @return The {@link AttributeUpdate}s with UUID Attribute Ids.
     */
    public Collection<AttributeUpdate> getUUIDAttributeUpdates() {
        return Collections.unmodifiableCollection(this.uuidAttributes);
    }

    @Override
    public boolean add(AttributeUpdate au) {
        if (au.getAttributeId().isUUID()) {
            this.uuidAttributes.add(au);
        } else {
            this.variableAttributes.add(au);
        }

        return true;
    }

    @Override
    public Iterator<AttributeUpdate> iterator() {
        return Iterators.concat(this.uuidAttributes.iterator(), this.variableAttributes.iterator());
    }

    @Override
    public int size() {
        return this.uuidAttributes.size() + this.variableAttributes.size();
    }

    @Override
    @VisibleForTesting
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        } else if (!(o instanceof Collection)) {
            return false;
        } else {
            val other = (Collection<?>) o;
            if (this.size() != other.size()) {
                return false;
            }
            val thisIterator = iterator();
            val otherIterator = other.iterator();
            while (thisIterator.hasNext() && otherIterator.hasNext()) {
                if (!thisIterator.next().equals(otherIterator.next())) {
                    return false;
                }
            }

            return !thisIterator.hasNext() && !otherIterator.hasNext();
        }
    }

    @Override
    @VisibleForTesting
    public int hashCode() {
        int hashCode = 1;
        for (val e : this) {
            hashCode = 31 * hashCode + (e == null ? 0 : e.hashCode());
        }
        return hashCode;
    }
}
