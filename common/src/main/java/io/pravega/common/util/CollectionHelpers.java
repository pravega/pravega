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
package io.pravega.common.util;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;

/**
 * Helper methods for collections.
 */
public final class CollectionHelpers {

    /**
     * Performs a binary search on the given sorted list using the given comparator.
     * This method has undefined behavior if the list is not sorted.
     * <p>
     * This method is different than that in java.util.Collections in the following ways:
     * 1. This one searches by a simple comparator, vs the ones in the Collections class which search for a specific element.
     * This one is useful if we don't have an instance of a search object or we want to implement a fuzzy comparison.
     * 2. This one returns -1 if the element is not found. The ones in the Collections class return (-(start+1)), which is
     * the index where the item should be inserted if it were to go in the list.
     *
     * @param list       The list to search on.
     * @param comparator The comparator to use for comparison. Returns -1 if sought item is before the current item,
     *                   +1 if it is after or 0 if an exact match.
     * @param <T>        Type of the elements in the list.
     * @return The index of the sought item, or -1 if not found.
     */
    public static <T> int binarySearch(List<? extends T> list, Function<? super T, Integer> comparator) {
        return binarySearch(list, comparator, false);
    }

    private static <T> int binarySearch(List<? extends T> list, Function<? super T, Integer> comparator, boolean findGreatestLowerBound) {
        int start = 0;
        int end = list.size() - 1;

        while (start <= end) {
            int midIndex = start + end >>> 1;
            T midElement = list.get(midIndex);
            int compareResult = comparator.apply(midElement);
            if (compareResult < 0) {
                end = midIndex - 1;
            } else if (compareResult > 0) {
                if (findGreatestLowerBound) {
                    T next = list.size() > midIndex + 1 ? list.get(midIndex + 1) : null;
                    if (next == null || (comparator.apply(next) < 0)) {
                        return midIndex;
                    }
                }
                start = midIndex + 1;
            } else {
                return midIndex;
            }
        }

        return -1;
    }

    /**
     * Performs a binary search on the given sorted list to find the greatest lower bound for the supplied element.
     * @param list list to search in
     * @param comparator A bifunction comparator that compares elements in list of type T to value of type U
     * @param <T> Type of elements in the list
     * @return returns index of element in the list is greatest lower bound for the given value.
     * If value is not found, this returns -1
     */
    public static <T> int findGreatestLowerBound(final List<? extends T> list, Function<? super T, Integer> comparator) {
        Preconditions.checkArgument(!list.isEmpty(), "supplied list is empty");
        return binarySearch(list, comparator, true);
    }

    /**
     * Returns a new collection which contains all the items in the given collection that are not to be excluded.
     *
     * @param collection The collection to check.
     * @param toExclude  The elements to exclude.
     * @param <T>        Type of elements.
     * @return A new collection containing all items in collection, except those in toExclude.
     */
    public static <T> Collection<T> filterOut(Collection<T> collection, Collection<T> toExclude) {
        return collection.stream().filter(o -> !toExclude.contains(o)).collect(Collectors.toList());
    }

    /**
     * Returns an unmodifiable Set View made up of the given Sets. The returned Set View does not copy any of the data from
     * any of the given Sets, therefore any changes in the two Sets will be reflected in the View.
     *
     * NOTE: The iterator of this SetView will not de-duplicate items, so if the two Sets are not disjoint, the same item
     * may appear multiple times.
     *
     * @param set1 The first Set.
     * @param set2 The second Set.
     * @param <T>  The type of the items in the Sets.
     * @return A new Set View made up of the two Sets.
     */
    public static <T> Set<T> joinSets(Set<T> set1, Set<T> set2) {
        return new NonConvertedSetView<>(set1, set2);
    }

    /**
     * Returns an unmodifiable Set View made up of the given Sets while translating the items into a common type. The returned
     * Set View does not copy any of the data from any of the given Sets, therefore any changes in the two Sets will be
     * reflected in the View.
     *
     * NOTE: The iterator of this SetView will not de-duplicate items, so if the two Sets are not disjoint, of if the converter
     * functions yield the same value for different inputs, the same item may appear multiple times.
     *
     * @param set1         The first Set, which contains items of type Type1.
     * @param converter1   A Function that translates from Type1 to OutputType.
     * @param set2         The second Set, which contains items of type Type2.
     * @param converter2   A Function that translates from Type2 to OutputType.
     * @param <OutputType> The type of the items in the returned Set View.
     * @param <Type1>      The type of the items in Set 1.
     * @param <Type2>      The type of the items in Set 2.
     * @return A new Set View made up of the two Collections, with translation applied.
     */
    public static <OutputType, Type1, Type2> Set<OutputType> joinSets(Set<Type1> set1, Function<Type1, OutputType> converter1,
                                                                      Set<Type2> set2, Function<Type2, OutputType> converter2) {
        return new ConvertedSetView<>(set1, converter1, set2, converter2);
    }

    /**
     * Returns an unmodifiable Collection View made up of the given Collections while translating the items into a common type.
     * The returned Collection View does not copy any of the data from any of the given Collections, therefore any changes
     * in the two Collections will be reflected in the View.
     *
     * @param c1           The first Collection, which contains items of type Type1.
     * @param converter1   A Function that translates from Type1 to OutputType.
     * @param c2           The second Collection, which contains items of type Type2.
     * @param converter2   A Function that translates from Type2 to OutputType.
     * @param <OutputType> The type of the items in the returned Set View.
     * @param <Type1>      The type of the items in Set 1.
     * @param <Type2>      The type of the items in Set 2.
     * @return A new Collection View made up of the two Collections, with translation applied.
     */
    public static <OutputType, Type1, Type2> Collection<OutputType> joinCollections(Collection<Type1> c1, Function<Type1, OutputType> converter1,
                                                                                    Collection<Type2> c2, Function<Type2, OutputType> converter2) {
        return new ConvertedSetView<>(c1, converter1, c2, converter2);
    }

    //region SetView

    /**
     * Base class for joining two Collections.
     */
    private static abstract class SetView<OutputType, Type1, Type2> extends AbstractSet<OutputType> {
        protected final Collection<Type1> set1;
        protected final Collection<Type2> set2;

        SetView(Collection<Type1> set1, Collection<Type2> set2) {
            this.set1 = Preconditions.checkNotNull(set1, "set1");
            this.set2 = Preconditions.checkNotNull(set2, "set2");
        }

        @Override
        public int size() {
            return this.set1.size() + this.set2.size();
        }

        @Override
        public boolean isEmpty() {
            return this.set1.isEmpty() && this.set2.isEmpty();
        }

        @Override
        public boolean add(OutputType t) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean remove(Object o) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean addAll(Collection<? extends OutputType> collection) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean retainAll(Collection<?> collection) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean removeAll(Collection<?> collection) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void clear() {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Joins two Collections containing items of different types into a Set View containing items of a third type.
     */
    private static class ConvertedSetView<OutputType, Type1, Type2> extends SetView<OutputType, Type1, Type2> {
        private final Function<Type1, OutputType> converter1;
        private final Function<Type2, OutputType> converter2;

        ConvertedSetView(Collection<Type1> set1, Function<Type1, OutputType> converter1, Collection<Type2> set2, Function<Type2, OutputType> converter2) {
            super(set1, set2);
            this.converter1 = Preconditions.checkNotNull(converter1, "converter1");
            this.converter2 = Preconditions.checkNotNull(converter2, "converter2");
        }

        @Override
        public boolean contains(Object o) {
            return Iterators.contains(iterator(), o);
        }

        @Override
        public Iterator<OutputType> iterator() {
            return Iterators.unmodifiableIterator(Iterators.concat(
                    new ConvertedIterator<>(this.set1.iterator(), this.converter1),
                    new ConvertedIterator<>(this.set2.iterator(), this.converter2)));
        }
    }

    /**
     * Joins two Collections into a Set View.
     */
    private static class NonConvertedSetView<T> extends SetView<T, T, T> {
        NonConvertedSetView(Collection<T> set1, Collection<T> set2) {
            super(set1, set2);
        }

        @Override
        public boolean contains(Object o) {
            return this.set1.contains(o) || this.set2.contains(o);
        }

        @Override
        public Iterator<T> iterator() {
            return Iterators.unmodifiableIterator(Iterators.concat(this.set1.iterator(), this.set2.iterator()));
        }
    }

    /**
     * Helps convert an Iterator with elements of one type to an Iterator with elements of a different type.
     */
    @RequiredArgsConstructor
    private static class ConvertedIterator<InputType, OutputType> implements Iterator<OutputType> {
        private final Iterator<InputType> iterator;
        private final Function<InputType, OutputType> converter;

        @Override
        public final boolean hasNext() {
            return this.iterator.hasNext();
        }

        @Override
        public final OutputType next() {
            return this.converter.apply(this.iterator.next());
        }

        @Override
        public final void remove() {
            this.iterator.remove();
        }
    }

    //endregion
}
