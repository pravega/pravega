/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.tables;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;
import lombok.NonNull;

/**
 * TODO javadoc extensively in this file.
 * @param <KeyT>
 * @param <ValueT>
 */
@SuppressWarnings("NullableProblems")
public interface MapWrapper<KeyT, ValueT> extends Map<KeyT, ValueT> {

    @Override
    boolean containsKey(@NonNull Object key);

    @Override
    boolean containsValue(@NonNull Object o);

    @Override
    ValueT get(@NonNull Object key);

    @Override
    ValueT getOrDefault(@NonNull Object key, ValueT defaultValue);

    @Override
    ValueT put(@NonNull KeyT key, @NonNull ValueT value);

    void putDirect(@NonNull KeyT key, @NonNull ValueT value);

    @Override
    ValueT putIfAbsent(@NonNull KeyT key, @NonNull ValueT value);

    @Override
    void putAll(@NonNull Map<? extends KeyT, ? extends ValueT> map);

    @Override
    boolean replace(@NonNull KeyT key, @NonNull ValueT expectedValue, @NonNull ValueT newValue);

    @Override
    ValueT replace(@NonNull KeyT key, @NonNull ValueT value);

    @Override
    void replaceAll(@NonNull BiFunction<? super KeyT, ? super ValueT, ? extends ValueT> convert);

    @Override
    ValueT remove(@NonNull Object key);

    @Override
    boolean remove(@NonNull Object key, Object expectedValue);

    void removeDirect(@NonNull KeyT key);

    @Override
    ValueT compute(@NonNull KeyT key, @NonNull BiFunction<? super KeyT, ? super ValueT, ? extends ValueT> toCompute);

    @Override
    ValueT computeIfPresent(@NonNull KeyT key, BiFunction<? super KeyT, ? super ValueT, ? extends ValueT> toCompute);

    @Override
    ValueT computeIfAbsent(@NonNull KeyT key, Function<? super KeyT, ? extends ValueT> toCompute);

    @Override
    KeySet<KeyT> keySet();

    @Override
    ValuesCollection<ValueT> values();

    @Override
    EntrySet<KeyT, ValueT> entrySet();

    @Override
    int size();

    @Override
    boolean isEmpty();

    @Override
    void clear();

    interface KeySet<KeyT> extends Set<KeyT> {
        @Override
        boolean contains(Object key);

        @Override
        boolean containsAll(Collection<?> keyCollection);

        @Override
        boolean remove(Object key);

        @Override
        boolean removeIf(Predicate<? super KeyT> filter);

        @Override
        boolean removeAll(Collection<?> keyCollection);

        @Override
        boolean retainAll(Collection<?> collection);

        @Override
        void clear();

        @Override
        int size();

        @Override
        boolean isEmpty();

        @Override
        Iterator<KeyT> iterator();

        @Override
        Stream<KeyT> stream();

        @Override
        Object[] toArray();
    }

    interface EntrySet<KeyT, ValueT> extends Set<Entry<KeyT, ValueT>> {
        @Override
        boolean contains(@NonNull Object o);

        @Override
        boolean containsAll(@NonNull Collection<?> collection);

        @Override
        boolean add(@NonNull Entry<KeyT, ValueT> e);

        @Override
        boolean addAll(@NonNull Collection<? extends Entry<KeyT, ValueT>> collection);

        @Override
        boolean remove(Object o);

        @Override
        boolean removeIf(Predicate<? super Entry<KeyT, ValueT>> filter);

        @Override
        boolean removeAll(Collection<?> collection);

        @Override
        void clear();

        @Override
        int size();

        @Override
        boolean isEmpty();

        @Override
        Iterator<Entry<KeyT, ValueT>> iterator();

        @Override
        Stream<Entry<KeyT, ValueT>> stream();

        @Override
        Object[] toArray();
    }

    interface ValuesCollection<ValueT> extends Collection<ValueT> {
        @Override
        boolean contains(@NonNull Object o);

        @Override
        boolean remove(Object o);

        @Override
        boolean removeAll(Collection<?> collection);

        @Override
        boolean removeIf(Predicate<? super ValueT> test);

        @Override
        boolean containsAll(Collection<?> collection);

        @Override
        void clear();

        @Override
        int size();

        @Override
        boolean isEmpty();

        @Override
        Iterator<ValueT> iterator();

        @Override
        Stream<ValueT> stream();

        @Override
        Object[] toArray();
    }
}
