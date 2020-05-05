/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.tables.impl;

import com.google.common.base.Functions;
import com.google.common.collect.Iterators;
import io.pravega.client.tables.BadKeyVersionException;
import io.pravega.client.tables.ConditionalTableUpdateException;
import io.pravega.client.tables.MapWrapper;
import io.pravega.client.tables.TableEntry;
import io.pravega.client.tables.TableKey;
import io.pravega.client.tables.Version;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.Retry;
import java.util.AbstractCollection;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;

/**
 * Implementation of {@link MapWrapper}.
 *
 * @param <KeyT>   Key Type.
 * @param <ValueT> Value Type.
 */
@RequiredArgsConstructor
@SuppressWarnings({"unchecked", "NullableProblems"})
final class MapWrapperImpl<KeyT, ValueT> implements MapWrapper<KeyT, ValueT> {
    private static final int ITERATOR_BATCH_SIZE = 100;
    private static final Retry.RetryAndThrowExceptionally<ConditionalTableUpdateException, RuntimeException> RETRY = Retry
            .withExpBackoff(10, 4, 10, 30000)
            .retryingOn(ConditionalTableUpdateException.class)
            .throwingOn(RuntimeException.class);
    @NonNull
    private final KeyValueTableImpl<KeyT, ValueT> kvt;
    private final String keyFamily;

    //region Map Implementation

    @Override
    public boolean containsKey(@NonNull Object key) {
        val e = this.kvt.get(this.keyFamily, (KeyT) key).join();
        return e != null;
    }

    @Override
    public boolean containsValue(@NonNull Object o) {
        ValueT value = (ValueT) o;
        return entryStream().anyMatch(e -> areSame(e.getValue(), value));
    }

    @Override
    public ValueT get(@NonNull Object key) {
        val e = this.kvt.get(this.keyFamily, (KeyT) key).join();
        return e == null ? null : e.getValue();
    }

    @Override
    public ValueT getOrDefault(@NonNull Object key, ValueT defaultValue) {
        ValueT value = get(key);
        return value == null ? defaultValue : value;
    }

    @Override
    public ValueT put(@NonNull KeyT key, @NonNull ValueT value) {
        val oldValue = new AtomicReference<ValueT>();
        this.kvt.get(this.keyFamily, key)
                .thenCompose(existingEntry -> {
                    oldValue.set(existingEntry == null ? null : existingEntry.getValue());
                    return this.kvt.put(this.keyFamily, key, value);
                })
                .join();
        return oldValue.get();
    }

    private void putDirect(@NonNull KeyT key, @NonNull ValueT value) {
        this.kvt.put(this.keyFamily, key, value).join();
    }

    @Override
    public void putAll(@NonNull Map<? extends KeyT, ? extends ValueT> map) {
        requiresKeyFamily("putAll");

        // Need to go through a few hoops to get rid of the class capture that we were forced into with the Map interface.
        val iterator = Iterators.transform(map.entrySet().iterator(), e -> (Map.Entry<KeyT, ValueT>) e);
        this.kvt.putAll(this.keyFamily, iterator).join();
    }

    @Override
    public ValueT putIfAbsent(@NonNull KeyT key, @NonNull ValueT value) {
        return Futures.exceptionallyComposeExpecting(
                this.kvt.putIfAbsent(this.keyFamily, key, value).thenApply(version -> value),
                ex -> ex instanceof BadKeyVersionException,
                () -> this.kvt.get(this.keyFamily, key).thenApply(TableEntry::getValue))
                .join();
    }

    @Override
    public void replaceAll(@NonNull BiFunction<? super KeyT, ? super ValueT, ? extends ValueT> convert) {
        requiresKeyFamily("replaceAll");
        val baseIterator = kvt.entryIterator(keyFamily, ITERATOR_BATCH_SIZE, null).asIterator();
        val updateFutures = new ArrayList<CompletableFuture<List<Version>>>();
        while (baseIterator.hasNext()) {
            val toUpdate = baseIterator.next().getItems().stream()
                    .map(e -> (Map.Entry<KeyT, ValueT>) new AbstractMap.SimpleImmutableEntry<KeyT, ValueT>(
                            e.getKey().getKey(),
                            convert.apply(e.getKey().getKey(), e.getValue())))
                    .iterator();
            if (toUpdate.hasNext()) {
                updateFutures.add(kvt.putAll(keyFamily, toUpdate));
            }
        }

        if (updateFutures.size() > 0) {
            Futures.allOf(updateFutures).join();
        }
    }

    @Override
    public ValueT remove(@NonNull Object key) {
        KeyT k = (KeyT) key;
        val existingValue = new AtomicReference<ValueT>();
        RETRY.run(
                () -> this.kvt.get(this.keyFamily, k)
                        .thenCompose(e -> {
                            if (e == null) {
                                existingValue.set(null);
                                return this.kvt.remove(this.keyFamily, k);
                            } else {
                                existingValue.set(e.getValue());
                                return this.kvt.remove(this.keyFamily, k, e.getKey().getVersion());
                            }
                        }).join());
        return existingValue.get();
    }

    @Override
    public boolean remove(@NonNull Object key, Object expectedValue) {
        KeyT k = (KeyT) key;
        ValueT ev = (ValueT) expectedValue;
        return RETRY.run(
                () -> this.kvt.get(this.keyFamily, k)
                        .thenCompose(e -> {
                            if (e != null && areSame(ev, e.getValue())) {
                                return this.kvt.remove(this.keyFamily, k, e.getKey().getVersion()).thenApply(v -> true);
                            } else {
                                return CompletableFuture.completedFuture(false);
                            }
                        }).join());
    }

    private void removeDirect(KeyT key) {
        this.kvt.remove(this.keyFamily, key).join();
    }

    @Override
    public boolean replace(@NonNull KeyT key, @NonNull ValueT expectedValue, @NonNull ValueT newValue) {
        return RETRY.run(
                () -> this.kvt.get(this.keyFamily, key)
                        .thenCompose(e -> {
                            if (e != null && areSame(expectedValue, e.getValue())) {
                                return this.kvt.replace(this.keyFamily, key, newValue, e.getKey().getVersion())
                                        .thenApply(v -> true);
                            } else {
                                return CompletableFuture.completedFuture(false);
                            }
                        }).join());
    }

    @Override
    public ValueT replace(@NonNull KeyT key, @NonNull ValueT value) {
        return RETRY.run(
                () -> this.kvt.get(this.keyFamily, key)
                        .thenCompose(e -> {
                            if (e != null) {
                                return this.kvt.replace(this.keyFamily, key, value, e.getKey().getVersion())
                                        .thenApply(v -> e.getValue());
                            } else {
                                return CompletableFuture.completedFuture(null);
                            }
                        }).join());
    }

    @Override
    public ValueT compute(@NonNull KeyT key, @NonNull BiFunction<? super KeyT, ? super ValueT, ? extends ValueT> toCompute) {
        ValueT existingValue = get(key);
        ValueT newValue = toCompute.apply(key, existingValue);
        if (newValue == null) {
            if (existingValue != null) {
                removeDirect(key);
            }

            return null;
        } else {
            putDirect(key, newValue);
            return newValue;
        }
    }

    @Override
    public KeySet<KeyT> keySet() {
        requiresKeyFamily("keySet");
        return new KeySetImpl();
    }

    @Override
    public ValuesCollection<ValueT> values() {
        requiresKeyFamily("values");
        return new ValuesCollectionImpl();
    }

    @Override
    public EntrySet<KeyT, ValueT> entrySet() {
        requiresKeyFamily("entrySet");
        return new EntrySetImpl();
    }

    @Override
    public int size() {
        requiresKeyFamily("size");
        long size = keyStream().count();
        return size > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) size;
    }

    @Override
    public boolean isEmpty() {
        requiresKeyFamily("isEmpty");
        return !keyStream().findFirst().isPresent();
    }

    @Override
    public void clear() {
        clear(key -> true);
    }

    private boolean clear(Predicate<? super KeyT> keyFilter) {
        requiresKeyFamily("clear");
        val baseIterator = MapWrapperImpl.this.kvt.keyIterator(this.keyFamily, ITERATOR_BATCH_SIZE, null).asIterator();
        val deleteFutures = new ArrayList<CompletableFuture<Void>>();
        while (baseIterator.hasNext()) {
            val toDelete = baseIterator.next().getItems().stream()
                    .filter(k -> keyFilter.test(k.getKey()))
                    .map(k -> TableKey.unversioned(k.getKey()))
                    .collect(Collectors.toList());
            if (toDelete.size() > 0) {
                deleteFutures.add(this.kvt.removeAll(this.keyFamily, toDelete));
            }
        }

        if (deleteFutures.size() > 0) {
            Futures.allOf(deleteFutures).join();
            return true;
        }

        return false;
    }

    //endregion

    //region Helpers

    private void requiresKeyFamily(String opName) {
        if (this.keyFamily == null) {
            throw new UnsupportedOperationException(opName + "() requires a Key Family.");
        }
    }

    private boolean areSame(ValueT expected, ValueT actual) {
        return expected == null && actual == null || Objects.equals(expected, actual);
    }

    private boolean areSame(List<TableEntry<KeyT, ValueT>> expected, List<ValueT> actual) {
        assert expected.size() == actual.size();
        for (int i = 0; i < expected.size(); i++) {
            if (!areSame(actual.get(i), expected.get(i).getValue())) {
                return false;
            }
        }

        return true;
    }

    private Stream<TableKey<KeyT>> keyStream() {
        val baseIterator = MapWrapperImpl.this.kvt.keyIterator(MapWrapperImpl.this.keyFamily, ITERATOR_BATCH_SIZE, null).asIterator();
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(baseIterator, 0), false)
                .flatMap(iteratorItem -> iteratorItem.getItems().stream());
    }

    private Stream<TableEntry<KeyT, ValueT>> entryStream() {
        val baseIterator = MapWrapperImpl.this.kvt.entryIterator(MapWrapperImpl.this.keyFamily, ITERATOR_BATCH_SIZE, null).asIterator();
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(baseIterator, 0), false)
                .flatMap(iteratorItem -> iteratorItem.getItems().stream());
    }

    private Map.Entry<KeyT, ValueT> toMapEntry(TableEntry<KeyT, ValueT> e) {
        return new AbstractMap.SimpleImmutableEntry<>(e.getKey().getKey(), e.getValue());
    }

    //endregion

    //region BaseCollection

    private abstract class BaseCollection<T> extends AbstractCollection<T> implements Collection<T> {
        @Override
        public void clear() {
            MapWrapperImpl.this.clear();
        }

        @Override
        public int size() {
            return MapWrapperImpl.this.size();
        }

        @Override
        public boolean isEmpty() {
            return MapWrapperImpl.this.isEmpty();
        }

        @Override
        public Iterator<T> iterator() {
            return stream().iterator();
        }

        @Override
        public Spliterator<T> spliterator() {
            return stream().spliterator();
        }

        @Override
        public abstract Stream<T> stream();

        @Override
        public Object[] toArray() {
            return stream().toArray();
        }

        @Override
        public <V> V[] toArray(V[] ts) {
            throw new UnsupportedOperationException("toArray(T[])");
        }

        @Override
        public boolean retainAll(Collection<?> collection) {
            val toKeep = collection instanceof Set ? (Set) collection : new HashSet(collection);
            return removeIf(item -> !toKeep.contains(item));
        }

        @Override
        public String toString() {
            // AbstractSet.toString() invokes the iterator, so we shouldn't be using it.
            return this.getClass().getName();
        }

        @Override
        public boolean equals(Object o) {
            throw new UnsupportedOperationException("equals()");
        }

        @Override
        public int hashCode() {
            throw new UnsupportedOperationException("hashCode()");
        }
    }

    //endregion

    //region KeySet Implementation

    private class KeySetImpl extends BaseCollection<KeyT> implements KeySet<KeyT> {
        @Override
        public boolean contains(Object key) {
            return MapWrapperImpl.this.containsKey(key);
        }

        @Override
        public boolean remove(Object key) {
            return MapWrapperImpl.this.remove(key) != null;
        }

        @Override
        public boolean containsAll(Collection<?> keyCollection) {
            val keys = toKeys(keyCollection, Functions.identity());
            val existingEntries = MapWrapperImpl.this.kvt.getAll(MapWrapperImpl.this.keyFamily, keys).join();
            return existingEntries.stream().allMatch(Objects::nonNull);
        }

        @Override
        public boolean removeAll(Collection<?> keyCollection) {
            val keys = toKeys(keyCollection, TableKey::unversioned);
            MapWrapperImpl.this.kvt.removeAll(MapWrapperImpl.this.keyFamily, keys).join();
            return true;
        }

        @Override
        public boolean removeIf(Predicate<? super KeyT> filter) {
            return MapWrapperImpl.this.clear(filter);
        }

        @Override
        public Stream<KeyT> stream() {
            return MapWrapperImpl.this.keyStream().map(TableKey::getKey);
        }

        private <T> List<T> toKeys(Collection<?> keyCollection, Function<KeyT, T> converter) {
            T[] keyArray = (T[]) new Object[keyCollection.size()];
            int index = 0;
            for (Object o : keyCollection) {
                keyArray[index++] = converter.apply((KeyT) o);
            }
            return Arrays.asList(keyArray);
        }
    }

    //endregion

    //region ValuesCollection Implementation

    private class ValuesCollectionImpl extends BaseCollection<ValueT> implements ValuesCollection<ValueT> {
        @Override
        public boolean contains(@NonNull Object o) {
            ValueT value = (ValueT) o;
            return stream().anyMatch(v -> areSame(value, v));
        }

        @Override
        public boolean remove(Object o) {
            ValueT value = (ValueT) o;
            return removeIf(v -> areSame(value, v));
        }

        @Override
        public boolean removeIf(Predicate<? super ValueT> test) {
            val baseIterator = kvt.entryIterator(keyFamily, ITERATOR_BATCH_SIZE, null).asIterator();
            val deleteFutures = new ArrayList<CompletableFuture<Void>>();
            while (baseIterator.hasNext()) {
                val toDelete = baseIterator.next().getItems().stream()
                        .filter(e -> test.test(e.getValue()))
                        .map(TableEntry::getKey)
                        .collect(Collectors.toList());
                if (toDelete.size() > 0) {
                    deleteFutures.add(kvt.removeAll(keyFamily, toDelete));
                }
            }

            if (deleteFutures.size() > 0) {
                Futures.allOf(deleteFutures).join();
                return true;
            }

            return false;
        }

        @Override
        public boolean removeAll(Collection<?> collection) {
            val toRemove = collection instanceof Set ? (Set) collection : new HashSet(collection);
            return removeIf(toRemove::contains);
        }

        @Override
        public boolean containsAll(Collection<?> collection) {
            val valuesToCheck = collection instanceof Set ? (Set) collection : new HashSet(collection);
            val existingValues = new HashSet<>();
            val baseIterator = kvt.entryIterator(keyFamily, ITERATOR_BATCH_SIZE, null).asIterator();
            while (existingValues.size() < valuesToCheck.size() && baseIterator.hasNext()) {
                baseIterator.next().getItems().stream()
                        .filter(e -> valuesToCheck.contains(e.getValue()))
                        .forEach(existingValues::add);
            }

            return existingValues.size() >= valuesToCheck.size();
        }

        @Override
        public Stream<ValueT> stream() {
            return entryStream().map(TableEntry::getValue);
        }
    }

    //endregion

    //region EntrySet Implementation

    private class EntrySetImpl extends BaseCollection<Entry<KeyT, ValueT>> implements EntrySet<KeyT, ValueT> {
        @Override
        public boolean contains(@NonNull Object o) {
            if (o instanceof Map.Entry) {
                Map.Entry<KeyT, ValueT> e = (Map.Entry<KeyT, ValueT>) o;
                ValueT value = MapWrapperImpl.this.get(e.getKey());
                return areSame(e.getValue(), value);
            }
            return false;
        }

        @Override
        public boolean containsAll(@NonNull Collection<?> collection) {
            val keys = new ArrayList<KeyT>(collection.size());
            val values = new ArrayList<ValueT>(collection.size());
            for (Object o : collection) {
                if (!(o instanceof Map.Entry)) {
                    return false;
                }
                Map.Entry<KeyT, ValueT> e = (Map.Entry<KeyT, ValueT>) o;
                keys.add(e.getKey());
                values.add(e.getValue());
            }

            val existingEntries = MapWrapperImpl.this.kvt.getAll(MapWrapperImpl.this.keyFamily, keys).join();
            return areSame(existingEntries, values);
        }

        @Override
        public boolean add(@NonNull Entry<KeyT, ValueT> e) {
            ValueT finalValue = putIfAbsent(e.getKey(), e.getValue());
            return areSame(e.getValue(), finalValue);
        }

        @Override
        public boolean addAll(@NonNull Collection<? extends Entry<KeyT, ValueT>> collection) {
            MapWrapperImpl.this.kvt.putAll(MapWrapperImpl.this.keyFamily, Collections.unmodifiableCollection(collection)).join();
            return true;
        }

        @Override
        public boolean remove(Object o) {
            Map.Entry<KeyT, ValueT> e = (Map.Entry<KeyT, ValueT>) o;
            return MapWrapperImpl.this.remove(e.getKey(), e.getValue());
        }

        @Override
        public boolean removeAll(Collection<?> collection) {
            val keys = new ArrayList<KeyT>(collection.size());
            val values = new ArrayList<ValueT>(collection.size());
            for (Object o : collection) {
                Map.Entry<KeyT, ValueT> e = (Map.Entry<KeyT, ValueT>) o;
                keys.add(e.getKey());
                values.add(e.getValue());
            }
            return MapWrapperImpl.this.kvt.getAll(MapWrapperImpl.this.keyFamily, keys)
                    .thenCompose(existingValues -> {
                        if (areSame(existingValues, values)) {
                            val toRemove = existingValues.stream().map(TableEntry::getKey).collect(Collectors.toList());
                            return MapWrapperImpl.this.kvt.removeAll(MapWrapperImpl.this.keyFamily, toRemove).thenApply(v -> true);
                        } else {
                            return CompletableFuture.completedFuture(false);
                        }
                    }).join();
        }

        @Override
        public boolean removeIf(Predicate<? super Entry<KeyT, ValueT>> filter) {
            val baseIterator = kvt.entryIterator(keyFamily, ITERATOR_BATCH_SIZE, null).asIterator();
            val deleteFutures = new ArrayList<CompletableFuture<Void>>();
            while (baseIterator.hasNext()) {
                val toDelete = baseIterator.next().getItems().stream()
                        .map(MapWrapperImpl.this::toMapEntry)
                        .filter(filter)
                        .map(e -> TableKey.unversioned(e.getKey()))
                        .collect(Collectors.toList());
                if (toDelete.size() > 0) {
                    deleteFutures.add(kvt.removeAll(keyFamily, toDelete));
                }
            }

            if (deleteFutures.size() > 0) {
                Futures.allOf(deleteFutures).join();
                return true;
            }

            return false;
        }

        @Override
        public Stream<Entry<KeyT, ValueT>> stream() {
            return MapWrapperImpl.this.entryStream().map(MapWrapperImpl.this::toMapEntry);
        }
    }

    //endregion
}
