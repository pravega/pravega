package io.pravega.client.segment.impl;

import java.util.NavigableMap;

public interface LimitedNavigableMap<K, V>
extends NavigableMap<K, V> {

    /**
     A method allowing to insert the entry conditionally (until the map meets the size limit)
     @param k key
     @param v value
     @return true if the entry was inserted, false otherwise
     */
    boolean putIfNotFull(K k, V v);
}
