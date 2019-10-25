/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.segment.impl;

import java.util.NavigableMap;

public interface LimitedNavigableMap<K, V>
extends NavigableMap<K, V> {

    /**
     A method allowing to insert the entry conditionally (until the map meets the size limit).
     @param k key
     @param v value
     @return true if the entry was inserted, false otherwise
     */
    boolean putIfNotFull(K k, V v);
}
