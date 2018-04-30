/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common.util;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import java.util.Map;
import java.util.function.Function;

import static java.util.stream.Collectors.toMap;

public class ToStringUtils {

    /**
     * Transforms a map into a string of the from:
     * "K1=V1, K2=V2, K3=V3"
     * Where the string versions of the key and value are derived from their toString() function.
     * 
     * @param <K> The type of the keys of the map.
     * @param <V> The type of the values of the map.
     * @param map The map to be serialized to a string
     * @return A string representation of the map.
     */
    public static <K, V> String mapToString(Map<K, V> map) {
        Map<String, String> asStrings = map.entrySet()
                                           .stream()
                                           .collect(toMap(e -> e.getKey().toString(), e -> e.getValue().toString()));
        asStrings.forEach((k, v) -> {
            Preconditions.checkArgument(k == null || !((k.contains(",") || k.contains("="))), "Invalid key: %s", k);
            Preconditions.checkArgument(v == null || !((v.contains(",") || v.contains("="))), "Invalid value: %s", v);
        });
        return Joiner.on(", ").withKeyValueSeparator('=').join(asStrings);
    }

    /**
     * Performs the reverse of {@link #mapToString(Map)}. It parses a map written in a string form
     * back into a Java map.
     * 
     * Note that in order to parse properly, it is important that none the keys or values that
     * were serialized contain '=' or ',' characters as this prevents parsing. For this reason it
     * should be noted that this simple format does not support nesting.
     * 
     * @param <K> The type of the keys of the map.
     * @param <V> The type of the values of the map.
     * @param serialized The serialized form of the map.
     * @param keyMaker The constructor for the key objects
     * @param valueMaker The constructor for the value objects
     * @return A map the corresponds to the serialized string.
     */
    public static <K, V> Map<K, V> stringToMap(String serialized, Function<String, K> keyMaker,
                                               Function<String, V> valueMaker) {
        Map<String, String> map = Splitter.on(',').trimResults().withKeyValueSeparator('=').split(serialized);
        return map.entrySet()
                  .stream()
                  .collect(toMap(e -> keyMaker.apply(e.getKey()), e -> valueMaker.apply(e.getValue())));
    }

}
