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

    public static <K, V> Map<K, V> stringToMap(String serialized, Function<String, K> keyMaker,
                                               Function<String, V> valueMaker) {
        Map<String, String> map = Splitter.on(',').trimResults().withKeyValueSeparator('=').split(serialized);
        return map.entrySet()
                  .stream()
                  .collect(toMap(e -> keyMaker.apply(e.getKey()), e -> valueMaker.apply(e.getValue())));
    }

}
