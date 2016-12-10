/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package com.emc.pravega.stream.impl;

import com.emc.pravega.stream.EventPointer;
import com.emc.pravega.stream.Segment;

import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@EqualsAndHashCode
@RequiredArgsConstructor
public class EventPointerImpl implements EventPointer {
    private static final char SEPARATOR = '#';
    @Getter(value = AccessLevel.PACKAGE)
    private final Segment segment;
    @Getter(value = AccessLevel.PACKAGE)
    private final long offset;

    @Override
    public EventPointerImpl asImpl() {
        return this;
    }

    @Override
    public String asString() {
        return segment.getScopedName() + SEPARATOR + offset;
    }

    public static EventPointerImpl fromString(String qualifiedName) {
        String[] tokens = qualifiedName.split("[" + SEPARATOR + "]");
        if (tokens.length != 2) {
            throw new IllegalArgumentException("Not a valid EventPointer");
        }
        return new EventPointerImpl(Segment.fromScopedName(tokens[0]), Long.parseLong(tokens[1]));
    }

}
