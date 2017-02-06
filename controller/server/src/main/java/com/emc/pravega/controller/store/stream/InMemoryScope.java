/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.emc.pravega.controller.store.stream;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class InMemoryScope implements Scope {

    private final String scopeName;

    //private final Map<InMemoryScope, List<InMemoryStream>> scopesToStreamMap = new HashMap<>();
    private List<InMemoryStream> streamsInScope;

    InMemoryScope(String scopeName) {
        this.scopeName = scopeName;
    }

    @Override
    public String getName() {
        return this.scopeName;
    }

    @Override
    public CompletableFuture<Boolean> createScope(String scope) {
        streamsInScope = new ArrayList<>();
        return CompletableFuture.completedFuture(true);
    }

    @Override
    public CompletableFuture<Boolean> deleteScope(String scope) {
        return null;
    }

    @Override
    public CompletableFuture<List<Stream>> listStreamsInScope(String scope) {
        return null;
    }

    @Override
    public void refresh() {

    }

    public void addStreamToScope(InMemoryStream stream) {
        streamsInScope.add(stream);
    }
}
