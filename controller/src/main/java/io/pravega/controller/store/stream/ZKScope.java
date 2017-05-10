/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
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
package io.pravega.controller.store.stream;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public class ZKScope implements Scope {

    private static final String SCOPE_PATH = "/store/%s";
    private final String scopePath;
    private final String scopeName;
    private final ZKStoreHelper store;

    protected ZKScope(final String scopeName, ZKStoreHelper store) {
        this.scopeName = scopeName;
        this.store = store;
        scopePath = String.format(SCOPE_PATH, scopeName);
    }

    @Override
    public String getName() {
        return this.scopeName;
    }

    @Override
    public CompletableFuture<Void> createScope() {
        return store.addNode(scopePath);
    }

    @Override
    public CompletableFuture<Void> deleteScope() {
        return store.deleteNode(scopePath);
    }

    @Override
    public CompletableFuture<List<String>> listStreamsInScope() {
        return store.getStreamsInPath(scopePath);
    }

    @Override
    public void refresh() {
    }

}
