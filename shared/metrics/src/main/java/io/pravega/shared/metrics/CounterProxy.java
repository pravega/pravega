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
package io.pravega.shared.metrics;

import java.util.function.Consumer;

class CounterProxy extends MetricProxy<Counter, CounterProxy> implements Counter {

    CounterProxy(Counter counter, String proxyName, Consumer<CounterProxy> closeCallback) {
        super(counter, proxyName, closeCallback);
    }

    @Override
    public void clear() {
        getInstance().clear();
    }

    @Override
    public void inc() {
        getInstance().inc();
    }

    @Override
    public void add(long delta) {
        getInstance().add(delta);
    }

    @Override
    public long get() {
        return getInstance().get();
    }

    @Override
    protected CounterProxy getSelf() {
        return this;
    }
}
