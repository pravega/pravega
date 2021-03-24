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

public class MeterProxy extends MetricProxy<Meter> implements Meter {
    MeterProxy(Meter meter, String proxyName, Consumer<String> closeCallback) {
        super(meter, proxyName, closeCallback);
    }

    @Override
    public void recordEvent() {
        getInstance().recordEvent();
    }

    @Override
    public void recordEvents(long n) {
        getInstance().recordEvents(n);
    }

    @Override
    public long getCount() {
        return getInstance().getCount();
    }
}
