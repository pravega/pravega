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

import java.time.Duration;
import java.util.function.Consumer;

public class OpStatsLoggerProxy extends MetricProxy<OpStatsLogger, OpStatsLoggerProxy> implements OpStatsLogger {

    OpStatsLoggerProxy(OpStatsLogger logger, String proxyName, Consumer<OpStatsLoggerProxy> closeCallback) {
        super(logger, proxyName, closeCallback);
    }

    @Override
    public void reportSuccessEvent(Duration duration) {
        getInstance().reportSuccessEvent(duration);
    }

    @Override
    public void reportFailEvent(Duration duration) {
        getInstance().reportFailEvent(duration);
    }

    @Override
    public void reportSuccessValue(long value) {
        getInstance().reportSuccessValue(value);
    }

    @Override
    public void reportFailValue(long value) {
        getInstance().reportFailValue(value);
    }

    @Override
    public OpStatsData toOpStatsData() {
        return getInstance().toOpStatsData();
    }

    @Override
    public void clear() {
        getInstance().clear();
    }

    @Override
    protected OpStatsLoggerProxy getSelf() {
        return this;
    }
}
