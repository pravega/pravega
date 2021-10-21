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

import java.util.function.Supplier;

/**
 * Defines a Gauge, which will wrap a gauge instance and its name.
 */
public interface Gauge extends Metric {

    /**
     * Set the supplier of gauge value.
     * @param supplier the supplier of gauge value.
     */
    void setSupplier(Supplier<Number> supplier);

    /**
     * Get the supplier of gauge value.
     * @return the supplier of gauge value.
     */
    Supplier<Number> getSupplier();
}
