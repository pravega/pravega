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
package io.pravega.segmentstore.storage.chunklayer;

import io.pravega.common.AbstractTimer;
import io.pravega.common.Timer;
import lombok.Getter;
import lombok.Setter;

import java.util.function.Supplier;

/***
 * Class containing utility methods to deal with timers.
 */
public class TimerUtils {
    /**
     * Singleton instance.
     */
    @Getter
    private static final TimerUtils SINGLETON = new TimerUtils();

    /**
     * {@link Supplier} that creates new instance of {@link AbstractTimer}.
     */
    @Getter
    @Setter
    private Supplier<AbstractTimer> timerSupplier = Timer::new;

    /**
     * Creates new instance of {@link AbstractTimer}.
     */
    static public AbstractTimer createTimer() {
        return SINGLETON.timerSupplier.get();
    }
}
