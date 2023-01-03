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
package io.pravega.segmentstore.storage.mocks;

import io.pravega.segmentstore.storage.noop.StorageExtraConfig;
import lombok.RequiredArgsConstructor;
import lombok.val;

import java.time.Duration;
import java.util.Random;
import java.util.function.Supplier;

/**
 * States different types of delays generated in the working of LTS.
 * It can throttle when it is needed, but also that it can stop throttling when LTS works well again.
 * Configuration properties stated in {@link StorageExtraConfig} are used to inject different types of delays (or delay generators).
 */
public class SlowDelaySuppliers {
    /**
     * Factory method that creates {@link Supplier<Duration>} based on {@link StorageExtraConfig}.
     * @param config config to use.
     * @return instance of {@link Supplier<Duration>}.
     */
    static Supplier<Duration> getDurationSupplier(StorageExtraConfig config) {
        if (config.getDistributionType().equals(StorageDelayDistributionType.FIXED_DISTRIBUTION_TYPE)) {
            return new FixedDelaySupplier(config.getSlowModeLatencyMeanMillis());
        }
        if (config.getDistributionType().equals(StorageDelayDistributionType.NORMAL_DISTRIBUTION_TYPE)) {
            return new GaussianDelaySupplier(config.getSlowModeLatencyMeanMillis(), config.getSlowModeLatencyStdDevMillis());
        }
        if (config.getDistributionType().equals(StorageDelayDistributionType.SINUSOIDAL_DISTRIBUTION_TYPE)) {
            return new SinusoidalDelaySupplier(config.getSlowModeLatencyMeanMillis(), config.getSlowModeLatencyCycleTimeMillis());
        }
        throw new UnsupportedOperationException();
    }

    /**
     * To generate a delay in the working of LTS with normal distribution type.
     */
    @RequiredArgsConstructor
    static class GaussianDelaySupplier implements Supplier<Duration> {
        private final long mean;
        private final long stdDev;
        private final Random random = new Random();
        @Override
        public Duration get() {
            return Duration.ofMillis(calculateValue(() -> random.nextGaussian()));
        }

        /**
         * Calculates value for normal distribution using random generator.
         * @param randomSupplier generates random value
         * @return calculated delay value
         */
        long calculateValue(Supplier<Double> randomSupplier) {
            return Math.max(0, Math.round(mean + randomSupplier.get() * stdDev));
        }
    }

    /**
     * Implements sinusoidal delay for taking care of the high and low latency of the LTS.
     */
    static class SinusoidalDelaySupplier implements Supplier<Duration> {
        private long startTime;
        private final long mean;
        private final long cycleTime;

        SinusoidalDelaySupplier(long mean, long cycleTime) {
            this.startTime = System.currentTimeMillis();
            this.mean = mean;
            this.cycleTime = cycleTime;
        }

        @Override
        public Duration get() {
            final long returnValue = calculateValue(System.currentTimeMillis(), startTime, cycleTime);
            return Duration.ofMillis(returnValue);
        }

        /**
         * Calculates the delay value for sinusoidal distribution type
         * @param currentTime time at that instance
         * @param startTime time at which the sin wave cycle start
         * @param cycleTime time required to complete one cycle for the sinusoidal waveform
         * @return calculated delay value
         */
        long calculateValue(long currentTime, long startTime, long cycleTime) {
            val timeCal = (currentTime - startTime) % cycleTime;
            val returnValue = (1 + Math.sin(Math.toRadians(timeCal))) * mean;
            return Math.round(returnValue);
        }
    }

    /**
     * Implemented to always generate fixed delay.
     */
    @RequiredArgsConstructor
    static class FixedDelaySupplier implements Supplier<Duration> {
        private final long duration;
        @Override
        public Duration get() {
            return Duration.ofMillis(duration);
        }
    }
}
