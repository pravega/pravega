/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.shared.health;

public class TestHealthIndicators {

    /**
     * Implements an {@link HealthIndicator} that *always* supplies a 'healthy' result. This class also sets one
     * details entry.
     */
    public static class SampleHealthyIndicator extends HealthIndicator {
        public static final String DETAILS_KEY = "indicator-details-key";

        public static final String DETAILS_VAL = "sample-indicator-details-value";

        public SampleHealthyIndicator() {
            super("sample-healthy-indicator", new DetailsProvider().add(DETAILS_KEY, () -> DETAILS_VAL));
        }

        public SampleHealthyIndicator(String name) {
            super(name, new DetailsProvider().add(DETAILS_KEY, () -> DETAILS_VAL));
        }

        public void doHealthCheck(Health.HealthBuilder builder) {
            setBuilder(builder, true, Status.UP);
        }
    }

    /**
     * Implements an {@link HealthIndicator} that *always* supplies a 'failing' result.
     */
    public static class SampleFailingIndicator extends HealthIndicator {
        public SampleFailingIndicator() {
            super("sample-failing-indicator");
        }

        public SampleFailingIndicator(String name) {
            super(name);
        }

        public void doHealthCheck(Health.HealthBuilder builder) {
            setBuilder(builder, false, Status.DOWN);
        }
    }

    /**
     * Implements an {@link HealthIndicator} that will set some {link Details} within it's
     * {@link HealthIndicator#doHealthCheck(Health.HealthBuilder)} method.
     */
    public static class DynamicHealthyIndicator extends SampleHealthyIndicator {
        public static final String DETAILS_VAL = "dynamic-indicator-details-value";

        public DynamicHealthyIndicator() {
            super("dynamic-healthy-indicator");
        }

        public void doHealthCheck(Health.HealthBuilder builder) {
            setBuilder(builder, true, Status.UP);
            setDetail(DETAILS_KEY, () -> DETAILS_VAL);
        }
    }

    /**
     * Implements an {@link HealthIndicator} that provide *no* logic within the {@link HealthIndicator#doHealthCheck(Health.HealthBuilder)}.
     */
    public static class BodylessIndicator extends HealthIndicator {
        public BodylessIndicator() {
            super("bodyless-indicator");
        }

        public void doHealthCheck(Health.HealthBuilder builder) {
        }
    }

    /**
     * Implements an {@link HealthIndicator} that *always* will throw an error within it's {@link HealthIndicator#doHealthCheck(Health.HealthBuilder)}.
     */
    public static class ThrowingIndicator extends HealthIndicator {
        public ThrowingIndicator() {
            super("throwing-indicator");
        }

        public void doHealthCheck(Health.HealthBuilder builder) {
            setBuilder(builder, true, Status.UP);
            throw new RuntimeException();
        }
    }

    private static void setBuilder(Health.HealthBuilder builder, boolean flag, Status status) {
        builder.ready(flag);
        builder.alive(flag);
        builder.status(status);
    }
}
