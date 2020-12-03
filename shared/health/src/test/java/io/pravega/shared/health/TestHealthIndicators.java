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

    public static class BodylessIndicator extends HealthIndicator {
        public BodylessIndicator() {
            super("bodyless-indicator");
        }

        public void doHealthCheck(Health.HealthBuilder builder) {
        }
    }

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
        builder.ready(true);
        builder.alive(true);
        builder.status(status);
    }
}
