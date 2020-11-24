/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.shared.health;

public class TestHealthIndicators {

    public static class SampleHealthyIndicator extends HealthIndicator {
        public static final String DETAILS_KEY = "sample-indicator-details-key";

        public static final String DETAILS_VAL = "sample-indicator-details-value";

        public SampleHealthyIndicator() {
            super("sample-healthy-indicator", new DetailsProvider().add(DETAILS_KEY, () -> DETAILS_VAL));
        }

        public SampleHealthyIndicator(String name) {
            super(name, new DetailsProvider().add(DETAILS_KEY, () -> DETAILS_VAL));
        }

        public void doHealthCheck(Health.HealthBuilder builder) {
            builder.status(Status.UP);
            builder.alive(true);
            builder.ready(true);
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
            builder.status(Status.DOWN);
            builder.alive(false);
            builder.ready(false);
        }
    }

}
