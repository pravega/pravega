/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.emc.pravega.service.server.host.benchmark;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AtomicDouble;

import java.util.ArrayList;
import java.util.Collections;

/**
 * Collects and calculates statistics on a series.
 */
public class SeriesStatistic {
    private final ArrayList<Double> items;
    private double average;
    private boolean isSnapshot;

    public SeriesStatistic() {
        this.items = new ArrayList<>();
    }

    public void add(double value) {
        this.items.add(value);
    }

    public void snapshot() {
        Collections.sort(this.items);

        // Calculate average.
        if (this.items.size() > 0) {
            AtomicDouble sum = new AtomicDouble();
            this.items.forEach(sum::addAndGet);
            this.average = sum.get() / this.items.size();
        }

        this.isSnapshot = true;
    }

    public double min() {
        Preconditions.checkState(this.isSnapshot, "Cannot get a statistic if snapshot() hasn't been run.");
        Preconditions.checkState(this.items.size() > 0, "No elements in the series.");
        return this.items.get(0);
    }

    public double max() {
        Preconditions.checkState(this.isSnapshot, "Cannot get a statistic if snapshot() hasn't been run.");
        Preconditions.checkState(this.items.size() > 0, "No elements in the series.");
        return this.items.get(this.items.size() - 1);
    }

    public double avg() {
        Preconditions.checkState(this.isSnapshot, "Cannot get a statistic if snapshot() hasn't been run.");
        Preconditions.checkState(this.items.size() > 0, "No elements in the series.");
        return this.average;
    }

    public double percentile(double percentile) {
        Preconditions.checkState(this.isSnapshot, "Cannot get a statistic if snapshot() hasn't been run.");
        Preconditions.checkState(this.items.size() > 0, "No elements in the series.");
        Preconditions.checkArgument(percentile >= 0 && percentile <= 100, "Invalid percentile. Must be between 0 and 100 (inclusive).");
        int index = (int) (this.items.size() * percentile / 100);
        return this.items.get(index);
    }

    @Override
    public String toString() {
        if (!this.isSnapshot) {
            return "Snapshot was not run";
        } else if (this.items.size() == 0) {
            return "Empty series.";
        } else {
            return String.format(
                    "Min = %.1f, Max = %.1f, Avg = %.1f, 50%% = %.1f, 90%% = %.1f, 95%% = %.1f, 99%% = %.1f, 99.9%% = %.1f",
                    min(),
                    max(),
                    avg(),
                    percentile(50),
                    percentile(90),
                    percentile(95),
                    percentile(99),
                    percentile(99.9));
        }
    }
}