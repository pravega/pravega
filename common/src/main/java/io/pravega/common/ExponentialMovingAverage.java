/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common;

import com.google.common.base.Preconditions;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Provides an Exponential moving average of some value.
 */
public class ExponentialMovingAverage {

    private final double newSampleWeight;
    private final AtomicLong valueEncodedAsLong;
    private final boolean logarithmicWeighting;
    
    /**
     * Creates a new value to track.
     * 
     * @param initialValue The value to be used as the initial average
     * @param newSampleWeight The fractional weight to give to new samples. 0.0 - 1.0 (exclusive)
     * @param logarithmicWeighting If the samples should be weighted logarithmically to reduce the impact of outliers.
     */
    public ExponentialMovingAverage(double initialValue, double newSampleWeight, boolean logarithmicWeighting ) {
        Preconditions.checkArgument(newSampleWeight > 0.0 && newSampleWeight < 1.0, "New sample weight must be between 0.0 and 1.0");       
        this.newSampleWeight = newSampleWeight;
        this.logarithmicWeighting = logarithmicWeighting;
        double value = calculateLog(initialValue);
        this.valueEncodedAsLong = new AtomicLong(Double.doubleToLongBits(value));
    }
    
    /**
     * Returns the current moving average.
     *
     * @return Double indicating the current moving average.
     */
    public double getCurrentValue() {
        double result = Double.longBitsToDouble(valueEncodedAsLong.get());
        return calculateExponential(result);
    }

    
    /**
     * Adds a new sample to the moving average and returns the updated value.
     * 
     * @param newSample the new value to be added
     * @return Double indicating the updated moving average value after adding a new sample.
     */
    public double addNewSample(double newSample) {
        final double sample = calculateLog(newSample);
        return Double.longBitsToDouble(valueEncodedAsLong.updateAndGet(value -> {
            return Double.doubleToRawLongBits(sample * newSampleWeight + (1.0 - newSampleWeight) * Double.longBitsToDouble(value));
        }));
    }

    private double calculateLog(double newSample) {
        if (!logarithmicWeighting) {
            return newSample;
        } 
        
        return Math.signum(newSample) * Math.log1p(Math.abs(newSample));
    }
    
    private double calculateExponential(double result) {
        if (!logarithmicWeighting) {
            return result;
        } 
        
        return Math.signum(result) * (Math.expm1(Math.abs(result)));
    }
    
    @Override
    public String toString() {
        return Double.toString(getCurrentValue());
    }
}
