package com.emc.pravega.common;

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
     * @param initalValue The value to be used as the initial average
     * @param newSampleWeight The fractional weight to give to new samples. 0.0 - 1.0
     * @param logarithmicWeighting If the samples should be weighted logarithmically to reduce the impact of outliers.
     */
    public ExponentialMovingAverage(double initalValue, double newSampleWeight, boolean logarithmicWeighting ) {
        Preconditions.checkArgument(newSampleWeight > 0.0 && newSampleWeight < 1.0, "New sample weight must be between 0.0 and 1.0");       
        this.newSampleWeight = newSampleWeight;
        this.logarithmicWeighting = logarithmicWeighting;
        double value = logarithmicWeighting ? Math.log(initalValue) : initalValue;
        this.valueEncodedAsLong = new AtomicLong(Double.doubleToLongBits(value));
    }
    
    /**
     * Returns the current moving average.
     */
    public double getCurrentValue() {
        double result = Double.longBitsToDouble(valueEncodedAsLong.get());
        return logarithmicWeighting ? Math.exp(result) : result;
    }
    
    /**
     * Adds a new sample to the moving average and returns the updated value.
     */
    public double addNewSample(double newSample) {
        final double sample = logarithmicWeighting ? Math.log(newSample) : newSample;
        return Double.longBitsToDouble(valueEncodedAsLong.updateAndGet(value -> {
            return Double.doubleToRawLongBits(sample * newSampleWeight + (1.0 - newSampleWeight) * Double.longBitsToDouble(value));
        }));
    }
    
}
