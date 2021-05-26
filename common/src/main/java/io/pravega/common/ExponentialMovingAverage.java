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
package io.pravega.common;

import com.google.common.base.Preconditions;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Provides an Exponential moving average of some value.
 */
public class ExponentialMovingAverage {

    private final double newSampleWeight;
    private final AtomicLong valueEncodedAsLong;
    private final boolean sqrtWeighting;
    
    /**
     * Creates a new value to track.
     * 
     * @param initialValue The value to be used as the initial average
     * @param newSampleWeight The fractional weight to give to new samples. 0.0 - 1.0 (exclusive)
     * @param sqrtWeighting If the samples should be weighted according to the square root to reduce the impact of outliers.
     */
    public ExponentialMovingAverage(double initialValue, double newSampleWeight, boolean sqrtWeighting ) {
        Preconditions.checkArgument(newSampleWeight > 0.0 && newSampleWeight < 1.0, "New sample weight must be between 0.0 and 1.0");       
        this.newSampleWeight = newSampleWeight;
        this.sqrtWeighting = sqrtWeighting;
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
        if (!sqrtWeighting) {
            return newSample;
        } 
        
        return Math.signum(newSample) * Math.sqrt(Math.abs(newSample));
    }
    
    private double calculateExponential(double result) {
        if (!sqrtWeighting) {
            return result;
        } 
        
        return result * Math.abs(result);
    }
    
    @Override
    public String toString() {
        return Double.toString(getCurrentValue());
    }
}
