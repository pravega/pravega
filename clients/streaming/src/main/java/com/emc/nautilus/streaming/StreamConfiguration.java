package com.emc.nautilus.streaming;

public interface StreamConfiguration {
    String getName();

    ScalingingPolicy getScalingingPolicy();
}
