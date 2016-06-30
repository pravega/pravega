package com.emc.nautilus.streaming;

public interface RateChangeListener {

	public void rateChanged(Stream stream, boolean isRebalanceUrgent);

}
