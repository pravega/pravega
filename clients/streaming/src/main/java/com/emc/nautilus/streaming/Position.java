package com.emc.nautilus.streaming;

import java.io.Serializable;

import com.emc.nautilus.streaming.impl.PositionImpl;

public interface Position extends Serializable {
	PositionImpl asImpl();
}