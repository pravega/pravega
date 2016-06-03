package com.emc.nautilus.streaming;

import java.io.Serializable;

import com.emc.nautilus.logclient.LogOutputConfiguration;

import lombok.Data;

@Data
public class ProducerConfig implements Serializable {

	private final LogOutputConfiguration logConfig;

}
