package com.emc.nautilus.streaming;

import java.util.List;

import lombok.Data;

@Data
public class StreamLogs {
	public final List<LogId> logs;
	public final long time;
}