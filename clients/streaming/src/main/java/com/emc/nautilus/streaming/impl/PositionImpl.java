package com.emc.nautilus.streaming.impl;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import com.emc.nautilus.streaming.LogId;
import com.emc.nautilus.streaming.Position;

public class PositionImpl implements Position {
	
	private final Map<LogId, Long> ownedLogs;
	private final Map<LogId, Long> futureOwnedLogs;

	PositionImpl(Map<LogId,Long> ownedLogs, Map<LogId, Long> futureOwnedLogs) {
		this.ownedLogs = ownedLogs;
		this.futureOwnedLogs = futureOwnedLogs;		
	}

	Set<LogId> getOwnedLogs() {
		return Collections.unmodifiableSet(ownedLogs.keySet());
	}
	
	Long getOffsetForOwnedLog(LogId log) {
		return ownedLogs.get(log);
	}

	@Override
	public PositionImpl asImpl() {
		return this;
	}

	public Map<LogId, Long> getFutureOwnedLogs() {
		return Collections.unmodifiableMap(futureOwnedLogs);
	}

}
