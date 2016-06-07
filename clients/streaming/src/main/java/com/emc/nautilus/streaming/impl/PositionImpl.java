package com.emc.nautilus.streaming.impl;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import com.emc.nautilus.streaming.SegmentId;
import com.emc.nautilus.streaming.Position;

public class PositionImpl implements Position {
	
	private final Map<SegmentId, Long> ownedLogs;
	private final Map<SegmentId, Long> futureOwnedLogs;

	PositionImpl(Map<SegmentId,Long> ownedLogs, Map<SegmentId, Long> futureOwnedLogs) {
		this.ownedLogs = ownedLogs;
		this.futureOwnedLogs = futureOwnedLogs;		
	}

	Set<SegmentId> getOwnedSegments() {
		return Collections.unmodifiableSet(ownedLogs.keySet());
	}
	
	Long getOffsetForOwnedLog(SegmentId log) {
		return ownedLogs.get(log);
	}

	@Override
	public PositionImpl asImpl() {
		return this;
	}

	public Map<SegmentId, Long> getFutureOwnedLogs() {
		return Collections.unmodifiableMap(futureOwnedLogs);
	}

}
