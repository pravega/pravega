package com.emc.nautilus.streaming;

import java.util.List;
import java.util.Map;

public interface RebalancerUtils {
	List<Position> getIntitialPositions(StreamSegments logs, int numberOfConsumers);
	List<Position> rebalance(List<Position> consumers, int newNumberOfConsumers);
	Map<String,Position> rebalance(Map<String,Position> consumers, List<String> newConsumers);
}
