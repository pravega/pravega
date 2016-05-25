package com.emc.nautilus.streaming.impl;

import java.util.Collection;

public interface Orderer<Type> {

	/**
	 * Given a list of logs this consumer owns, (which contain their positions)
	 * returns the one that should be read from next. This is done in a
	 * consistent way. IE: Calling this method with the same consumers at the
	 * same positions, should yield the same result.
	 * (The passed collection is not modified)
	 */
	LogConsumer<Type> nextConsumer(Collection<LogConsumer<Type>> logs);

}
