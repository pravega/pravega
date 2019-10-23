package io.pravega.client.segment.impl;

import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Semaphore;

public class LimitedConcurrentSkipListMap<K, V>
extends ConcurrentSkipListMap<K, V>
implements LimitedNavigableMap<K, V> {

	private final int sizeLimit;
	private final Semaphore insertPermits;

	public LimitedConcurrentSkipListMap(final int sizeLimit) {
		this.sizeLimit = sizeLimit;
		this.insertPermits = new Semaphore(sizeLimit, true);
	}

	@Override
	public final int size() {
		return sizeLimit - insertPermits.availablePermits();
	}

	@Override
	public final boolean isEmpty() {
		return sizeLimit == insertPermits.availablePermits();
	}

	@Override
	public final boolean putIfNotFull(final K k, final V v) {
		if(insertPermits.tryAcquire()) {
			super.put(k, v);
			return true;
		}
		return false;
	}

	@Override
	public final V remove(final Object o) {
		insertPermits.release();
		return super.remove(o);
	}

	@Override
	public final boolean remove(final Object o, final Object o1) {
		insertPermits.release();
		return super.remove(o, o1);
	}

	@Override
	public final void clear() {
		insertPermits.release(sizeLimit);
		super.clear();
	}
}
