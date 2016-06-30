package com.emc.example.client;

import com.emc.example.client.dummy.Checkpointed;
import com.emc.example.client.dummy.RichParallelSourceFunction;
import com.emc.example.client.dummy.SourceContext;
import com.emc.nautilus.streaming.Consumer;
import com.emc.nautilus.streaming.Position;

public class StreamConsumerSource<OUT> extends RichParallelSourceFunction<OUT> implements Checkpointed<Position> {

	private final Consumer<OUT> consumer;
	private volatile boolean isRunning = true;

	public StreamConsumerSource(Consumer<OUT> consumer) {
		this.consumer = consumer;
	}

	@Override
	public void run(SourceContext<OUT> ctx) throws Exception {
		while (isRunning) {
			OUT nextElement = consumer.getNextEvent(1000);
			ctx.collect(nextElement);
		}
	}

	@Override
	public void cancel() {
		isRunning = false;
	}

	@Override
	public Position snapshotState(long checkpointId, long checkpointTimestamp) {
		return consumer.getPosition();
	}

	@Override
	public void restoreState(Position state) {
		consumer.setPosition(state);
	}
}
