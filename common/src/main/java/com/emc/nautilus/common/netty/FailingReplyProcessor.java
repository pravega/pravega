package com.emc.nautilus.common.netty;

import com.emc.nautilus.common.netty.WireCommands.*;

public class FailingReplyProcessor implements ReplyProcessor {

	@Override
	public void wrongHost(WrongHost wrongHost) {
		throw new IllegalStateException("Unexpected operation");
	}

	@Override
	public void segmentIsSealed(SegmentIsSealed segmentIsSealed) {
		throw new IllegalStateException("Unexpected operation");
	}

	@Override
	public void segmentAlreadyExists(SegmentAlreadyExists segmentAlreadyExists) {
		throw new IllegalStateException("Unexpected operation");
	}

	@Override
	public void noSuchSegment(NoSuchSegment noSuchSegment) {
		throw new IllegalStateException("Unexpected operation");
	}

	@Override
	public void noSuchBatch(NoSuchBatch noSuchBatch) {
		throw new IllegalStateException("Unexpected operation");
	}

	@Override
	public void appendSetup(AppendSetup appendSetup) {
		throw new IllegalStateException("Unexpected operation");
	}

	@Override
	public void dataAppended(DataAppended dataAppended) {
		throw new IllegalStateException("Unexpected operation");
	}

	@Override
	public void segmentRead(SegmentRead data) {
		throw new IllegalStateException("Unexpected operation");
	}

	@Override
	public void streamSegmentInfo(StreamSegmentInfo streamInfo) {
		throw new IllegalStateException("Unexpected operation");
	}

	@Override
	public void segmentCreated(SegmentCreated streamsSegmentCreated) {
		throw new IllegalStateException("Unexpected operation");
	}

	@Override
	public void batchCreated(BatchCreated batchCreated) {
		throw new IllegalStateException("Unexpected operation");
	}

	@Override
	public void batchMerged(BatchMerged batchMerged) {
		throw new IllegalStateException("Unexpected operation");
	}

	@Override
	public void segmentSealed(SegmentSealed segmentSealed) {
		throw new IllegalStateException("Unexpected operation");
	}

	@Override
	public void segmentDeleted(SegmentDeleted segmentDeleted) {
		throw new IllegalStateException("Unexpected operation");
	}

	@Override
	public void keepAlive(KeepAlive keepAlive) {
		throw new IllegalStateException("Unexpected operation");
	}

}
