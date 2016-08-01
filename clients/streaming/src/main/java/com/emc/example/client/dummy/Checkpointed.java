package com.emc.example.client.dummy;

import com.emc.nautilus.stream.Position;

public interface Checkpointed<T> {

    Position snapshotState(long checkpointId, long checkpointTimestamp);

    void restoreState(Position state);

}
