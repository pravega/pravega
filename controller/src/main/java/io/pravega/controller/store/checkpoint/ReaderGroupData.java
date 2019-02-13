package io.pravega.controller.store.checkpoint;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;
import java.util.List;

// TODO: shivesh: change this to use custom serialization
@Data
@AllArgsConstructor
class ReaderGroupData implements Serializable {
    enum State {
        Active,
        Sealed,
    }

    private final State state;
    private final List<String> readerIds;
}
