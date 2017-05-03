package io.pravega.client.stream.impl.segment;

import io.pravega.client.stream.Sequence;
import io.pravega.shared.protocol.netty.AppendSequence;
import lombok.Data;

@Data
public class SequenceImpl implements Sequence {

    private final long highOrder;
    private final long lowOrder;

    private SequenceImpl(long highOrder, long lowOrder) {
        super();
        this.highOrder = highOrder;
        this.lowOrder = lowOrder;
    }
    
    public static Sequence create(long highOrder, long lowOrder) {
        return new SequenceImpl(highOrder, lowOrder);
    }
    
    static Sequence fromWire(AppendSequence seq) {
        return new SequenceImpl(seq.getHighOrder(), seq.getLowOrder());
    }

    @Override
    public SequenceImpl asImpl() {
        return this;
    }
    
    AppendSequence toWire() {
        return AppendSequence.create(highOrder, lowOrder);
    }
}
