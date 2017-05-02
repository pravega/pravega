package io.pravega.shared.protocol.netty;

import lombok.Data;

/**
 * A wrapper for two numbers. Where one is treated as 'high order' and the other as 'low order' to
 * break ties when the former is the same.
 */
@Data(staticConstructor = "create")
public class AppendSequence implements Comparable<AppendSequence> {
    public static final AppendSequence MAX_VALUE = new AppendSequence(Long.MAX_VALUE, Long.MAX_VALUE);
    public static final AppendSequence MIN_VALUE = new AppendSequence(Long.MIN_VALUE, Long.MIN_VALUE);
    private final long highOrder;
    private final long lowOrder;
    
    @Override
    public int compareTo(AppendSequence o) {
        int result = Long.compare(highOrder, o.highOrder);
        if (result == 0) {
            return Long.compare(lowOrder, o.lowOrder);
        }
        return result;
    }
}