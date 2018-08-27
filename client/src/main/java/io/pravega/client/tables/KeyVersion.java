package io.pravega.client.tables;

import java.io.Serializable;
import lombok.Data;

@Data
public class KeyVersion implements Serializable {
    private static final long serialVersionUID = 1L;

    private final long segmentId;
    private final long segmentVersion;
}
