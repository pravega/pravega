package io.pravega.client.tables;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class GetResult<ValueT> {
    private final KeyVersion keyVersion;
    private final ValueT value;
}
