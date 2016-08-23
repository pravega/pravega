package com.emc.pravega.controller.store.host;

import lombok.Data;
import lombok.ToString;
import java.util.List;

@Data
@ToString(includeFieldNames = true)
public class Host {
    private String ipAddr;
    private int port;

    private List<Integer> containers;
}
