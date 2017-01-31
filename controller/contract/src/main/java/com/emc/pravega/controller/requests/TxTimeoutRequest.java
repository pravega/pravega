package com.emc.pravega.controller.requests;

import lombok.Data;

@Data
public class TxTimeoutRequest implements ControllerRequest {

    private final String scope;
    private final String stream;
    private final String txid;
    private final long timeToDrop;

    @Override
    public RequestType getType() {
        return RequestType.TransactionRequest;
    }

    @Override
    public String getKey() {
        return String.format("%s/%s", scope, stream);
    }
}
