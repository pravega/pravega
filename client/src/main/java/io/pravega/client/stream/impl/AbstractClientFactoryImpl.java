package io.pravega.client.stream.impl;

import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.SynchronizerClientFactory;
import io.pravega.client.connection.impl.ConnectionPool;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public abstract class AbstractClientFactoryImpl implements EventStreamClientFactory, SynchronizerClientFactory {

    @NonNull
    protected final String scope;
    @NonNull
    @Getter
    protected final Controller controller;
    @NonNull 
    @Getter
    protected final ConnectionPool connectionPool;
}
