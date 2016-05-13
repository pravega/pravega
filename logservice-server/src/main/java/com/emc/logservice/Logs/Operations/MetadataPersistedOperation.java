package com.emc.logservice.Logs.Operations;

import com.emc.logservice.Logs.SerializationException;

import java.io.*;

/**
 * Log Operation that indicates that Metadata has been checkpointed (persisted) into its durable storage.
 */
public class MetadataPersistedOperation extends MetadataOperation {
    //region Members

    public static final byte OperationType = 6;
    private static final byte Version = 0;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the MetadataPersistedOperation class.
     */
    public MetadataPersistedOperation() {
        super();
    }

    protected MetadataPersistedOperation(OperationHeader header, DataInputStream source) throws SerializationException {
        super(header, source);
    }

    //endregion

    //region Operation Implementation

    @Override
    protected byte getOperationType() {
        return OperationType;
    }

    @Override
    protected void serializeContent(DataOutputStream target) throws IOException {
        target.writeByte(Version);
    }

    @Override
    protected void deserializeContent(DataInputStream source) throws IOException, SerializationException {
        byte version = readVersion(source, Version);
    }

    @Override
    public String toString() {
        return super.toString();
    }

    //endregion
}
