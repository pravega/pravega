package com.emc.logservice.Logs.Operations;

import com.emc.logservice.Logs.SerializationException;

import java.io.DataInputStream;

/**
 * Log Operation that deals with Metadata Operations. This is generally an internal-only operation and is not necessarily
 * the direct outcome of an external call.
 */
public abstract class MetadataOperation extends Operation
{
    //region Constructor

    /**
     * Creates a new instance of the MetadataOperation class.
     */
    public MetadataOperation()
    {
        super();
    }

    protected MetadataOperation(OperationHeader header, DataInputStream source) throws SerializationException
    {
        super(header, source);
    }

    //endregion
}
