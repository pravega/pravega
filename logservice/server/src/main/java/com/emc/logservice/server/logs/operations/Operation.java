package com.emc.logservice.server.logs.operations;

import com.emc.logservice.common.Exceptions;
import com.emc.logservice.server.LogItem;
import com.emc.logservice.server.core.MagicGenerator;
import com.emc.logservice.server.logs.SerializationException;
import com.google.common.base.Preconditions;

import java.io.*;

/**
 * Base class for a Log Operation.
 */
public abstract class Operation implements LogItem {
    //region Members

    public static final long NoSequenceNumber = Long.MIN_VALUE;
    private long sequenceNumber;

    //endregion

    //region Constructors

    /**
     * Creates a new instance of the Operation class.
     */
    public Operation() {
        this.sequenceNumber = NoSequenceNumber;
    }

    /**
     * Creates a new instance of the Operation class using the given header and source.
     *
     * @param header The Operation header to use.
     * @param source A DataInputStream to deserialize from.
     * @throws SerializationException If the deserialization failed.
     */
    protected Operation(OperationHeader header, DataInputStream source) throws SerializationException {
        this.sequenceNumber = header.sequenceNumber;
        deserialize(header, source);
    }

    //endregion

    //region Properties

    /**
     * Gets a value indicating the Sequence Number for this Operation.
     * The Operation Sequence Number is a unique, strictly monotonically increasing number that assigns order to operations.
     *
     * @return The Sequence Number for this Operation.
     */
    @Override
    public long getSequenceNumber() {
        return this.sequenceNumber;
    }

    /**
     * Sets the Sequence Number for this operation, if not already set.
     *
     * @param value The Sequence Number to set.
     * @throws IllegalStateException    If the Sequence Number has already been set.
     * @throws IllegalArgumentException If the Sequence Number is negative.
     */
    public void setSequenceNumber(long value) {
        Preconditions.checkState(this.sequenceNumber < 0, "Sequence Number has been previously set for this entry. Cannot set a new one.");
        Exceptions.checkArgument(value >= 0, "value", "Sequence Number must be a non-negative number.");

        this.sequenceNumber = value;
    }

    /**
     * Gets an internal unique number representing the type of this operation.
     *
     * @return
     */
    protected abstract byte getOperationType();

    @Override
    public String toString() {
        return String.format("%s: SequenceNumber = %d", this.getClass().getSimpleName(), getSequenceNumber());
    }

    //endregion

    //region Serialization

    /**
     * Serializes this Operation to the given OutputStream.
     *
     * @param output The OutputStream to serialize to.
     * @throws IOException           If the given OutputStream threw one.
     * @throws IllegalStateException If the serialization conditions are not met.
     */
    @Override
    public void serialize(OutputStream output) throws IOException {
        ensureSerializationCondition(this.sequenceNumber >= 0, "Sequence Number has not been assigned for this entry.");

        DataOutputStream target = new DataOutputStream(output);
        OperationHeader header = new OperationHeader(getOperationType(), this.sequenceNumber);
        header.serialize(target);
        serializeContent(target);

        // We write the magic value again at the end; this way when we deserialize, we can detect data corruptions.
        target.writeInt(header.magic);
    }

    /**
     * Deserializes the Operation.
     *
     * @param header The OperationHeader to use.
     * @param source The input stream to read from.
     * @throws IOException            If the given DataInputStream threw one.
     * @throws SerializationException If the deserialization failed.
     */
    private void deserialize(OperationHeader header, DataInputStream source) throws SerializationException {
        if (header.operationType != getOperationType()) {
            throw new SerializationException("Operation.deserialize", String.format("Invalid Operation Type. Expected %d, Found %d.", getOperationType(), header.operationType));
        }

        int endMagic;
        try {
            deserializeContent(source);

            // Read the magic value at the end. This must match the beginning magic - this way we know we reached the end properly.
            endMagic = source.readInt();
        }
        catch (IOException ex) {
            throw new SerializationException("Operation.deserialize", "Unable to read from the InputStream.", ex);
        }

        if (header.magic != endMagic) {
            throw new SerializationException("Operation.deserialize", String.format("Start and End Magic values mismatch. This indicates possible data corruption. SequenceNumber = %d, EntryType = %d.", header.sequenceNumber, header.operationType));
        }
    }

    /**
     * Reads a version byte from the given input stream and compares it to the given expected version.
     *
     * @param source          The input stream to read from.
     * @param expectedVersion The expected version to compare to.
     * @return The version.
     * @throws IOException            If the input stream threw one.
     * @throws SerializationException If the versions mismatched.
     */
    protected byte readVersion(DataInputStream source, byte expectedVersion) throws IOException, SerializationException {
        byte version = source.readByte();
        if (version != expectedVersion) {
            throw new SerializationException(String.format("%s.deserialize", this.getClass().getSimpleName()), String.format("Unsupported version: %d.", version));
        }

        return version;
    }

    /**
     * If the given condition is false, throws an exception with the given message.
     *
     * @param isTrue  Whether the condition is true or false.
     * @param message The message to include in the exception.
     * @throws IllegalStateException The exception that is thrown.
     */
    protected void ensureSerializationCondition(boolean isTrue, String message) {
        Preconditions.checkState(isTrue, "Unable to serialize Operation: {}", message);
    }

    /**
     * Serializes the content of this Operation.
     *
     * @param target The DataOutputStream to serialize to.
     * @throws IOException If the DataOutputStream threw one.
     */
    protected abstract void serializeContent(DataOutputStream target) throws IOException;

    /**
     * Deserializes the content of this Operation.
     *
     * @param source The DataInputStream to read from.
     * @throws IOException            If the DataInputStream threw one.
     * @throws SerializationException If we detected an error, such as data corruption.
     */
    protected abstract void deserializeContent(DataInputStream source) throws IOException, SerializationException;

    // endregion

    //region OperationHeader

    /**
     * Header for a serialized Operation.
     */
    protected static class OperationHeader {
        /**
         * The type of the operation.
         */
        public final byte operationType;

        /**
         * The sequence number for the operation.
         */
        public final long sequenceNumber;

        /**
         * Magic value.
         */
        public final int magic;
        private static final byte HeaderVersion = 0;

        /**
         * Creates a new instance of the OperationHeader class.
         *
         * @param operationType  The type of the operation.
         * @param sequenceNumber The sequence number for the operation.
         */
        public OperationHeader(byte operationType, long sequenceNumber) {
            this.operationType = operationType;
            this.sequenceNumber = sequenceNumber;
            this.magic = MagicGenerator.newMagic();
        }

        /**
         * Creates a new instance of the OperationHeader class.
         *
         * @param source The DataInputStream to deserialize from.
         * @throws SerializationException If deserialization failed.
         */
        public OperationHeader(DataInputStream source) throws SerializationException {
            try {
                byte headerVersion = source.readByte();
                if (headerVersion == HeaderVersion) {
                    this.magic = source.readInt();
                    this.operationType = source.readByte();
                    this.sequenceNumber = source.readLong();
                }
                else {
                    throw new SerializationException("OperationHeader.deserialize", String.format("Unsupported version: %d.", headerVersion));
                }
            }
            catch (IOException ex) {
                throw new SerializationException("OperationHeader.deserialize", "Unable to deserialize Operation Header", ex);
            }
        }

        /**
         * Serializes this OperationHeader to the given target.
         *
         * @param target The DataOutputStream to serialize to.
         * @throws IOException If the DataOutputStream threw one.
         */
        public void serialize(DataOutputStream target) throws IOException {
            target.writeByte(HeaderVersion);
            target.writeInt(this.magic);
            target.writeByte(this.operationType);
            target.writeLong(this.sequenceNumber);
        }

        @Override
        public String toString() {
            return String.format("SequenceNumber = %d, EntryType = %d", this.sequenceNumber, this.operationType);
        }
    }

    //endregion

}
