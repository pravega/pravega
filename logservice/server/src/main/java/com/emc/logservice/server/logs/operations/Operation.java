package com.emc.logservice.server.logs.operations;

import com.emc.logservice.server.core.MagicGenerator;
import com.emc.logservice.server.logs.SerializationException;
import com.emc.logservice.contracts.StreamingException;

import java.io.*;
import java.util.HashMap;

/**
 * Base class for a Log Operation.
 */
public abstract class Operation {
    //region Members

    public static final long NoSequenceNumber = Long.MIN_VALUE;
    private static final OperationConstructors constructors = new OperationConstructors();
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
        if (this.sequenceNumber >= 0) {
            throw new IllegalStateException("Sequence Number has been previously set for this entry. Cannot set a new one.");
        }

        if (value < 0) {
            throw new IllegalArgumentException("Sequence Number must be a non-negative number.");
        }

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
     * @param source The input stream to getReader from.
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
            throw new SerializationException("Operation.deserialize", "Unable to getReader from the InputStream.", ex);
        }

        if (header.magic != endMagic) {
            throw new SerializationException("Operation.deserialize", String.format("Start and End Magic values mismatch. This indicates possible data corruption. SequenceNumber = %d, EntryType = %d.", header.sequenceNumber, header.operationType));
        }
    }

    /**
     * Reads a version byte from the given input stream and compares it to the given expected version.
     *
     * @param source          The input stream to getReader from.
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
        if (!isTrue) {
            throw new IllegalStateException("Unable to serialize Operation: " + message);
        }
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
     * @param source The DataInputStream to getReader from.
     * @throws IOException            If the DataInputStram threw one.
     * @throws SerializationException If we detected an error, such as data corruption.
     */
    protected abstract void deserializeContent(DataInputStream source) throws IOException, SerializationException;

    /**
     * Attempts to
     *
     * @param input
     * @return
     * @throws IOException
     * @throws SerializationException
     */
    public static Operation deserialize(InputStream input) throws SerializationException {
        DataInputStream source = new DataInputStream(input);
        OperationHeader header = new OperationHeader(source);
        return constructors.create(header, source);
    }

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

    // region OperationConstructors (factory)

    /**
     * Helps collect and invoke constructors for Log Operations.
     */
    private static class OperationConstructors {
        private final HashMap<Byte, OperationConstructor> constructors;

        public OperationConstructors() {
            constructors = new HashMap<>();
            try {
                //TODO: there might be a better way to do this dynamically...
                map(StreamSegmentAppendOperation.OperationType, StreamSegmentAppendOperation::new);
                map(StreamSegmentSealOperation.OperationType, StreamSegmentSealOperation::new);
                map(MergeBatchOperation.OperationType, MergeBatchOperation::new);
                map(MetadataPersistedOperation.OperationType, MetadataPersistedOperation::new);
                map(StreamSegmentMapOperation.OperationType, StreamSegmentMapOperation::new);
                map(BatchMapOperation.OperationType, BatchMapOperation::new);
            }
            catch (StreamingException se) {
                throw new ExceptionInInitializerError(se);
            }
        }

        public void map(byte operationType, OperationConstructor constructor) throws StreamingException {
            synchronized (constructors) {
                if (constructors.containsKey(operationType)) {
                    throw new StreamingException(String.format("Duplicate Operation Type found: %d.", operationType));
                }

                constructors.put(operationType, constructor);
            }
        }

        public Operation create(OperationHeader header, DataInputStream source) throws SerializationException {
            OperationConstructor constructor = constructors.get(header.operationType);
            if (constructor == null) {
                throw new SerializationException("Operation.deserialize", String.format("Invalid Operation Type %d.", header.operationType));
            }

            return constructor.apply(header, source);
        }

        @FunctionalInterface
        private interface OperationConstructor {
            Operation apply(OperationHeader header, DataInputStream source) throws SerializationException;
        }
    }

    // endregion
}
