package com.emc.logservice.server.logs;

import com.emc.logservice.common.ByteArraySegment;
import com.emc.logservice.common.IteratorWithException;

import java.io.*;
import java.util.NoSuchElementException;

/**
 * Helps serialize entries into fixed-size batches. Allows writing multiple records per frame, as well as splitting a record
 * across multiple frames.
 */
public class DataFrame {
    //region Members

    private static final byte CurrentVersion = 0;
    private final int MinEntryLengthNeeded = EntryHeader.HeaderSize + 1;
    private final ByteArraySegment data;
    private FrameHeader header;
    private ByteArraySegment contents;
    private long frameSequence; // Assigned by DataFrameLog - no need to serialize.

    private int writeEntryStartIndex;
    private EntryHeader writeEntryHeader;
    private int writePosition;
    private boolean sealed;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the DataFrame class with given maximum size and start magic value.
     *
     * @param previousFrameSequence The offset (within the log) of the previous Data Frame. When reading, comparing this
     *                              number with the actual offset from the previous frame ensures that the frames are
     *                              read in order.
     * @param maxSize               The maximum size of the frame, including Frame Header and other control structures
     *                              that the frame may use to organize records.
     * @throws IllegalArgumentException When the value for startMagic is invalid.
     */
    public DataFrame(long previousFrameSequence, int maxSize) {
        this.data = new ByteArraySegment(new byte[maxSize]);
        this.writeEntryStartIndex = -1;
        this.writePosition = 0;
        this.sealed = false;

        formatForWriting(previousFrameSequence);
    }

    /**
     * Creates a new instance of the DataFrame class using the given byte array as serialization source.
     *
     * @param source The source byte array.
     * @throws SerializationException If the source cannot be deserialized into a DataFrame.
     * @throws NullPointerException   If the source is null.
     */
    public DataFrame(byte[] source) throws SerializationException {
        this(new ByteArraySegment(source, 0, source.length));
    }

    /**
     * Creates a new instance of the DataFrame class using the given byte array as serialization source.
     *
     * @param source The source ByteArraySegment.
     * @throws SerializationException If the source cannot be deserialized into a DataFrame.
     * @throws NullPointerException   If the source is null.
     */
    public DataFrame(ByteArraySegment source) throws SerializationException {
        this.data = source.asReadOnly();
        this.writeEntryStartIndex = -1;
        this.writePosition = -1;
        this.sealed = true;
        parse();
    }

    //endregion

    //region Properties

    /**
     * Gets a value indicating the Frame Sequence Number within its serialization chain.
     * This value is not serialized with the data frame, rather it is assigned by the DataFrameBuilder or DataFrameReader
     * when constructing frames.
     *
     * @return
     */
    public long getFrameSequence() {
        return this.frameSequence;
    }

    /**
     * Sets the Frame Sequence Number.
     * This value is not serialized with the data frame, rather it is assigned by the DataFrameBuilder or DataFrameReader
     * when constructing frames.
     *
     * @param value The Sequence Number to set.
     */
    public void setFrameSequence(long value) {
        this.frameSequence = value;
    }

    /**
     * Gets a value indicating the the Sequence Number of the previous Frame in the Log. When reading frames, comparing
     * this value with the previous frame's value ensures the frames are read in order.
     *
     * @return
     */
    public long getPreviousFrameSequence() {
        return this.header.getPreviousFrameSequence();
    }

    /**
     * Gets a value indicating the length, in bytes, of the frame, including the header, contents and any other control
     * structures needed to serialize the frame.
     * When creating new frames (write mode), this value may be less than the 'maxLength' provided in the constructor.
     * When reading frames from a source (read mode), this value may be less than the size of the source.
     * This value is serialized with the frame.
     *
     * @return
     */
    public int getLength() {
        return this.header.getSerializationLength() + this.header.getContentLength();
    }

    /**
     * Returns a new InputStream representing the serialized form of this frame.
     *
     * @return
     */
    public InputStream getData() {
        if (this.data.isReadOnly()) {
            return this.data.getReader();
        }
        else {
            // We have just created this frame. Only return the segment of the buffer that contains data.
            return this.data.getReader(0, this.header.getSerializationLength() + this.header.getContentLength());
        }
    }

    /**
     * Gets a value indicating whether the DataFrame is empty (if it has no entries).
     *
     * @return
     */
    public boolean isEmpty() {
        // TODO: should we check for anything else?
        return this.header.getContentLength() == 0;
    }

    /**
     * Gets a value indicating whether the DataFrame is sealed.
     *
     * @return
     */
    public boolean isSealed() {
        return this.sealed || this.contents.isReadOnly();
    }

    //endregion

    //region Writing

    /**
     * Indicates that a new DataFrame Entry should be opened.
     *
     * @param firstRecordEntry If true, this entry will be marked as the first entry in a record (records can be split
     *                         across multiple frames, and this helps ensure that, upon reading, the records are recomposed
     *                         starting with the right Data Frame Entry).
     * @return True if we were able to start an entry in this Frame, or false otherwise (this happens in case the frame is full).
     * If the frame is full, the new entry will need to be started on a new Frame.
     * @throws IllegalStateException If the entry is sealed.
     */
    public boolean startNewEntry(boolean firstRecordEntry) {
        if (this.sealed) {
            throw new IllegalStateException("WriteFrame is sealed.");
        }

        endEntry(true);

        if (getAvailableLength() < MinEntryLengthNeeded) {
            // If we cannot fit at least entry header + 1 byte, we cannot record anything in this write frame anymore.
            return false;
        }

        this.writeEntryStartIndex = this.writePosition;
        this.writeEntryHeader = new EntryHeader(this.contents.subSegment(this.writePosition, EntryHeader.HeaderSize));
        this.writeEntryHeader.setFirstRecordEntry(firstRecordEntry);
        this.writePosition += EntryHeader.HeaderSize;
        return true;
    }

    /**
     * Discards the currently started entry and deletes any data associated with it.
     */
    public void discardEntry() {
        if (this.writeEntryStartIndex < 0) {
            // Nothing to do.
        }

        // Revert back to where we began the current entry.
        this.writePosition = this.writeEntryStartIndex;
        this.writeEntryStartIndex = -1;
        this.writeEntryHeader = null;
    }

    /**
     * Indicates that the currently open DataFrame Entry can be ended.
     *
     * @param endOfRecord If true, this entry will be marked as the last entry in a record (records can be split across
     *                    multiple frames, and this helps ensure that, upon read, the records are recomposed ending with
     *                    the right DataFrame Entry).
     * @return True if we have any more space (to start a new record) in this DataFrame, false otherwise.
     */
    public boolean endEntry(boolean endOfRecord) {
        // Check to see if we actually have an entry started.
        if (this.writeEntryStartIndex >= 0) {
            int entryLength = this.writePosition - this.writeEntryStartIndex - EntryHeader.HeaderSize;

            //TODO: assert that entryLength >= 0
            this.writeEntryHeader.setEntryLength(entryLength);
            this.writeEntryHeader.setLastRecordEntry(endOfRecord);
            this.writeEntryHeader.serialize();
            this.writeEntryHeader = null;
            this.writeEntryStartIndex = -1;
        }

        // Check to see if we have enough room to begin another entry.
        return getAvailableLength() >= MinEntryLengthNeeded;
    }

    /**
     * Appends a byte to the DataFrame.
     *
     * @param b The byte to append.
     * @return The number of bytes written. If zero, then the frame is full and we cannot write anything anymore.
     * @throws IllegalStateException If the frame is sealed or no entry has been started.
     */
    public int append(byte b) {
        ensureAppendConditions();
        if (getAvailableLength() >= 1) {
            this.contents.set(writePosition, b);
            writePosition++;
            return 1;
        }
        else {
            // Current DataFrame is full. we can't write anymore.
            return 0;
        }
    }

    /**
     * Appends the contents of the ByteArraySegment to the DataFrame.
     *
     * @param data The ByteArraySegment to append.
     * @return The number of bytes written. If less than the length of the ByteArraySegment, the frame is full and cannot
     * write anything anymore. The remaining bytes will need to be written to a new frame.
     * @throws IllegalStateException If the frame is sealed or no entry has been started.
     */
    public int append(ByteArraySegment data) {
        ensureAppendConditions();

        int actualLength = Math.min(data.getLength(), getAvailableLength());
        if (actualLength > 0) {
            this.contents.copyFrom(data, writePosition, actualLength);
            writePosition += actualLength;
        }

        return actualLength;
    }

    /**
     * Seals the frame for writing. After this method returns, no more modifications are allowed on this DataFrame.
     * This method has no effect if the Frame is read-only if it is already sealed.
     *
     * @throws IllegalStateException If an open entry exists (entries must be closed prior to sealing).
     */
    public void seal() throws SerializationException {
        if (!this.sealed && !this.contents.isReadOnly()) {
            if (writeEntryStartIndex >= 0) {
                throw new IllegalStateException("An open entry exists. All open entries must be closed prior to sealing.");
            }

            this.header.setContentLength(writePosition);
            this.header.commit();
            this.sealed = true;
        }
    }

    /**
     * Calculates the number of bytes available in the frame for writing.
     *
     * @return
     */
    private int getAvailableLength() {
        return this.contents.getLength() - writePosition;
    }

    /**
     * Formats the DataFrame buffer for writing. Creates a FrameHeader and assigns a content.
     *
     * @param previousFrameSequence
     */
    private void formatForWriting(long previousFrameSequence) {
        if (this.header != null || this.contents != null) {
            throw new IllegalStateException("DataFrame already contains data; cannot re-format.");
        }

        int sourceLength = this.data.getLength();
        if (sourceLength <= FrameHeader.SerializationLength) // "<=" because we want to use the DataFrame for at least 1 byte of data.
        {
            throw new IllegalArgumentException(String.format("Insufficient array length. Byte array must have a length of at least %d.", FrameHeader.SerializationLength + 1));
        }

        this.header = new FrameHeader(CurrentVersion, previousFrameSequence, this.data.subSegment(0, FrameHeader.SerializationLength));
        this.contents = this.data.subSegment(FrameHeader.SerializationLength, sourceLength - FrameHeader.SerializationLength);
    }

    private void ensureAppendConditions() {
        if (this.sealed) {
            throw new IllegalStateException("WriteFrame is sealed.");
        }

        if (writeEntryStartIndex < 0) {
            throw new IllegalStateException("No entry started.");
        }
    }

    //endregion

    //region Reading

    /**
     * Gets an Iterator that returns all DataFrameEntries in this Data Frame
     *
     * @return
     */
    public IteratorWithException<DataFrameEntry, SerializationException> getEntries() {
        // The true max length differs based on whether we are still writing this frame or if it's read-only.
        int maxLength = this.writePosition >= 0 ? this.writePosition : this.contents.getLength();
        return new DataFrameEntryIterator(this.contents, this.getFrameSequence(), maxLength);
    }

    private void parse() throws SerializationException {
        // Used for Reading.
        // FrameHeader will validate that the header is indeed a DataFrame header.
        this.header = new FrameHeader(this.data);

        // Check to see that we have enough bytes in the array.
        if (this.data.getLength() < this.header.getSerializationLength() + this.header.getContentLength()) {
            throw new SerializationException("DataFrame.deserialize", String.format("Given buffer has insufficient number of bytes for this DataFrame. Expected %d, actual %d.", this.header.getSerializationLength() + this.header.getContentLength(), this.contents.getLength()));
        }

        this.contents = this.data.subSegment(this.header.getSerializationLength(), this.header.getContentLength());
    }

    //endregion

    //region EntryHeader

    /**
     * Header for a DataFrameEntry.
     */
    private class EntryHeader {
        //region Members

        public static final int HeaderSize = Integer.BYTES + Byte.BYTES; // Length(int) + Flags(byte)
        private static final int FlagsOffset = Integer.BYTES;
        private static final byte FirstEntryMask = 1;
        private static final byte LastEntryMask = 2;
        private int entryLength;
        private boolean firstRecordEntry;
        private boolean lastRecordEntry;
        private ByteArraySegment data;

        //endregion

        //region Constructor

        /**
         * Creates a new instance of the EntryHeader class.
         *
         * @param headerContents The ByteArraySegment to use. If the given ByteArraySegment is ReadOnly, it will be used
         *                       for deserialization, otherwise it will be used to serialize the header later on.
         * @throws IllegalArgumentException If the size of the headerContents ByteArraySegment is incorrect.
         */
        public EntryHeader(ByteArraySegment headerContents) {
            if (headerContents.getLength() != HeaderSize) {
                throw new IllegalArgumentException(String.format("Invalid headerContents size. Expected %d, given %d.", HeaderSize, headerContents.getLength()));
            }

            if (headerContents.isReadOnly()) {
                // We are reading.
                this.entryLength = readInt(headerContents, 0);
                byte flags = headerContents.get(FlagsOffset);
                this.firstRecordEntry = (flags & FirstEntryMask) == FirstEntryMask;
                this.lastRecordEntry = (flags & LastEntryMask) == LastEntryMask;
            }
            else {
                // We are creating a new one.
                this.entryLength = 0;
                this.firstRecordEntry = this.lastRecordEntry = false;
                this.data = headerContents;
            }
        }

        //endregion

        //region Operations

        /**
         * Serializes the contents of this EntryHeader to the ByteArraySegment given during construction.
         *
         * @throws IllegalStateException If this EntryHeader was deserialized (and not new).
         */
        public void serialize() {
            if (this.data == null || this.data.isReadOnly()) {
                throw new IllegalStateException("Cannot serialize a read-only EntryHeader.");
            }

            // Write length.
            writeInt(this.data, 0, this.entryLength);

            // Write flags.
            byte flags = this.firstRecordEntry ? FirstEntryMask : 0;
            flags |= this.lastRecordEntry ? LastEntryMask : 0;
            this.data.set(FlagsOffset, flags);
        }

        /**
         * Gets a value indicating the length of the Entry, in bytes.
         *
         * @return
         */
        public int getEntryLength() {
            return this.entryLength;
        }

        /**
         * Sets the Entry Length.
         *
         * @param value The length of the Entry, in bytes.
         */
        public void setEntryLength(int value) {
            this.entryLength = value;
        }

        /**
         * Gets a value indicating whether this is the first entry for the record.
         *
         * @return
         */
        public boolean isFirstRecordEntry() {
            return this.firstRecordEntry;
        }

        /**
         * Sets whether this is the first entry for the record.
         *
         * @param value
         */
        public void setFirstRecordEntry(boolean value) {
            this.firstRecordEntry = value;
        }

        /**
         * Gets a value indicating whether this is the last entry for the record.
         *
         * @return
         */
        public boolean isLastRecordEntry() {
            return this.lastRecordEntry;
        }

        /**
         * Sets whether this is the last entry for the record.
         *
         * @param value
         */
        public void setLastRecordEntry(boolean value) {
            this.lastRecordEntry = value;
        }

        @Override
        public String toString() {
            return String.format("Length = %d, FirstEntry = %s, LastEntry = %s", getEntryLength(), isFirstRecordEntry(), isLastRecordEntry());
        }

        //endregion

        //region Helpers

        private void writeInt(ByteArraySegment target, int offset, int value) {
            target.setSequence(
                    offset,
                    (byte) (value >>> 24),
                    (byte) (value >>> 16),
                    (byte) (value >>> 8),
                    (byte) value);
        }

        private int readInt(ByteArraySegment source, int position) {
            return (source.get(position) & 0xFF) << 24
                    | (source.get(position + 1) & 0xFF) << 16
                    | (source.get(position + 2) & 0xFF) << 8
                    | (source.get(position + 3) & 0xFF);
        }

        //endregion
    }

    //endregion

    //region FrameHeader

    /**
     * Header for the DataFrame.
     */
    private class FrameHeader {
        //region Members

        public static final int SerializationLength = Byte.BYTES + Long.BYTES + Integer.BYTES + Byte.BYTES;
        private final long previousFrameSequence;
        private final byte version;
        private int contentLength;
        private int actualSerializationLength;
        private ByteArraySegment buffer;

        //endregion

        //region Constructor

        /**
         * Creates a new instance of the FrameHeader class for a writeable frame.
         *
         * @param version               The serialization version for the frame.
         * @param previousFrameSequence The Sequence Number in the Log for the previous Frame.
         * @param target                The target buffer where to write frame contents.
         * @throws NullPointerException     If the target buffer is null.
         * @throws IllegalArgumentException If the target buffer has an incorrect length.
         */
        public FrameHeader(byte version, long previousFrameSequence, ByteArraySegment target) {
            if (target.getLength() != SerializationLength) {
                throw new IllegalArgumentException(String.format("Unexpected length for target buffer. Expected %d, given %d.", SerializationLength, target.getLength()));
            }

            this.version = version;
            this.previousFrameSequence = previousFrameSequence;
            this.actualSerializationLength = SerializationLength;
            this.contentLength = 0;
            this.buffer = target;
        }

        /**
         * Creates a new instance of the FrameHeader class for a read-only frame (deserialization constructor).
         *
         * @param source The source ByteArraySegment to read from.
         * @throws SerializationException If we are unable to deserialize the header.
         */
        public FrameHeader(ByteArraySegment source) throws SerializationException {
            if (source == null || source.getLength() == 0) {
                throw new SerializationException("DataFrame.Header.deserialize", "Null or empty source buffer.");
            }

            try (DataInputStream input = new DataInputStream(source.getReader())) {
                this.version = input.readByte();
                this.actualSerializationLength = SerializationLength; //TODO: this will change based on serialization version.
                if (source.getLength() < this.actualSerializationLength) {
                    throw new SerializationException("DataFrame.Header.deserialize", "DataFrame.Header has insufficient number of bytes given its serialization version.");
                }

                this.previousFrameSequence = input.readLong();
                this.contentLength = input.readInt();
                byte flags = input.readByte();
                decodeFlags(flags, version);
                this.buffer = null;
            }
            catch (IOException ex) {
                throw new SerializationException("DataFrame.Header.deserialize", "Cannot deserialize frame header.", ex);
            }
        }

        //endregion

        //region Operations

        /**
         * Commits (serializes) the contents of the FrameHeader to the ByteArraySegment given during construction.
         *
         * @throws SerializationException If we are unable to serialize the header.
         * @throws IllegalStateException  If this FrameHeader was created from a read-only buffer (it was deserialized).
         */
        public void commit() throws SerializationException {
            if (this.buffer == null || this.buffer.isReadOnly()) {
                throw new IllegalStateException("Cannot commit a read-only FrameHeader");
            }

            // We already checked the size of the target buffer (in the constructor); no need to do it here again.
            try (DataOutputStream output = new DataOutputStream(this.buffer.getWriter())) {
                output.writeByte(this.version);
                output.writeLong(this.previousFrameSequence);
                output.writeInt(this.contentLength);
                output.writeByte(encodeFlags());
            }
            catch (IOException ex) {
                throw new SerializationException("DataFrame.Header.serialize", "Cannot serialize frame header.", ex);
            }
        }

        /**
         * Gets a value indicating the Sequence Number of the previous Frame in the Log.
         *
         * @return
         */
        public long getPreviousFrameSequence() {
            return this.previousFrameSequence;
        }

        /**
         * Gets a value indicating the Serialization Version for the frame.
         *
         * @return
         */
        public byte getVersion() {
            return this.version;
        }

        /**
         * Gets a value indicating the length of the Frame's payload (contents), in bytes.
         *
         * @return
         */
        public int getContentLength() {
            return this.contentLength;
        }

        /**
         * Sets the length of the Frame's payload (contents), in bytes.
         *
         * @param value
         */
        public void setContentLength(int value) {
            this.contentLength = value;
        }

        /**
         * Gets a value indicating the total number of bytes used for serializing this FrameHeader instance.
         *
         * @return
         */
        public int getSerializationLength() {
            return this.actualSerializationLength;
        }

        @Override
        public String toString() {
            return String.format("Version = %d, PrevOffset = %d, ContentLength = %d", getVersion(), getPreviousFrameSequence(), getContentLength());
        }

        private void decodeFlags(byte flags, byte version) {
            // Placeholder method. Nothing to do yet.
        }

        private byte encodeFlags() {
            // Placeholder method. Nothing to do yet.
            return 0;
        }

        //endregion
    }

    //endregion

    //region DataFrameEntry

    /**
     * Represents an Entry in the DataFrame.
     */
    public class DataFrameEntry {
        private final boolean firstRecordEntry;
        private final boolean lastRecordEntry;
        private final boolean lastEntryInDataFrame;
        private final long dataFrameSequence;
        private final ByteArraySegment data;

        /**
         * Creates a new instance of the DataFrameEntry class.
         *
         * @param header               The Header for this entry.
         * @param data                 A ByteArraySegment representing the contents of this frame.
         * @param dataFrameSequence
         * @param lastEntryInDataFrame
         */
        protected DataFrameEntry(EntryHeader header, ByteArraySegment data, long dataFrameSequence, boolean lastEntryInDataFrame) {
            this.firstRecordEntry = header.isFirstRecordEntry();
            this.lastRecordEntry = header.isLastRecordEntry();
            this.lastEntryInDataFrame = lastEntryInDataFrame;
            this.dataFrameSequence = dataFrameSequence;
            this.data = data;
        }

        /**
         * Gets a value indicating whether this is the first entry for a record.
         *
         * @return
         */
        public boolean isFirstRecordEntry() {
            return this.firstRecordEntry;
        }

        /**
         * Gets a value indicating whether this is the last entry for a record.
         *
         * @return
         */
        public boolean isLastRecordEntry() {
            return this.lastRecordEntry;
        }

        /**
         * Gets a value indicating whether this is the last entry in its containing DataFrame.
         *
         * @return
         */
        public boolean isLastEntryInDataFrame() {
            return this.lastEntryInDataFrame;
        }

        /**
         * Gets a value indicating Sequence Number of the containing DataFrame.
         *
         * @return
         */
        public long getDataFrameSequence() {
            return this.dataFrameSequence;
        }

        /**
         * Gets a ByteArraySegment with the contents of the entry.
         *
         * @return
         */
        public ByteArraySegment getData() {
            return this.data;
        }

        @Override
        public String toString() {
            return String.format("DataFrameSN = %d, Size = %d, First = %s, Last = %s, LastInDataFrame = %s", getDataFrameSequence(), getData().getLength(), isFirstRecordEntry(), isLastRecordEntry(), isLastEntryInDataFrame());
        }
    }

    //endregion

    //region DataFrameEntryIterator

    /**
     * Represents an iterator over all entries within a DataFrame.
     */
    private class DataFrameEntryIterator implements IteratorWithException<DataFrameEntry, SerializationException> {
        private final ByteArraySegment contents;
        private final long dataFrameSequence;
        private int currentPosition;
        private int maxLength;

        /**
         * Creates a new instance of the DataFrameEntryIterator class.
         *
         * @param contents          A ByteArraySegment with the contents of the DataFrame.
         * @param dataFrameSequence The Sequence Number of the Data Frame.
         * @param maxLength         The maximum number of bytes to consider reading from the contents.
         */
        public DataFrameEntryIterator(ByteArraySegment contents, long dataFrameSequence, int maxLength) {
            this.contents = contents;
            this.dataFrameSequence = dataFrameSequence;
            this.maxLength = maxLength;
            this.currentPosition = 0;
        }

        //region IteratorWithException Implementation

        @Override
        public boolean hasNext() {
            return this.currentPosition < this.maxLength;
        }

        @Override
        public DataFrameEntry pollNextElement() throws SerializationException {
            if (!hasNext()) {
                throw new NoSuchElementException("Reached the end of the DataFrame.");
            }

            // Integrity check. This means that we have a corrupt frame.
            if (this.currentPosition + EntryHeader.HeaderSize > this.maxLength) {
                throw new SerializationException("DataFrame.deserialize", String.format("Data Frame is corrupt. Expected Entry Header at position %d but it does not fit in Frame's length of %d.", this.currentPosition, this.maxLength));
            }

            // Determine the length of the next record && advance the position by the appropriate amount of bytes.
            EntryHeader header = new EntryHeader(this.contents.subSegment(this.currentPosition, EntryHeader.HeaderSize)); // TODO: fix for versioning
            this.currentPosition += EntryHeader.HeaderSize;

            // Integrity check. This means that we have a corrupt frame.
            if (this.currentPosition + header.getEntryLength() > this.maxLength) {
                throw new SerializationException("DataFrame.deserialize", String.format("Data Frame is corrupt. Found Entry Length %d at position %d which is outside of the Frame's length of %d.", header.getEntryLength(), this.currentPosition - EntryHeader.HeaderSize, this.maxLength));
            }

            // Get the result contents && advance the positions.
            ByteArraySegment resultContents = this.contents.subSegment(this.currentPosition, header.getEntryLength());
            this.currentPosition += header.getEntryLength();
            return new DataFrameEntry(header, resultContents, this.dataFrameSequence, !this.hasNext());
        }

        @Override
        public DataFrameEntry next() {
            try {
                return pollNextElement();
            }
            catch (SerializationException ex) {
                throw new RuntimeException(ex);
            }
        }

        //endregion
    }

    //endregion
}
