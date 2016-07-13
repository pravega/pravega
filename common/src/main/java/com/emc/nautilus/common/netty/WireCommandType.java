package com.emc.nautilus.common.netty;

import java.io.DataInput;
import java.io.IOException;

import com.emc.nautilus.common.netty.WireCommands.AppendSetup;
import com.emc.nautilus.common.netty.WireCommands.BatchCreated;
import com.emc.nautilus.common.netty.WireCommands.BatchMerged;
import com.emc.nautilus.common.netty.WireCommands.Constructor;
import com.emc.nautilus.common.netty.WireCommands.CreateBatch;
import com.emc.nautilus.common.netty.WireCommands.CreateSegment;
import com.emc.nautilus.common.netty.WireCommands.DataAppended;
import com.emc.nautilus.common.netty.WireCommands.DeleteSegment;
import com.emc.nautilus.common.netty.WireCommands.GetStreamSegmentInfo;
import com.emc.nautilus.common.netty.WireCommands.KeepAlive;
import com.emc.nautilus.common.netty.WireCommands.MergeBatch;
import com.emc.nautilus.common.netty.WireCommands.NoSuchBatch;
import com.emc.nautilus.common.netty.WireCommands.NoSuchSegment;
import com.emc.nautilus.common.netty.WireCommands.ReadSegment;
import com.emc.nautilus.common.netty.WireCommands.SealSegment;
import com.emc.nautilus.common.netty.WireCommands.SegmentAlreadyExists;
import com.emc.nautilus.common.netty.WireCommands.SegmentCreated;
import com.emc.nautilus.common.netty.WireCommands.SegmentDeleted;
import com.emc.nautilus.common.netty.WireCommands.SegmentIsSealed;
import com.emc.nautilus.common.netty.WireCommands.SegmentSealed;
import com.emc.nautilus.common.netty.WireCommands.SetupAppend;
import com.emc.nautilus.common.netty.WireCommands.StreamSegmentInfo;
import com.emc.nautilus.common.netty.WireCommands.WrongHost;

enum WireCommandType {
    WRONG_HOST(0, WrongHost::readFrom),
    SEGMENT_IS_SEALED(-1, SegmentIsSealed::readFrom),
    SEGMENT_ALREADY_EXISTS(-3, SegmentAlreadyExists::readFrom),
    NO_SUCH_SEGMENT(-4, NoSuchSegment::readFrom),
    NO_SUCH_BATCH(-5, NoSuchBatch::readFrom),

    SETUP_APPEND(1, SetupAppend::readFrom),
    APPEND_SETUP(2, AppendSetup::readFrom),

    APPEND_DATA(-100, null), // Splits into the two below for the wire.
    APPEND_DATA_HEADER(3, null), // Handled in the encoder/decoder directly
    APPEND_DATA_FOOTER(4, null), // Handled in the encoder/decoder directly
    DATA_APPENDED(5, DataAppended::readFrom),

    READ_SEGMENT(8, ReadSegment::readFrom),
    SEGMENT_READ(9, null), // Handled in the encoder/decoder directly

    GET_STREAM_SEGMENT_INFO(10, GetStreamSegmentInfo::readFrom),
    STREAM_SEGMENT_INFO(11, StreamSegmentInfo::readFrom),

    CREATE_SEGMENT(12, CreateSegment::readFrom),
    SEGMENT_CREATED(13, SegmentCreated::readFrom),

    CREATE_BATCH(14, CreateBatch::readFrom),
    BATCH_CREATED(15, BatchCreated::readFrom),

    MERGE_BATCH(16, MergeBatch::readFrom),
    BATCH_MERGED(17, BatchMerged::readFrom),

    SEAL_SEGMENT(18, SealSegment::readFrom),
    SEGMENT_SEALED(19, SegmentSealed::readFrom),

    DELETE_SEGMENT(20, DeleteSegment::readFrom),
    SEGMENT_DELETED(21, SegmentDeleted::readFrom),

    KEEP_ALIVE(100, KeepAlive::readFrom);

    private final int code;
    private final Constructor factory;

    WireCommandType(int code, Constructor factory) {
        this.code = code;
        this.factory = factory;
    }

    public int getCode() {
        return code;
    }

    public WireCommand readFrom(DataInput in) throws IOException {
        return factory.readFrom(in);
    }
}