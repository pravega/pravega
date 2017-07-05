/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.impl.extendeds3;

import com.emc.object.Range;
import com.emc.object.s3.S3Client;
import com.emc.object.s3.S3Exception;
import com.emc.object.s3.S3ObjectMetadata;
import com.emc.object.s3.bean.AccessControlList;
import com.emc.object.s3.bean.CanonicalUser;
import com.emc.object.s3.bean.CopyPartResult;
import com.emc.object.s3.bean.GetObjectResult;
import com.emc.object.s3.bean.Grant;
import com.emc.object.s3.bean.ListObjectsResult;
import com.emc.object.s3.bean.MultipartPartETag;
import com.emc.object.s3.bean.Permission;
import com.emc.object.s3.request.CompleteMultipartUploadRequest;
import com.emc.object.s3.request.CopyPartRequest;
import com.emc.object.s3.request.PutObjectRequest;
import com.emc.object.s3.request.SetObjectAclRequest;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.pravega.common.LoggerHelpers;
import io.pravega.common.io.StreamHelpers;
import io.pravega.common.util.ImmutableDate;
import io.pravega.segmentstore.contracts.BadOffsetException;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.StreamSegmentExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentInformation;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentSealedException;
import io.pravega.segmentstore.storage.SegmentHandle;
import io.pravega.segmentstore.storage.impl.filesystem.IdempotentStorageBase;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.AccessDeniedException;
import java.time.Duration;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import lombok.extern.slf4j.Slf4j;

/**
 * Storage adapter for extended S3 based storage.
 *
 * Each segment is represented as a single Object on the underlying storage.
 *
 * This implementation works under the assumption that data is only appended and never modified.
 * Each block of data has an offset assigned to it and Pravega always writes the same data to the same offset. As a result
 * the only flow when a write call is made to the same offset twice is when ownership of the segment changes
 * from one host to another and both the hosts are writing to it.
 *
 * As PutObject calls to with the same start-offset to an Extended S3 object are idempotent (any attempt to re-write data with the same file offset does not
 * cause any form of inconsistency), fencing is not required.
 *
 * Here is the expected behavior in case of ownership change: both the hosts will keep writing the same data at the same offset till the time the
 * earlier owner gets a notification that it is not the current owner. Once the earlier owner received this notification, it stops writing to the segment.
 */

@Slf4j
public class ExtendedS3Storage extends IdempotentStorageBase {

    //region members

    private final ExtendedS3StorageConfig config;
    private final S3Client client;

    //endregion

    //region constructor

    public ExtendedS3Storage(S3Client client, ExtendedS3StorageConfig config, ExecutorService executor) {
        super(executor);
        Preconditions.checkNotNull(config, "config");
        this.config = config;
        this.client = client;

    }

    //endregion


    //region Storage implementation

    /**
     * Initialize is a no op here as we do not need a locking mechanism in case of file system write.
     * @param containerEpoch The Container Epoch to initialize with (ignored here).
     */
    @Override
    public void initialize(long containerEpoch) {
    }

    @Override
    public CompletableFuture<SegmentHandle> openRead(String streamSegmentName) {
        return supplyAsync(streamSegmentName, () -> syncOpenRead(streamSegmentName));
    }

    @Override
    public CompletableFuture<Integer> read(SegmentHandle handle, long offset, byte[] buffer, int bufferOffset, int
            length, Duration timeout) {
        return supplyAsync(handle.getSegmentName(), () -> syncRead(handle, offset, buffer, bufferOffset, length));
    }

    @Override
    public CompletableFuture<SegmentProperties> getStreamSegmentInfo(String streamSegmentName, Duration timeout) {
        return supplyAsync(streamSegmentName, () -> syncGetStreamSegmentInfo(streamSegmentName));
    }

    @Override
    public CompletableFuture<Boolean> exists(String streamSegmentName, Duration timeout) {
        return supplyAsync(streamSegmentName, () -> syncExists(streamSegmentName));
    }

    @Override
    public CompletableFuture<SegmentHandle> openWrite(String streamSegmentName) {
        return supplyAsync(streamSegmentName, () -> syncOpenWrite(streamSegmentName));
    }

    @Override
    public CompletableFuture<SegmentProperties> create(String streamSegmentName, Duration timeout) {
        return supplyAsync(streamSegmentName, () -> syncCreate(streamSegmentName));
    }

    @Override
    public CompletableFuture<Void> write(SegmentHandle handle, long offset, InputStream data, int length, Duration
            timeout) {
        return supplyAsync(handle.getSegmentName(), () -> syncWrite(handle, offset, data, length));
    }

    @Override
    public CompletableFuture<Void> seal(SegmentHandle handle, Duration timeout) {
        return supplyAsync(handle.getSegmentName(), () -> syncSeal(handle));
    }

    @Override
    public CompletableFuture<Void> concat(SegmentHandle targetHandle, long offset, String sourceSegment, Duration
            timeout) {
        return supplyAsync(targetHandle.getSegmentName(),
                () -> syncConcat(targetHandle, offset, sourceSegment));
    }

    @Override
    public CompletableFuture<Void> delete(SegmentHandle handle, Duration timeout) {
        return supplyAsync(handle.getSegmentName(), () -> syncDelete(handle));
    }

    //endregion

    //region private sync implementation
    private SegmentHandle syncOpenRead(String streamSegmentName) {
        long traceId = LoggerHelpers.traceEnter(log, "openRead", streamSegmentName);

        StreamSegmentInformation info = syncGetStreamSegmentInfo(streamSegmentName);
        ExtendedS3SegmentHandle retHandle = ExtendedS3SegmentHandle.getReadHandle(streamSegmentName);
        LoggerHelpers.traceLeave(log, "openRead", traceId, streamSegmentName);
        return retHandle;
    }

    private SegmentHandle syncOpenWrite(String streamSegmentName) {
        long traceId = LoggerHelpers.traceEnter(log, "openWrite", streamSegmentName);
        StreamSegmentInformation info = syncGetStreamSegmentInfo(streamSegmentName);
        ExtendedS3SegmentHandle retHandle;
        if (info.isSealed()) {
            retHandle = ExtendedS3SegmentHandle.getReadHandle(streamSegmentName);
        } else {
            retHandle = ExtendedS3SegmentHandle.getWriteHandle(streamSegmentName);
        }

        LoggerHelpers.traceLeave(log, "openWrite", traceId);
        return retHandle;
    }

    private int syncRead(SegmentHandle handle, long offset, byte[] buffer, int bufferOffset, int length) throws IOException, StreamSegmentNotExistsException {
        long traceId = LoggerHelpers.traceEnter(log, "read", handle.getSegmentName(), offset, bufferOffset, length);

        if (offset < 0 || bufferOffset < 0 || length < 0) {
            throw new ArrayIndexOutOfBoundsException();
        }

        try (InputStream reader = client.readObjectStream(config.getBucket(),
                config.getRoot() + handle.getSegmentName(), Range.fromOffsetLength(offset, length))) {

            if (reader == null) {
                throw new StreamSegmentNotExistsException(handle.getSegmentName());
            }

            int bytesRead = StreamHelpers.readAll(reader, buffer, bufferOffset, length);

            LoggerHelpers.traceLeave(log, "read", traceId, bytesRead);
            return length;
        }
    }

    private StreamSegmentInformation syncGetStreamSegmentInfo(String streamSegmentName) {
        long traceId = LoggerHelpers.traceEnter(log, "getStreamSegmentInfo", streamSegmentName);
        S3ObjectMetadata result = client.getObjectMetadata(config.getBucket(),
                config.getRoot() + streamSegmentName);

        AccessControlList acls = client.getObjectAcl(config.getBucket(), config.getRoot() + streamSegmentName);

        boolean canWrite = acls.getGrants().stream().anyMatch((grant) -> grant.getPermission().compareTo(Permission.WRITE) >= 0);

        StreamSegmentInformation information = new StreamSegmentInformation(streamSegmentName,
                result.getContentLength(), !canWrite, false,
                new ImmutableDate(result.getLastModified().toInstant().toEpochMilli()));
        LoggerHelpers.traceLeave(log, "getStreamSegmentInfo", traceId, streamSegmentName);
        return information;
    }

    private boolean syncExists(String streamSegmentName) {
        ListObjectsResult result = null;
        try {
            result = client.listObjects(config.getBucket(), config.getRoot() + streamSegmentName);
            return !result.getObjects().isEmpty();
        } catch (S3Exception e) {
            if ( e.getErrorCode().equals("NoSuchKey")) {
                return false;
            } else {
                throw e;
            }
        }
    }

    private SegmentProperties syncCreate(String streamSegmentName) throws StreamSegmentExistsException {
        long traceId = LoggerHelpers.traceEnter(log, "create", streamSegmentName);

        if (!client.listObjects(config.getBucket(), config.getRoot() + streamSegmentName).getObjects().isEmpty()) {
            throw new StreamSegmentExistsException(streamSegmentName);
        }

        S3ObjectMetadata metadata = new S3ObjectMetadata();
        metadata.setContentLength((long) 0);

        PutObjectRequest request = new PutObjectRequest(config.getBucket(),
                config.getRoot() + streamSegmentName,
                (Object) null);

        AccessControlList acl = new AccessControlList();
        acl.addGrants(new Grant[]{
                new Grant(new CanonicalUser(config.getAccessKey(), config.getAccessKey()),
                        Permission.FULL_CONTROL)
        });

        request.setAcl(acl);

        client.putObject(request);

        LoggerHelpers.traceLeave(log, "create", traceId);
        return syncGetStreamSegmentInfo(streamSegmentName);
    }

    private Void syncWrite(SegmentHandle handle, long offset, InputStream data, int length) throws StreamSegmentSealedException, BadOffsetException {
        long traceId = LoggerHelpers.traceEnter(log, "write", handle.getSegmentName(), offset, length);

        Preconditions.checkArgument(!handle.isReadOnly(), "handle must not be read-only.");

        SegmentProperties si = syncGetStreamSegmentInfo(handle.getSegmentName());

        if (si.isSealed()) {
            throw new StreamSegmentSealedException(handle.getSegmentName());
        }

        if (si.getLength() != offset) {
            throw new BadOffsetException(handle.getSegmentName(), si.getLength(), offset);
        }

        client.putObject(this.config.getBucket(), this.config.getRoot() + handle.getSegmentName(),
                Range.fromOffsetLength(offset, length), data);
        LoggerHelpers.traceLeave(log, "write", traceId);
        return null;
    }

    private Void syncSeal(SegmentHandle handle) {
        long traceId = LoggerHelpers.traceEnter(log, "seal", handle.getSegmentName());

        Preconditions.checkArgument(!handle.isReadOnly(), "handle must not be read-only.");

        AccessControlList acl = client.getObjectAcl(config.getBucket(),
                config.getRoot() + handle.getSegmentName());
        acl.getGrants().clear();
        acl.addGrants(new Grant[]{new Grant(new CanonicalUser(config.getAccessKey(), config.getAccessKey()),
                Permission.READ)});

        client.setObjectAcl(
                new SetObjectAclRequest(config.getBucket(), config.getRoot() + handle.getSegmentName()).withAcl(acl));
        LoggerHelpers.traceLeave(log, "seal", traceId);
        return null;
    }

    private Void syncConcat(SegmentHandle targetHandle, long offset, String sourceSegment) throws StreamSegmentNotExistsException {
        long traceId = LoggerHelpers.traceEnter(log, "concat", targetHandle.getSegmentName(), offset, sourceSegment);

        SortedSet<MultipartPartETag> partEtags = new TreeSet<>();
        String targetPath = config.getRoot() + targetHandle.getSegmentName();
        String uploadId = client.initiateMultipartUpload(config.getBucket(), targetPath);

        // check whether the target exists
        if (!syncExists(targetHandle.getSegmentName())) {
            throw new StreamSegmentNotExistsException(targetHandle.getSegmentName());
        }
        // check whether the source is sealed
        SegmentProperties si = syncGetStreamSegmentInfo(sourceSegment);
        Preconditions.checkState(si.isSealed(), "Cannot concat segment '%s' into '%s' because it is not sealed.", sourceSegment, targetHandle.getSegmentName());

        //Upload the first part
        CopyPartRequest copyRequest = new CopyPartRequest(config.getBucket(),
                targetPath,
                config.getBucket(),
                targetPath,
                uploadId,
                1).withSourceRange(Range.fromOffsetLength(0, offset));
        CopyPartResult copyResult = client.copyPart(copyRequest);

        partEtags.add(new MultipartPartETag(copyResult.getPartNumber(), copyResult.getETag()));

        //Upload the second part
        S3ObjectMetadata metadataResult = client.getObjectMetadata(config.getBucket(),
                config.getRoot() + sourceSegment);
        long objectSize = metadataResult.getContentLength(); // in bytes

        copyRequest = new CopyPartRequest(config.getBucket(),
                config.getRoot() + sourceSegment,
                config.getBucket(),
                targetPath,
                uploadId,
                2).withSourceRange(Range.fromOffsetLength(0, objectSize));

        copyResult = client.copyPart(copyRequest);
        partEtags.add(new MultipartPartETag(copyResult.getPartNumber(), copyResult.getETag()));

        //Close the upload
        client.completeMultipartUpload(new CompleteMultipartUploadRequest(config.getBucket(),
                targetPath, uploadId).withParts(partEtags));

        client.deleteObject(config.getBucket(), config.getRoot() + sourceSegment);
        LoggerHelpers.traceLeave(log, "concat", traceId);

        return null;
    }

    private Void syncDelete(SegmentHandle handle) {

        client.deleteObject(config.getBucket(), config.getRoot() + handle.getSegmentName());
        return null;
    }

    @Override
    protected Throwable translateException(String segmentName, Throwable e) {
        Throwable retVal = e;

        if (e instanceof S3Exception && !Strings.isNullOrEmpty(((S3Exception) e).getErrorCode())) {
            if (((S3Exception) e).getErrorCode().equals("NoSuchKey")) {
                retVal = new StreamSegmentNotExistsException(segmentName);
            }
            if (((S3Exception) e).getErrorCode().equals("InvalidRange")) {
                retVal = new IllegalArgumentException(segmentName, e);
            }
        }

        if (e instanceof AccessDeniedException) {
            retVal = new StreamSegmentSealedException(segmentName, e);
        }

        return retVal;
    }

    //endregion

}
