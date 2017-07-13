/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.impl.extendeds3;

import com.emc.object.Range;
import com.emc.object.s3.S3Client;
import com.emc.object.s3.S3Exception;
import com.emc.object.s3.S3ObjectMetadata;
import com.emc.object.s3.bean.AccessControlList;
import com.emc.object.s3.bean.CanonicalUser;
import com.emc.object.s3.bean.CopyPartResult;
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
import io.pravega.common.Exceptions;
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
import io.pravega.segmentstore.storage.Storage;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.AccessDeniedException;
import java.time.Duration;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;

/**
 * Storage adapter for extended S3 based storage.
 *
 * Each segment is represented as a single Object on the underlying storage.
 *
 * This implementation works under the assumption that data is only appended and never modified.
 * Each block of data has an initial offset assigned to it. The data and the initial offset is stored in DurableLog.
 * In case of retries, Pravega always writes the same data to the same offset. As a result the only flow when a write
 * call is made to the same offset twice is when ownership of the segment changes from one host to another and both
 * the hosts are writing to it.
 *
 * As PutObject calls with the same start-offset to an Extended S3 object are idempotent (any attempt to re-write
 * data with the same file offset does not cause any form of inconsistency), fencing is not required.
 *
 * ZkSegmentContainerMonitor watches the shared zk entry that contains the segment container ownership information
 * and starts or stops appropriate segment containers locally. Any access to the segment from the new host causes the
 * ownership change.
 *
 * Here is the expected behavior in case of ownership change: both the hosts will keep writing the same data at the
 * same offset till the time the earlier owner gets a notification that it is not the current owner. Once the earlier
 * owner received this notification, it stops writing to the segment.
 *
 * The concat operation is implemented as multi part copy. This ensures that the objects are copied server side.
 * Multi part copy calls are idempotent too. Copying the same object at the same offset multiple times from different
 * hosts does not cause any form of inconsistency.
 *
 */

@Slf4j
public class ExtendedS3Storage implements Storage {

    //region members

    private final ExtendedS3StorageConfig config;
    private final S3Client client;
    private final ExecutorService executor;
    private final AtomicBoolean closed;

    //endregion

    //region constructor

    public ExtendedS3Storage(S3Client client, ExtendedS3StorageConfig config, ExecutorService executor) {
        Preconditions.checkNotNull(config, "config");
        this.config = config;
        this.client = client;
        this.executor = executor;
        this.closed = new AtomicBoolean(false);

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
            /*
             * TODO: This implementation assumes that if S3Client.readObjectStream returns null, then
             * the object does not exist and we throw StreamNotExistsException. The javadoc, however,
             * says that this call returns null in case of 304 and 412 responses. We need to
             * investigate what these responses mean precisely and react accordingly.
             *
             * See https://github.com/pravega/pravega/issues/1549
             */
            if (reader == null) {
                throw new StreamSegmentNotExistsException(handle.getSegmentName());
            }

            int bytesRead = StreamHelpers.readAll(reader, buffer, bufferOffset, length);

            LoggerHelpers.traceLeave(log, "read", traceId, bytesRead);
            return bytesRead;
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
            /*
             * TODO: This implementation is supporting both an empty list and a no such key
             * exception to indicate that the segment doesn't exist. It is trying to be safe,
             * but this is an indication that the behavior is not well understood. We need to
             * investigate the exact behavior we should expect out of this call and react
             * accordingly rather than guess.
             *
             * See https://github.com/pravega/pravega/issues/1559
             */
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

        /* TODO: Default behavior of putObject is to overwrite an existing object. This behavior can cause data loss.
         * Here is one of the scenarios in which data loss is observed:
         * 1. Host A owns the container and gets a create operation. It has not executed the putObject operation yet.
         * 2. Ownership changes and host B becomes the owner of the container. It picks up putObject from the queue, executes it.
         * 3. Host B gets a write operation which executes successfully.
         * 4. Now host A schedules the putObject. This will overwrite the write by host B.
         *
         * The solution for this issue is to implement put-if-absent behavior by using Set-If-None-Match header as described here:
         * http://www.emc.com/techpubs/api/ecs/v3-0-0-0/S3ObjectOperations_createOrUpdateObject_7916bd6f789d0ae0ff39961c0e660d00_ba672412ac371bb6cf4e69291344510e_detail.htm
         * But this does not work. Currently all the calls to putObject API fail if made with reqest.setIfNoneMatch("*").
         * once the issue with extended S3 API is fixed, addition of this one line will ensure put-if-absent semantics.
         * See: https://github.com/pravega/pravega/issues/1564
         */
        client.putObject(request);

        LoggerHelpers.traceLeave(log, "create", traceId);
        return syncGetStreamSegmentInfo(streamSegmentName);
    }

    private Void syncWrite(SegmentHandle handle,
                           long offset,
                           InputStream data,
                           int length) throws StreamSegmentSealedException, BadOffsetException {
        Preconditions.checkArgument(!handle.isReadOnly(), "handle must not be read-only.");

        long traceId = LoggerHelpers.traceEnter(log, "write", handle.getSegmentName(), offset, length);

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
        Preconditions.checkArgument(!handle.isReadOnly(), "handle must not be read-only.");

        long traceId = LoggerHelpers.traceEnter(log, "seal", handle.getSegmentName());

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

    /**
     * The concat is implemented using extended S3 implementation of multipart copy API. Please see here for
     * more detail on multipart copy:
     * http://docs.aws.amazon.com/AmazonS3/latest/dev/CopyingObjctsUsingLLJavaMPUapi.html
     *
     * The multipart copy is an atomic operation. We schedule two parts and commit them atomically using
     * completeMultiPartUpload call. Specifically, to concatenate, we are copying the target segment T and the
     * source segment S to T, so essentially we are doing T <- T + S.
     */
    private Void syncConcat(SegmentHandle targetHandle, long offset, String sourceSegment) throws StreamSegmentNotExistsException {
        Preconditions.checkArgument(!targetHandle.isReadOnly(), "target handle must not be read-only.");
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
        Preconditions.checkState(si.isSealed(), "Cannot concat segment '%s' into '%s' because it is not sealed.",
                sourceSegment, targetHandle.getSegmentName());

        //Copy the first part
        CopyPartRequest copyRequest = new CopyPartRequest(config.getBucket(),
                targetPath,
                config.getBucket(),
                targetPath,
                uploadId,
                1).withSourceRange(Range.fromOffsetLength(0, offset));
        CopyPartResult copyResult = client.copyPart(copyRequest);

        partEtags.add(new MultipartPartETag(copyResult.getPartNumber(), copyResult.getETag()));

        //Copy the second part
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

    private Throwable translateException(String segmentName, Throwable e) {
        Throwable retVal = e;

        if (e instanceof S3Exception && !Strings.isNullOrEmpty(((S3Exception) e).getErrorCode())) {
            if (((S3Exception) e).getErrorCode().equals("NoSuchKey")) {
                retVal = new StreamSegmentNotExistsException(segmentName);
            }

            if (((S3Exception) e).getErrorCode().equals("PreconditionFailed")) {
                retVal = new StreamSegmentExistsException(segmentName);
            }

            if (((S3Exception) e).getErrorCode().equals("InvalidRange")) {
                retVal = new IllegalArgumentException(segmentName, e);
            }
        }

        if (e instanceof IndexOutOfBoundsException) {
            retVal = new ArrayIndexOutOfBoundsException(e.getMessage());
        }

        if (e instanceof AccessDeniedException) {
            retVal = new StreamSegmentSealedException(segmentName, e);
        }

        return retVal;
    }


    /**
     * Executes the given supplier asynchronously and returns a Future that will be completed with the result.
     *
     * @param segmentName   Full name of the StreamSegment.
     * @param operation     The function to execute.
     * @param <R>           Return type of the operation.
     * @return              Instance of the return type of the operation.
     */
    private  <R> CompletableFuture<R> supplyAsync(String segmentName, Callable<R> operation) {
        Exceptions.checkNotClosed(this.closed.get(), this);

        CompletableFuture<R> result = new CompletableFuture<>();
        this.executor.execute(() -> {
            try {
                result.complete(operation.call());
            } catch (Throwable e) {
                handleException(e, segmentName, result);
            }
        });

        return result;
    }

    /**
     * Method defining implementation specific handling of exceptions thrown during call to supplyAsync.
     * @param e             The exception thrown during supplyAsync.
     * @param segmentName   Full name of the StreamSegment.
     * @param result        The CompletableFuture that needs to be responded to.
     * @param <R>           Return type of the operation.
     */
    private <R> void handleException(Throwable e, String segmentName, CompletableFuture<R> result) {
        result.completeExceptionally(translateException(segmentName, e));
    }

    //endregion

    //region AutoClosable

    @Override
    public void close() {
        this.closed.set(true);
    }

    //endregion

}
