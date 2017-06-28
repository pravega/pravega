/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.impl.exts3;

import com.emc.object.Range;
import com.emc.object.s3.S3Client;
import com.emc.object.s3.S3Config;
import com.emc.object.s3.S3Exception;
import com.emc.object.s3.S3ObjectMetadata;
import com.emc.object.s3.bean.AccessControlList;
import com.emc.object.s3.bean.CanonicalUser;
import com.emc.object.s3.bean.CopyPartResult;
import com.emc.object.s3.bean.GetObjectResult;
import com.emc.object.s3.bean.Grant;
import com.emc.object.s3.bean.MultipartPartETag;
import com.emc.object.s3.bean.Permission;
import com.emc.object.s3.jersey.S3JerseyClient;
import com.emc.object.s3.request.CompleteMultipartUploadRequest;
import com.emc.object.s3.request.CopyPartRequest;
import com.emc.object.s3.request.PutObjectRequest;
import com.emc.object.s3.request.SetObjectAclRequest;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.pravega.common.util.ImmutableDate;
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
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import lombok.extern.slf4j.Slf4j;

/**
 * Storage adapter for extended S3 based Tier2.
 *
 * Each segment is represented as a single Object on the underlying storage.
 *
 * Approach to fencing:
 *
 * This implementation works under the assumption that data is only appended and never modified.
 * Each block of data has an offset assigned to it and Pravega always writes the same data to the same offset.
 *
 * With this assumption the only flow when a write call is made to the same offset twice is when ownership of the
 * segment changes from one host to another and both the hosts are writing to it.
 *
 * As PutObject calls to with the same start-offset to an Exts3 object are idempotent (any attempt to re-write data with the same file offset does not
 * cause any form of inconsistency), fencing is not required.
 *
 *
 * In the absence of locking this is the expected behavior in case of ownership change: both the hosts will keep
 * writing the same data at the same offset till the time the earlier owner gets a notification that it is not the
 * current owner. Once the earlier owner received this notification, it stops writing to the segment.
 */

@Slf4j
public class ExtS3Storage implements Storage {

    //region members

    private final ExtS3StorageConfig config;
    private final ExecutorService executor;
    private S3Client client = null;

    //endregion

    //region constructor

    public ExtS3Storage(ExtS3StorageConfig config, ExecutorService executor) {
        Preconditions.checkNotNull(config, "config");
        Preconditions.checkNotNull(executor, "executor");
        this.config = config;
        this.executor = executor;

    }

    //endregion


    //region Storage implementation

    /**
     * Initialize is a no op here as we do not need a locking mechanism in case of file system write.
     * @param containerEpoch The Container Epoch to initialize with (ignored here).
     */
    @Override
    public void initialize(long containerEpoch) {
        if (client == null) {
            S3Config exts3Config = new S3Config(config.getExts3Url())
                    .withIdentity(config.getExts3AccessKey())
                    .withSecretKey(config.getExts3SecretKey());

            client = new S3JerseyClient(exts3Config);
        }
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

    //region AutoClosable

    @Override
    public void close() {

    }

    //endregion

    //region private sync implementation
    private SegmentHandle syncOpenRead(String streamSegmentName) {
        log.trace("Opening {} for read.", streamSegmentName);

        StreamSegmentInformation info = syncGetStreamSegmentInfo(streamSegmentName);
        ExtS3SegmentHandle retHandle = ExtS3SegmentHandle.getReadHandle(streamSegmentName);
        log.trace("Created read handle for segment {} ", streamSegmentName);
        return retHandle;
    }

    private SegmentHandle syncOpenWrite(String streamSegmentName) {
        log.trace("Opening {} for write.", streamSegmentName);
        StreamSegmentInformation info = syncGetStreamSegmentInfo(streamSegmentName);
        ExtS3SegmentHandle retHandle;
        if (info.isSealed()) {
            retHandle = ExtS3SegmentHandle.getReadHandle(streamSegmentName);
        } else {
            retHandle = ExtS3SegmentHandle.getWriteHandle(streamSegmentName);
        }

        log.trace("Created read handle for segment {} ", streamSegmentName);
        return retHandle;
    }

    private int syncRead(SegmentHandle handle, long offset, byte[] buffer, int bufferOffset, int length) throws IOException {
        log.info("Creating a inputstream at offset {} for stream {}", offset, handle.getSegmentName());

        if (offset < 0 || bufferOffset < 0 || length < 0) {
            throw new CompletionException(new ArrayIndexOutOfBoundsException());
        }

        try (InputStream reader = client.readObjectStream(config.getExts3Bucket(),
                config.getExts3Root() + handle.getSegmentName(), Range.fromOffsetLength(offset, length))) {

            if (reader == null) {
                log.info("Object does not exist {} in bucket {} ", config.getExts3Root() + handle.getSegmentName(),
                        config.getExts3Bucket());

                throw new CompletionException(new StreamSegmentNotExistsException(handle.getSegmentName(), null));
            }

            int originalLength = length;

            while (length != 0) {
                log.info("Reading {} ", length);
                int bytesRead = reader.read(buffer, bufferOffset, length);
                log.info("Read {} bytes out of requested {} from segment {}", bytesRead, length,
                        handle.getSegmentName());
                length -= bytesRead;
                bufferOffset += bytesRead;
            }
            return originalLength;
        }
    }

    private StreamSegmentInformation syncGetStreamSegmentInfo(String streamSegmentName) {
        S3ObjectMetadata result = client.getObjectMetadata(config.getExts3Bucket(),
                config.getExts3Root() + streamSegmentName);

        AccessControlList acls = client.getObjectAcl(config.getExts3Bucket(), config.getExts3Root() + streamSegmentName);

        boolean canWrite = false;
        canWrite = acls.getGrants().stream().filter((grant) -> {
            return grant.getPermission().compareTo(Permission.WRITE) >= 0;
        }).count() > 0;

        StreamSegmentInformation information = new StreamSegmentInformation(streamSegmentName,
                result.getContentLength(), !canWrite, false,
                new ImmutableDate(result.getLastModified().toInstant().toEpochMilli()));
        return information;
    }

    private boolean syncExists(String streamSegmentName) {
        GetObjectResult<InputStream> result = null;
        try {
            result = client.getObject(config.getExts3Bucket(), config.getExts3Root() + streamSegmentName);
        } catch (Exception e) {
            log.warn("Exception {} observed while getting segment info {}", e.getMessage(), streamSegmentName);
        }
        return result != null;
    }

    private SegmentProperties syncCreate(String streamSegmentName) {
        log.info("Creating Segment {}", streamSegmentName);

        if (client.listObjects(config.getExts3Bucket(), config.getExts3Root() + streamSegmentName)
                  .getObjects().size() != 0) {
            throw new CompletionException(new StreamSegmentExistsException(streamSegmentName));
        }

        S3ObjectMetadata metadata = new S3ObjectMetadata();
        metadata.setContentLength((long) 0);

        PutObjectRequest request = new PutObjectRequest(config.getExts3Bucket(),
                config.getExts3Root() + streamSegmentName,
                (Object) null);

        AccessControlList acl = new AccessControlList();
        acl.addGrants(new Grant[]{
                new Grant(new CanonicalUser(config.getExts3AccessKey(), config.getExts3AccessKey()),
                        Permission.FULL_CONTROL)
        });

        request.setAcl(acl);

        client.putObject(request);

        log.info("Created Segment {}", streamSegmentName);
        return syncGetStreamSegmentInfo(streamSegmentName);
    }

    private Void syncWrite(SegmentHandle handle, long offset, InputStream data, int length) throws StreamSegmentSealedException {
        log.trace("Writing {} to segment {} at offset {}", length, handle.getSegmentName(), offset);

        if (handle.isReadOnly()) {
            throw new IllegalArgumentException(handle.getSegmentName());
        }

        SegmentProperties si = syncGetStreamSegmentInfo(handle.getSegmentName());

        if (si.isSealed()) {
            throw new StreamSegmentSealedException(handle.getSegmentName());
        }

        client.putObject(this.config.getExts3Bucket(), this.config.getExts3Root() + handle.getSegmentName(),
                Range.fromOffsetLength(offset, length), data);
        return null;
    }

    private Void syncSeal(SegmentHandle handle) {

        if (handle.isReadOnly()) {
            log.info("Seal called on a read handle for segment {}", handle.getSegmentName());
            throw new IllegalArgumentException(handle.getSegmentName());
        }

        AccessControlList acl = client.getObjectAcl(config.getExts3Bucket(),
                config.getExts3Root() + handle.getSegmentName());
        acl.getGrants().clear();
        acl.addGrants(new Grant[]{new Grant(new CanonicalUser(config.getExts3AccessKey(), config.getExts3AccessKey()),
                Permission.READ)});

        client.setObjectAcl(
                new SetObjectAclRequest(config.getExts3Bucket(), config.getExts3Root() + handle.getSegmentName()).withAcl(acl));
        log.info("Successfully sealed segment {}", handle.getSegmentName());
        return null;
    }

    private Void syncConcat(SegmentHandle targetHandle, long offset, String sourceSegment) throws StreamSegmentNotExistsException {

        SortedSet<MultipartPartETag> partEtags = new TreeSet<>();
        String targetPath = config.getExts3Root() + targetHandle.getSegmentName();
        String uploadId = client.initiateMultipartUpload(config.getExts3Bucket(), targetPath);

        // check whether the target exists
        if (!syncExists(targetHandle.getSegmentName())) {
            throw new StreamSegmentNotExistsException(targetHandle.getSegmentName());
        }
        // check whether the source is sealed
        SegmentProperties si = syncGetStreamSegmentInfo(sourceSegment);
        if (!si.isSealed()) {
            throw new IllegalStateException(sourceSegment);
        }

        //Upload the first part
        CopyPartRequest copyRequest = new CopyPartRequest(config.getExts3Bucket(),
                targetPath,
                config.getExts3Bucket(),
                targetPath,
                uploadId,
                1).withSourceRange(Range.fromOffsetLength(0, offset));
        CopyPartResult copyResult = client.copyPart(copyRequest);

        partEtags.add(new MultipartPartETag(copyResult.getPartNumber(), copyResult.getETag()));

        //Upload the second part
        S3ObjectMetadata metadataResult = client.getObjectMetadata(config.getExts3Bucket(),
                config.getExts3Root() + sourceSegment);
        long objectSize = metadataResult.getContentLength(); // in bytes

        copyRequest = new CopyPartRequest(config.getExts3Bucket(),
                config.getExts3Root() + sourceSegment,
                config.getExts3Bucket(),
                targetPath,
                uploadId,
                2).withSourceRange(Range.fromOffsetLength(0, objectSize));

        copyResult = client.copyPart(copyRequest);
        partEtags.add(new MultipartPartETag(copyResult.getPartNumber(), copyResult.getETag()));

        //Close the upload
        client.completeMultipartUpload(new CompleteMultipartUploadRequest(config.getExts3Bucket(),
                targetPath, uploadId).withParts(partEtags));

        si = syncGetStreamSegmentInfo(targetHandle.getSegmentName());
        log.trace("Properties after concat completion : length is {} ", si.getLength());

        client.deleteObject(config.getExts3Bucket(), config.getExts3Root() + sourceSegment);

        return null;
    }

    private Void syncDelete(SegmentHandle handle) {

        client.deleteObject(config.getExts3Bucket(), config.getExts3Root() + handle.getSegmentName());
        return null;
    }

    /**
     * Executes the given supplier asynchronously and returns a Future that will be completed with the result.
     */
    private <R> CompletableFuture<R> supplyAsync(String segmentName, Callable<R> operation) {
        CompletableFuture<R> result = new CompletableFuture<>();
        this.executor.execute(() -> {
            try {
                result.complete(operation.call());
            } catch (Exception e) {
                handleException(e, segmentName, result);
            }
        });

        return result;
    }

    private <R> void handleException(Exception e, String segmentName, CompletableFuture<R> result) {
        result.completeExceptionally(translateException(segmentName, e));
    }

    private Exception translateException(String segmentName, Exception e) {
        Exception retVal = e;

        if (e instanceof S3Exception && !Strings.isNullOrEmpty(((S3Exception) e).getErrorCode())) {
            if (((S3Exception) e).getErrorCode().equals("NoSuchKey")) {
                retVal = new StreamSegmentNotExistsException(segmentName);
            }
            if (((S3Exception) e).getErrorCode().equals("InvalidRange")) {
                retVal = new IllegalArgumentException(segmentName, e);
            }
        }

        if (e instanceof IndexOutOfBoundsException || e instanceof ArrayIndexOutOfBoundsException) {
            retVal = new IllegalArgumentException(e.getMessage());
        }

        if (e instanceof AccessDeniedException) {
            retVal = new StreamSegmentSealedException(segmentName, e);
        }

        return retVal;
    }

    //endregion

    //region testing entry

    @VisibleForTesting
    public void setClient(S3JerseyClient client) {
        this.client = client;
    }

    //endregion
}
