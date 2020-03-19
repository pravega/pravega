/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.storage.extendeds3;

import com.emc.object.Range;
import com.emc.object.s3.S3Client;
import com.emc.object.s3.S3Exception;
import com.emc.object.s3.S3ObjectMetadata;
import com.emc.object.s3.bean.AccessControlList;
import com.emc.object.s3.bean.CanonicalUser;
import com.emc.object.s3.bean.CopyPartResult;
import com.emc.object.s3.bean.Grant;
import com.emc.object.s3.bean.MultipartPartETag;
import com.emc.object.s3.bean.Permission;
import com.emc.object.s3.request.CompleteMultipartUploadRequest;
import com.emc.object.s3.request.CopyPartRequest;
import com.emc.object.s3.request.PutObjectRequest;
import com.emc.object.s3.request.SetObjectAclRequest;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.pravega.common.Exceptions;
import io.pravega.common.io.StreamHelpers;
import io.pravega.segmentstore.storage.chunklayer.BaseChunkStorageProvider;
import io.pravega.segmentstore.storage.chunklayer.ChunkHandle;
import io.pravega.segmentstore.storage.chunklayer.ChunkInfo;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpStatus;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.AccessDeniedException;
import java.nio.file.FileAlreadyExistsException;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.Executor;

/**
 *  {@link io.pravega.segmentstore.storage.chunklayer.ChunkStorageProvider} for extended S3 based storage.
 *
 * Each chunk is represented as a single Object on the underlying storage.
 *
 * This implementation works under the assumption that data is only appended and never modified.
 * The concat operation is implemented as multi part copy.
 */

@Slf4j
public class ExtendedS3ChunkStorageProvider extends BaseChunkStorageProvider {

    //region members
    private final ExtendedS3StorageConfig config;
    private final S3Client client;

    //endregion

    //region constructor
    public ExtendedS3ChunkStorageProvider(Executor executor, S3Client client, ExtendedS3StorageConfig config) {
        super(executor);
        this.config = Preconditions.checkNotNull(config, "config");
        this.client = Preconditions.checkNotNull(client, "client");
    }
    //endregion
    //region implementation

    @Override
    protected ChunkHandle doOpenRead(String chunkName) throws IOException, IllegalArgumentException {
        if (!doesExist(chunkName)) {
           throw new FileNotFoundException(chunkName);
        }
        return ChunkHandle.readHandle(chunkName);
    }

    @Override
    protected  ChunkHandle doOpenWrite(String chunkName) throws IOException, IllegalArgumentException {
        if (!doesExist(chunkName)) {
            throw new FileNotFoundException(chunkName);
        }
        return ChunkHandle.writeHandle(chunkName);
    }

    @Override
    protected int doRead(ChunkHandle handle, long fromOffset, int length, byte[] buffer, int bufferOffset) throws IOException, NullPointerException, IndexOutOfBoundsException {
        try {
            if (fromOffset < 0 || bufferOffset < 0 || length < 0) {
                throw new ArrayIndexOutOfBoundsException();
            }

            try (InputStream reader = client.readObjectStream(config.getBucket(),
                    config.getPrefix() + handle.getChunkName(), Range.fromOffsetLength(fromOffset, length))) {
                /*
                 * TODO: This implementation assumes that if S3Client.readObjectStream returns null, then
                 * the object does not exist and we throw StreamNotExistsException. The javadoc, however,
                 * says that this call returns null in case of 304 and 412 responses. We need to
                 * investigate what these responses mean precisely and react accordingly.
                 *
                 * See https://github.com/pravega/pravega/issues/1549
                 */
                if (reader == null) {
                    throw new FileNotFoundException(handle.getChunkName());
                }

                int bytesRead = StreamHelpers.readAll(reader, buffer, bufferOffset, length);

                return bytesRead;
            }
        } catch (Exception e) {
            throwException(handle.getChunkName(), e);
        }
        return 0;
    }

    @Override
    protected int doWrite(ChunkHandle handle, long offset, int length, InputStream data) throws IOException, IndexOutOfBoundsException {
        Preconditions.checkArgument(!handle.isReadOnly(), "handle must not be read-only.");
        try {
            client.putObject(this.config.getBucket(), this.config.getPrefix() + handle.getChunkName(),
                    Range.fromOffsetLength(offset, length), data);
            return (int) length;
        } catch (Exception e) {
            throwException(handle.getChunkName(), e);
        }
        return 0;
    }

    @Override
    protected int doConcat(ChunkHandle target, ChunkHandle... sources) throws IOException, UnsupportedOperationException {
        int totalBytesConcated = 0;
        try {
            Preconditions.checkArgument(!target.isReadOnly(), "target handle must not be read-only.");
            int partNumber = 1;

            SortedSet<MultipartPartETag> partEtags = new TreeSet<>();
            String targetPath = config.getPrefix() + target.getChunkName();
            String uploadId = client.initiateMultipartUpload(config.getBucket(), targetPath);

            // check whether the target exists
            if (!doesExist(target.getChunkName())) {
                throw new FileNotFoundException(target.getChunkName());
            }

            //Copy the first part
            CopyPartRequest copyRequest = new CopyPartRequest(config.getBucket(),
                    targetPath,
                    config.getBucket(),
                    targetPath,
                    uploadId,
                    partNumber++).withSourceRange(Range.fromOffset(0));
            CopyPartResult copyResult = client.copyPart(copyRequest);

            partEtags.add(new MultipartPartETag(copyResult.getPartNumber(), copyResult.getETag()));

            //Copy the second part
            for (ChunkHandle sourceHandle: sources) {
                S3ObjectMetadata metadataResult = client.getObjectMetadata(config.getBucket(),
                        config.getPrefix() + sourceHandle.getChunkName());
                long objectSize = metadataResult.getContentLength(); // in bytes

                copyRequest = new CopyPartRequest(config.getBucket(),
                        config.getPrefix() + sourceHandle.getChunkName(),
                        config.getBucket(),
                        targetPath,
                        uploadId,
                        partNumber++).withSourceRange(Range.fromOffsetLength(0, objectSize));

                copyResult = client.copyPart(copyRequest);
                partEtags.add(new MultipartPartETag(copyResult.getPartNumber(), copyResult.getETag()));
                totalBytesConcated += objectSize;
            }

            //Close the upload
            client.completeMultipartUpload(new CompleteMultipartUploadRequest(config.getBucket(),
                    targetPath, uploadId).withParts(partEtags));

            for (ChunkHandle sourceHandle: sources) {
                client.deleteObject(config.getBucket(), config.getPrefix() + sourceHandle.getChunkName());
            }
        } catch (RuntimeException e) {
            throw e; // make spotbugs happy
        } catch (Exception e) {
            throwException(target.getChunkName(), e);
        }
        return totalBytesConcated;
    }

    @Override
    protected boolean doTruncate(ChunkHandle handle, long offset) throws IOException, UnsupportedOperationException {
        throw new UnsupportedOperationException();
    }

    @Override
    protected boolean doSetReadonly(ChunkHandle handle, boolean isReadOnly) throws IOException, UnsupportedOperationException {
        try {
            setPermission(handle, isReadOnly ? Permission.READ : Permission.FULL_CONTROL);
        } catch (Exception e) {
            throwException(handle.getChunkName(), e);
        }
        return true;
    }

    private void setPermission(ChunkHandle handle, Permission permission) {
        AccessControlList acl = client.getObjectAcl(config.getBucket(), config.getPrefix() + handle.getChunkName());
        acl.getGrants().clear();
        acl.addGrants(new Grant(new CanonicalUser(config.getAccessKey(), config.getAccessKey()), permission));

        client.setObjectAcl(
                new SetObjectAclRequest(config.getBucket(), config.getPrefix() + handle.getChunkName()).withAcl(acl));
    }

    private <T> T throwException(String chunkName, Exception e) throws IOException {
        if (e instanceof S3Exception) {
            S3Exception s3Exception = (S3Exception) e;
            String errorCode = Strings.nullToEmpty(s3Exception.getErrorCode());

            if (errorCode.equals("NoSuchKey")) {
                throw new FileNotFoundException(chunkName);
            }

            if (errorCode.equals("PreconditionFailed")) {
                throw new FileAlreadyExistsException(chunkName);
            }

            if (errorCode.equals("InvalidRange")
                    || errorCode.equals("InvalidArgument")
                    || errorCode.equals("MethodNotAllowed")
                    || s3Exception.getHttpCode() == HttpStatus.SC_REQUESTED_RANGE_NOT_SATISFIABLE) {
                throw new IllegalArgumentException(chunkName, e);
            }

            if (errorCode.equals("AccessDenied")) {
                throw new AccessDeniedException(chunkName);
            }
        }

        if (e instanceof IndexOutOfBoundsException) {
            throw new ArrayIndexOutOfBoundsException(e.getMessage());
        }

        throw Exceptions.sneakyThrow(e);
    }

    @Override
    protected ChunkInfo doGetInfo(String chunkName) throws IOException, IllegalArgumentException {
        try {
            S3ObjectMetadata result = client.getObjectMetadata(config.getBucket(),
                    config.getPrefix() + chunkName);

            AccessControlList acls = client.getObjectAcl(config.getBucket(), config.getPrefix() + chunkName);
            ChunkInfo information = ChunkInfo.builder()
                    .name(chunkName)
                    .length(result.getContentLength())
                    .build();

            return information;
        } catch (Exception e) {
            throwException(chunkName, e);
        }
        return null;
    }

    @Override
    protected ChunkHandle doCreate(String chunkName) throws IOException, IllegalArgumentException {
        try {
            if (!client.listObjects(config.getBucket(), config.getPrefix() + chunkName).getObjects().isEmpty()) {
                throw new FileAlreadyExistsException(chunkName);
            }

            S3ObjectMetadata metadata = new S3ObjectMetadata();
            metadata.setContentLength((long) 0);

            PutObjectRequest request = new PutObjectRequest(config.getBucket(), config.getPrefix() + chunkName, null);

            AccessControlList acl = new AccessControlList();
            acl.addGrants(new Grant(new CanonicalUser(config.getAccessKey(), config.getAccessKey()), Permission.FULL_CONTROL));
            request.setAcl(acl);

            /* Default behavior of putObject is to overwrite an existing object. This behavior can cause data loss.
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
             *
             * This issue is fixed in some versions of extended S3 implementation. The following code sets the IfNoneMatch
             * flag based on configuration.
             */
            if (config.isUseNoneMatch()) {
                request.setIfNoneMatch("*");
            }
            client.putObject(request);

            return ChunkHandle.writeHandle(chunkName);
        } catch (Exception e) {
            throwException(chunkName, e);
        }
        return null;
    }

    @Override
    protected  boolean doesExist(String chunkName) throws IOException, IllegalArgumentException {
        try {
            S3ObjectMetadata result = client.getObjectMetadata(config.getBucket(),
                    config.getPrefix() + chunkName);
            return true;
        } catch (S3Exception e) {
            if ( e.getErrorCode().equals("NoSuchKey")) {
                return false;
            } else {
                throw e;
            }
        }
    }

    @Override
    protected boolean doDelete(ChunkHandle handle) throws IOException, IllegalArgumentException {
        try {
            client.deleteObject(config.getBucket(), config.getPrefix() + handle.getChunkName());
        } catch (Exception e) {
            throwException(handle.getChunkName(), e);
        }
        return true;
    }

    //endregion

}
