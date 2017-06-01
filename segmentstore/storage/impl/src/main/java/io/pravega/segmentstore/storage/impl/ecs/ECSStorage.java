/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.impl.ecs;

import com.emc.object.Range;
import com.emc.object.s3.S3Client;
import com.emc.object.s3.S3Config;
import com.emc.object.s3.S3Exception;
import com.emc.object.s3.S3ObjectMetadata;
import com.emc.object.s3.bean.AccessControlList;
import com.emc.object.s3.bean.CannedAcl;
import com.emc.object.s3.bean.CanonicalUser;
import com.emc.object.s3.bean.CopyPartResult;
import com.emc.object.s3.bean.GetObjectResult;
import com.emc.object.s3.bean.Grant;
import com.emc.object.s3.bean.MultipartPartETag;
import com.emc.object.s3.bean.Permission;
import com.emc.object.s3.jersey.S3JerseyClient;
import com.emc.object.s3.request.CompleteMultipartUploadRequest;
import com.emc.object.s3.request.CopyPartRequest;
import com.emc.object.s3.request.ListObjectsRequest;
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
import lombok.extern.slf4j.Slf4j;
import org.jets3t.service.acl.GroupGrantee;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.FileAlreadyExistsException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

/**
 * Storage adapter for extended S3 based Tier2.
 *
 * Each segment is represented as a single Object on the underlying storage. As the writes at an offset for an object
 * file are idempotent, there is no need for locking when a container fails over to another host.
 * As Pravega does not modify data in Tier2 once written even a contention in writing will cause same data to be
 * written at the same offset till the time the original host gives up ownership.
 */

@Slf4j
public class ECSStorage implements Storage {

    //region members

    private final ECSStorageConfig config;
    private final ExecutorService executor;
    private S3Client client = null;

    //endregion

    //region constructor

    public ECSStorage(ECSStorageConfig config, ExecutorService executor) {
        Preconditions.checkNotNull(config, "config");
        Preconditions.checkNotNull(executor, "executor");
        this.config = config;
        this.executor = executor;
    }

    //endregion

    //region testing entry
    @VisibleForTesting
    public ECSStorage(S3Client client, ECSStorageConfig config, ExecutorService executor) {
        this.client = client;
        this.config = config;
        this.executor = executor;
    }
    //endregion

    //region Storage implementation
    @Override
    public void initialize(long containerEpoch) {
        if( client == null ) {
            S3Config ecsConfig = null;
            try {
                ecsConfig = new S3Config(new URI(config.getEcsUrl()));
                ecsConfig.withIdentity(config.getEcsAccessKey()).withSecretKey(config.getEcsSecretKey());

                if (!Strings.isNullOrEmpty(config.getEcsNamespace())) {
                    ecsConfig.withNamespace(config.getEcsNamespace());
                }
                client = new S3JerseyClient(ecsConfig);
            } catch (URISyntaxException e) {
                log.error("Wrong ECS URI {}. Can not continue.", config.getEcsUrl());
            }
        }

    }

    @Override
    public CompletableFuture<SegmentHandle> openRead(String streamSegmentName) {
        final CompletableFuture<SegmentHandle> retVal = new CompletableFuture<>();

        executor.execute( () -> {
            syncOpenRead(streamSegmentName, retVal);
        });

        return retVal;
    }


    @Override
    public CompletableFuture<Integer> read(SegmentHandle handle, long offset, byte[] buffer, int bufferOffset, int
            length, Duration timeout) {
        final CompletableFuture<Integer> retVal = new CompletableFuture<>();

        executor.execute( () -> {
         syncRead(handle, offset, buffer, bufferOffset, length, timeout, retVal);
        });

        return retVal;
    }

    @Override
    public CompletableFuture<SegmentProperties> getStreamSegmentInfo(String streamSegmentName, Duration timeout) {
        final CompletableFuture<SegmentProperties> retVal = new CompletableFuture<>();

        executor.execute( () -> {
           syncGetStreamSegmentInfo(streamSegmentName, timeout, retVal);
        });

        return retVal;
    }

    @Override
    public CompletableFuture<Boolean> exists(String streamSegmentName, Duration timeout) {
        final CompletableFuture<Boolean> retFuture = new CompletableFuture<>();

        executor.execute( () -> {
           syncExists(streamSegmentName, timeout, retFuture);
        });

        return retFuture;
    }

    @Override
    public CompletableFuture<SegmentHandle> openWrite(String streamSegmentName) {
        final CompletableFuture<SegmentHandle>[] retVal = new CompletableFuture[1];
        retVal[0] = new CompletableFuture<>();

        executor.execute( () -> {
            if ( !checkExists(streamSegmentName)) {
                retVal[0].completeExceptionally(new StreamSegmentNotExistsException(streamSegmentName));
                return;
            }

            AccessControlList acls = client.getObjectAcl(config.getEcsBucket(),
                    config.getEcsRoot() + streamSegmentName);

            boolean canWrite = false;
            canWrite = acls.getGrants().stream().filter((grant) -> {
                        return grant.getPermission().compareTo(Permission.WRITE) > 0;
            }).count() > 0;

            if (!canWrite) {
                ECSSegmentHandle retHandle = ECSSegmentHandle.getReadHandle(streamSegmentName);
                retVal[0].complete(retHandle);
            } else {
                ECSSegmentHandle retHandle = ECSSegmentHandle.getWriteHandle(streamSegmentName);
                retVal[0].complete(retHandle);
            }
        });

        return retVal[0];
    }

    private boolean checkExists(String streamSegmentName) {
        boolean result = false;
        try {
            result = exists(streamSegmentName, Duration.ZERO).get();
        } catch (Exception e) {
        }
        return result;
    }

    @Override
    public CompletableFuture<SegmentProperties> create(String streamSegmentName, Duration timeout) {

        final CompletableFuture<SegmentProperties> retVal = new CompletableFuture<>();

        executor.execute( () -> {
           syncCreate(streamSegmentName, timeout, retVal);
        });

        return retVal;
    }

    @Override
    public CompletableFuture<Void> write(SegmentHandle handle, long offset, InputStream data, int length, Duration
            timeout) {
        final CompletableFuture<Void> retVal = new CompletableFuture<>();

       executor.execute( () -> {
          syncWrite(handle, offset, data, length, timeout, retVal);
        });

        return retVal;
    }

    @Override
    public CompletableFuture<Void> seal(SegmentHandle handle, Duration timeout) {
        CompletableFuture<Void> retVal = new CompletableFuture<>();

        executor.execute( () -> {
            syncSeal( handle, timeout, retVal);
        });

        return retVal;
    }

    @Override
    public CompletableFuture<Void> concat(SegmentHandle targetHandle, long offset, String sourceSegment, Duration
            timeout) {
        CompletableFuture<Void> retVal = new CompletableFuture<>();

        executor.execute( () -> {
            syncConcat(targetHandle, offset, sourceSegment, timeout, retVal);
        });

        return retVal;
    }

    @Override
    public CompletableFuture<Void> delete(SegmentHandle handle, Duration timeout) {
        final CompletableFuture<Void> future = new CompletableFuture<>();

        executor.execute( () -> {
            syncDelete( handle, timeout, future);
        });

        return future;
    }

    //endregion

    //region AutoClosable

    @Override
    public void close() {

    }

    //endregion

    //region private sync implementation

    private void syncOpenRead(String streamSegmentName, CompletableFuture<SegmentHandle> retVal) {
        log.info("Opening {} for read.", streamSegmentName);

        GetObjectResult<InputStream> result = null;
        try {
            result = client.getObject(config.getEcsBucket(), config.getEcsRoot() + streamSegmentName);
        }catch (S3Exception e) {
            log.info("Exception {} while getting segment {}", e, streamSegmentName);
        }

        if ( result == null ) {
            log.info("Did not find segment {} ",streamSegmentName);
            retVal.completeExceptionally(new StreamSegmentNotExistsException(streamSegmentName));
            return;
        }

        ECSSegmentHandle retHandle = ECSSegmentHandle.getReadHandle(streamSegmentName);
        log.info("Created read handle for segment {} ",streamSegmentName);
        retVal.complete(retHandle);
    }


    private void syncRead(SegmentHandle handle, long offset, byte[] buffer, int bufferOffset, int length, Duration
            timeout, CompletableFuture<Integer> retVal) {

        try {
            InputStream reader = client.readObjectStream(config.getEcsBucket(),
                    config.getEcsRoot() + handle.getSegmentName(), Range.fromOffsetLength(offset, length));

            if (reader == null) {
                log.info("Object does not exist {} in bucket {} ", config.getEcsRoot() + handle.getSegmentName(),
                        config.getEcsBucket());

                retVal.completeExceptionally(new StreamSegmentNotExistsException(handle.getSegmentName(), null));
                return;
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
            retVal.complete(originalLength);
        } catch (Exception e) {
            if (e instanceof S3Exception) {
                retVal.completeExceptionally(new StreamSegmentNotExistsException(handle.getSegmentName()));
                return;
            }
            retVal.completeExceptionally(e);
        }

    }


    private void syncGetStreamSegmentInfo(String streamSegmentName, Duration timeout,
                                          CompletableFuture<SegmentProperties> retVal) {
        S3ObjectMetadata result = client.getObjectMetadata(config.getEcsBucket(),
                config.getEcsRoot() + streamSegmentName);

        //client.
        AccessControlList acls = client.getObjectAcl(config.getEcsBucket(), config.getEcsRoot() + streamSegmentName);

        boolean canWrite = false;
        canWrite = acls.getGrants().stream().filter((grant) -> {
            return  grant.getPermission().compareTo(Permission.WRITE) > 0;
        }).count() > 0;

        StreamSegmentInformation information = new StreamSegmentInformation(streamSegmentName,
                result.getContentLength(), !canWrite, false,
                new ImmutableDate(result.getLastModified().toInstant().toEpochMilli()));
            retVal.complete(information);
    }

    private void syncExists(String streamSegmentName, Duration timeout, CompletableFuture<Boolean> retFuture) {

        GetObjectResult<InputStream> result = null;
        try {
            result = client.getObject(config.getEcsBucket(), config.getEcsRoot() + streamSegmentName);
        } catch (S3Exception e) {
            log.info("Stream segment {} does not exist", streamSegmentName);
        }
        retFuture.complete( result != null );
    }


    private void syncCreate(String streamSegmentName, Duration timeout, CompletableFuture<SegmentProperties> retVal) {
        log.info("Creating Segment {}", streamSegmentName);
        try {
            if ( client.listObjects(config.getEcsBucket(), config.getEcsRoot() + streamSegmentName)
                    .getObjects().size()!=0) {
                retVal.completeExceptionally(new StreamSegmentExistsException(streamSegmentName));
                return;
            }

            S3ObjectMetadata metadata = new S3ObjectMetadata();
            metadata.setContentLength((long) 0);

            PutObjectRequest request = new PutObjectRequest(config.getEcsBucket(),
                    config.getEcsRoot() + streamSegmentName,
                     (Object) null);

            request.setCannedAcl(CannedAcl.BucketOwnerFullControl);
            client.putObject(request);

            log.info("Created Segment {}", streamSegmentName);
            retVal.complete(this.getStreamSegmentInfo(streamSegmentName, timeout).get());
        } catch (Exception e) {
            log.info("Exception {} while creating a segment {}", e, streamSegmentName);
            if (e instanceof FileAlreadyExistsException) {
                retVal.completeExceptionally(new StreamSegmentExistsException(streamSegmentName, e));
            } else {
                retVal.completeExceptionally(e);
            }
        }
    }

    private void syncWrite(SegmentHandle handle, long offset, InputStream data, int length, Duration timeout,
                           CompletableFuture<Void> retVal) {
        log.trace("Writing {} to segment {} at offset {}", length, handle.getSegmentName(), offset);

        if( handle.isReadOnly()) {
            retVal.completeExceptionally(new IllegalArgumentException(handle.getSegmentName()));
            return;
        }

        if (!checkExists(handle.getSegmentName())) {
            retVal.completeExceptionally(new StreamSegmentNotExistsException(handle.getSegmentName()));
            return;
        }

        try {
            SegmentProperties si = getStreamSegmentInfo(handle.getSegmentName(), Duration.ZERO).get();

            if ( si.isSealed()) {
                retVal.completeExceptionally(new StreamSegmentSealedException(handle.getSegmentName()));
                return;
            }

            client.putObject(this.config.getEcsBucket(), this.config.getEcsRoot() + handle.getSegmentName(),
                    Range.fromOffsetLength(offset, length), data);
            retVal.complete(null);
        } catch (Exception exc) {
            log.info("Write to segment {} at offset {} failed with exception {} ", handle.getSegmentName(), offset,
                    exc.getMessage());
            retVal.completeExceptionally(exc);
        }
   }


    private void syncSeal(SegmentHandle handle, Duration timeout, CompletableFuture<Void> retVal) {

        if (handle.isReadOnly()) {
            log.info("Seal called on a read handle for segment {}", handle.getSegmentName());
            retVal.completeExceptionally(new IllegalArgumentException(handle.getSegmentName()));
            return;
        }

        try {
            AccessControlList acl = client.getObjectAcl(config.getEcsBucket(),
                    config.getEcsRoot() + handle.getSegmentName());
            acl.getGrants().clear();
            acl.addGrants(new Grant[]{
                    new Grant(new CanonicalUser(config.getEcsAccessKey(), config.getEcsAccessKey())
                    ,Permission.READ_ACP )
            });

         client.setObjectAcl(new SetObjectAclRequest( config.getEcsBucket(),
                 config.getEcsRoot() + handle.getSegmentName())
                            .withAcl(acl));

            log.info("Successfully sealed segment {}", handle.getSegmentName());
            retVal.complete(null);
        } catch (Exception e) {
            log.info("Seal failed with {} for segment {}", e, handle.getSegmentName());
            if(e instanceof S3Exception) {
                retVal.completeExceptionally(new StreamSegmentNotExistsException(handle.getSegmentName()));
                return;
            }
            retVal.completeExceptionally(e);
        }
    }


    private void syncConcat(SegmentHandle targetHandle, long offset, String sourceSegment, Duration timeout,
                            CompletableFuture<Void> retVal) {

        try {
            SortedSet<MultipartPartETag> partEtags = new TreeSet<>();

            String targetPath = config.getEcsRoot() + targetHandle.getSegmentName();

            String uploadId = client.initiateMultipartUpload(config.getEcsBucket(), targetPath);

            //Upload the first part
            CopyPartRequest copyRequest = new CopyPartRequest(config.getEcsBucket(),
                    targetPath,
                    config.getEcsBucket(),
                    targetPath,
                    uploadId,
                    1).withSourceRange(Range.fromOffsetLength(0, offset));
            CopyPartResult copyResult = client.copyPart(copyRequest);

            partEtags.add(new MultipartPartETag(copyResult.getPartNumber(), copyResult.getETag()));

            //Upload the second part
            S3ObjectMetadata metadataResult = client.getObjectMetadata(config.getEcsBucket(),
                    config.getEcsRoot() + sourceSegment);
            long objectSize = metadataResult.getContentLength(); // in bytes

            copyRequest = new CopyPartRequest(config.getEcsBucket(),
                    config.getEcsRoot() + sourceSegment,
                    config.getEcsBucket(),
                    targetPath,
                    uploadId,
                    2).withSourceRange(Range.fromOffsetLength(0, objectSize));

            copyResult = client.copyPart(copyRequest);
            partEtags.add(new MultipartPartETag(copyResult.getPartNumber(), copyResult.getETag()));


            //Close the upload
            client.completeMultipartUpload(new CompleteMultipartUploadRequest(config.getEcsBucket(),
                    targetPath, uploadId).withParts(partEtags));

            SegmentProperties si = getStreamSegmentInfo(targetHandle.getSegmentName(), Duration.ZERO).get();
            log.info("Properties after concat completion : length is {} ",si.getLength());

            client.deleteObject(config.getEcsBucket(), config.getEcsRoot() + sourceSegment);

            retVal.complete(null);
        } catch (Exception e) {
            log.info("Concat of {} on {} failed with {}", sourceSegment, targetHandle.getSegmentName(), e);
            if( e instanceof S3Exception) {
                retVal.completeExceptionally(new StreamSegmentNotExistsException(e.getMessage()));
                return;
            }
            retVal.completeExceptionally(e);
        }
    }


    private void syncDelete(SegmentHandle handle, Duration timeout, CompletableFuture<Void> future) {
        try {
            client.deleteObject(config.getEcsBucket(), config.getEcsRoot() + handle.getSegmentName());
            future.complete(null);
        } catch (Exception e) {
            future.completeExceptionally(e);
        }
    }

    //endregion

}
