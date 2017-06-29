/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.host;

import com.emc.object.Range;
import com.emc.object.s3.S3Config;
import com.emc.object.s3.S3Exception;
import com.emc.object.s3.S3ObjectMetadata;
import com.emc.object.s3.bean.AccessControlList;
import com.emc.object.s3.bean.CompleteMultipartUploadResult;
import com.emc.object.s3.bean.CopyPartResult;
import com.emc.object.s3.bean.DeleteObjectsResult;
import com.emc.object.s3.bean.GetObjectResult;
import com.emc.object.s3.bean.ListObjectsResult;
import com.emc.object.s3.bean.PutObjectResult;
import com.emc.object.s3.bean.S3Object;
import com.emc.object.s3.jersey.S3JerseyClient;
import com.emc.object.s3.request.CompleteMultipartUploadRequest;
import com.emc.object.s3.request.CopyPartRequest;
import com.emc.object.s3.request.DeleteObjectsRequest;
import com.emc.object.s3.request.PutObjectRequest;
import com.emc.object.s3.request.SetObjectAclRequest;
import io.pravega.segmentstore.contracts.BadOffsetException;
import io.pravega.segmentstore.storage.impl.exts3.EXTS3StorageTest;
import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import lombok.SneakyThrows;

/**
 * Client wrapper for S3JerseyClient.
 */
class S3ClientWrapper extends S3JerseyClient {
    private static final ConcurrentMap<String, EXTS3StorageTest.AclSize> ACL_MAP = new ConcurrentHashMap<>();
    private String baseDir;
    private final ConcurrentMap<String, Map<Integer, CopyPartRequest>> multipartUploads = new ConcurrentHashMap<>();

    public S3ClientWrapper(S3Config exts3Config, String baseDir) {
        super(exts3Config);
        this.baseDir = baseDir;
    }

    @Override
    public DeleteObjectsResult deleteObjects(DeleteObjectsRequest request) {
        return new DeleteObjectsResult();
    }

    @Override
    public PutObjectResult putObject(PutObjectRequest request) {

        if (request.getObjectMetadata() != null) {
            request.setObjectMetadata(null);
        }
        try {
            Path path = Paths.get(this.baseDir, request.getBucketName(), request.getKey());
            Files.createDirectories(path.getParent());
            Files.createFile(path);
        } catch (IOException e) {
            throw new S3Exception(e.getMessage(), 0);
        }
        PutObjectResult retVal = new PutObjectResult();
        if (request.getAcl() != null) {
            long size = 0;
            if (request.getRange() != null) {
                size = request.getRange().getLast() + 1;
            }
            ACL_MAP.put(request.getKey(), new EXTS3StorageTest.AclSize(request.getAcl(),
                    size));
        }
        return retVal;
    }

    @Override
    @SneakyThrows
    public void putObject(String bucketName, String key, Range range, Object content) {

        Path path = Paths.get(this.baseDir, bucketName, key);
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.WRITE)) {

            long bytesTransferred = channel.transferFrom(Channels.newChannel((InputStream) content),
                    range.getFirst(), range.getLast() + 1 - range.getFirst());

            ACL_MAP.get(key).setSize(range.getLast() + 1);
        } catch (IOException | IllegalArgumentException e) {
            throw new BadOffsetException(key, 0, 0);
        }
    }

    @Override
    public void setObjectAcl(String bucketName, String key, AccessControlList acl) {
        EXTS3StorageTest.AclSize retVal = ACL_MAP.get(key);
        if (retVal == null) {
            throw new S3Exception("NoObject", 500, "NoSuchKey", key);
        }
        retVal.setAcl(acl);
    }

    @Override
    public void setObjectAcl(SetObjectAclRequest request) {
        EXTS3StorageTest.AclSize retVal = ACL_MAP.get(request.getKey());
        if (retVal == null) {
            throw new S3Exception("NoObject", 500, "NoSuchKey", request.getKey());
        }
        retVal.setAcl(request.getAcl());
    }

    @Override
    public AccessControlList getObjectAcl(String bucketName, String key) {
        EXTS3StorageTest.AclSize retVal = ACL_MAP.get(key);
        if (retVal == null) {
            throw new S3Exception("NoObject", 500, "NoSuchKey", key);
        }
        return retVal.getAcl();
    }

    @Override
    public void deleteObject(String bucketName, String key) {
        Path path = Paths.get(this.baseDir, bucketName, key);
        try {
            Files.delete(path);
        } catch (IOException e) {
            throw new S3Exception("NoSuchKey", 0, "NoSuchKey", "");
        }
        ACL_MAP.remove(key);
    }

    @Override
    public ListObjectsResult listObjects(String bucketName, String prefix) {
        ListObjectsResult result = new ListObjectsResult();
        ArrayList<S3Object> list = new ArrayList<>();
        Path path = Paths.get(this.baseDir, bucketName, prefix);
        try {
            Files.list(path).forEach((file) -> {
                S3Object object = new S3Object();
                object.setKey(file.toString().replaceFirst(Paths.get(this.baseDir, bucketName).toString(), ""));
                list.add(object);
            });
        } catch (IOException e) {
        }
        result.setObjects(list);
        return result;
    }

    @Override
    public S3ObjectMetadata getObjectMetadata(String bucketName, String key) {
        S3ObjectMetadata metadata = new S3ObjectMetadata();
        EXTS3StorageTest.AclSize data = ACL_MAP.get(key);
        if (data == null) {
            throw new S3Exception("NoSuchKey", 0, "NoSuchKey", "");
        }
        metadata.setContentLength(data.getSize());
        Path path = Paths.get(this.baseDir, bucketName, key);
        metadata.setLastModified(new Date(path.toFile().lastModified()));
        return metadata;
    }

    @Override
    public InputStream readObjectStream(String bucketName, String key, Range range) {
        byte[] bytes = new byte[Math.toIntExact(range.getLast() + 1 - range.getFirst())];
        Path path = Paths.get(this.baseDir, bucketName, key);
        FileInputStream returnStream;
        try {
            returnStream = new FileInputStream(path.toFile());
            returnStream.getChannel().read(ByteBuffer.wrap(bytes), range.getFirst());
            return new ByteArrayInputStream(bytes);
        } catch (IOException e) {
            throw new S3Exception("NoSuchKey", 0, "NoSuchKey", "");
        }
    }

    @Override
    public String initiateMultipartUpload(String bucketName, String key) {
        multipartUploads.put(key, new HashMap());
        return Integer.toString(multipartUploads.size());
    }

    @Override
    public CopyPartResult copyPart(CopyPartRequest request) {
        Map<Integer, CopyPartRequest> partMap = multipartUploads.get(request.getKey());
        if (partMap == null) {
            throw new S3Exception("NoSuchKey", 0, "NoSuchKey", "");
        }
        partMap.put(request.getPartNumber(), request);
        CopyPartResult result = new CopyPartResult();
        result.setPartNumber(request.getPartNumber());
        result.setETag(request.getUploadId());
        return result;
    }

    @Override
    public CompleteMultipartUploadResult completeMultipartUpload(CompleteMultipartUploadRequest request) {
        Map<Integer, CopyPartRequest> partMap = multipartUploads.get(request.getKey());
        if (partMap == null) {
            throw new S3Exception("NoSuchKey", 0, "NoSuchKey", "");
        }
        partMap.forEach((index, copyPart) -> {
            if (copyPart.getKey() != copyPart.getSourceKey()) {
                Path sourcePath = Paths.get(this.baseDir, copyPart.getBucketName(), copyPart.getSourceKey());
                Path targetPath = Paths.get(this.baseDir, copyPart.getBucketName(), copyPart.getKey());
                try (FileChannel sourceChannel = FileChannel.open(sourcePath, StandardOpenOption.READ);
                     FileChannel targetChannel = FileChannel.open(targetPath, StandardOpenOption.WRITE)) {
                    targetChannel.transferFrom(sourceChannel, Files.size(targetPath),
                            copyPart.getSourceRange().getLast() + 1 - copyPart.getSourceRange().getFirst());
                    targetChannel.close();
                    ACL_MAP.get(copyPart.getKey()).setSize(Files.size(targetPath));
                } catch (IOException e) {
                    throw new S3Exception("NoSuchKey", 0, "NoSuchKey", "");
                }
            }
        });
        return new CompleteMultipartUploadResult();
    }

    @Override
    public GetObjectResult<InputStream> getObject(String bucketName, String key) {
        if (ACL_MAP.get(key) != null) {
            return new GetObjectResult<>();
        } else {
            return null;
        }
    }

    @Override
    public void destroy() {
    }
}
