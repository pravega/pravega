/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.storage.extendeds3;

import com.emc.object.Range;
import com.emc.object.s3.S3Exception;
import com.emc.object.s3.S3ObjectMetadata;
import com.emc.object.s3.bean.AccessControlList;
import com.emc.object.s3.bean.CompleteMultipartUploadResult;
import com.emc.object.s3.bean.CopyPartResult;
import com.emc.object.s3.bean.DeleteObjectsResult;
import com.emc.object.s3.bean.GetObjectResult;
import com.emc.object.s3.bean.ListObjectsResult;
import com.emc.object.s3.bean.ObjectKey;
import com.emc.object.s3.bean.PutObjectResult;
import com.emc.object.s3.bean.S3Object;
import com.emc.object.s3.request.CompleteMultipartUploadRequest;
import com.emc.object.s3.request.CopyPartRequest;
import com.emc.object.s3.request.DeleteObjectsRequest;
import com.emc.object.s3.request.PutObjectRequest;
import com.emc.object.s3.request.SetObjectAclRequest;
import io.pravega.common.io.StreamHelpers;
import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import lombok.Synchronized;
import org.apache.commons.httpclient.HttpStatus;

/**
 * File system based implementation of extended S3 emulation API.
 */
public class S3FileSystemImpl extends S3ImplBase {
    private final String baseDir;
    private final ConcurrentMap<String, ConcurrentMap<Integer, CopyPartRequest>> multipartUploads = new ConcurrentHashMap<>();

    public S3FileSystemImpl(String baseDir) {
        this.baseDir = baseDir;
    }

    @Override
    public void start() throws Exception {
    }

    @Override
    public void stop() throws Exception {
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
            throw new S3Exception(e.getMessage(), 0, e);
        }
        PutObjectResult retVal = new PutObjectResult();
        if (request.getAcl() != null) {
            long size = 0;
            if (request.getRange() != null) {
                size = request.getRange().getLast() + 1;
            }
            aclMap.putIfAbsent(request.getKey(), new AclSize(request.getAcl(), size));
        }
        return retVal;
    }

    @Synchronized
    @Override
    public void putObject(String bucketName, String key, Range range, Object content) {

        Path path = Paths.get(this.baseDir, bucketName, key);
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.WRITE)) {

            long startOffset = range.getFirst();
            long length = range.getLast() + 1 - range.getFirst();
            do {
                long bytesTransferred = channel.transferFrom(Channels.newChannel((InputStream) content),
                        range.getFirst(), range.getLast() + 1 - range.getFirst());
                length -= bytesTransferred;
                startOffset += bytesTransferred;
            } while (length > 0);

            AclSize aclKey = aclMap.get(key);
            aclMap.put(key, aclKey.withSize(range.getLast() + 1));
        } catch (IOException e) {
            throw new S3Exception("NoObject", 404, "NoSuchKey", key);
        }
    }

    @Synchronized
    @Override
    public void setObjectAcl(String bucketName, String key, AccessControlList acl) {
        AclSize retVal = aclMap.get(key);
        if (retVal == null) {
            throw new S3Exception("NoObject", HttpStatus.SC_NOT_FOUND, "NoSuchKey", key);
        }
        aclMap.put(key, retVal.withAcl(acl));
    }

    @Synchronized
    @Override
    public void setObjectAcl(SetObjectAclRequest request) {
        AclSize retVal = aclMap.get(request.getKey());
        if (retVal == null) {
            throw new S3Exception("NoObject", HttpStatus.SC_NOT_FOUND, "NoSuchKey", request.getKey());
        }
        aclMap.put(request.getKey(), retVal.withAcl(request.getAcl()));
    }

    @Override
    public AccessControlList getObjectAcl(String bucketName, String key) {
        AclSize retVal = aclMap.get(key);
        if (retVal == null) {
            throw new S3Exception("NoObject", HttpStatus.SC_NOT_FOUND, "NoSuchKey", key);
        }
        return retVal.getAcl();
    }

    @Override
    public CopyPartResult copyPart(CopyPartRequest request) {
        Map<Integer, CopyPartRequest> partMap = multipartUploads.get(request.getKey());
        if (partMap == null) {
            throw new S3Exception("NoSuchKey", HttpStatus.SC_NOT_FOUND, "NoSuchKey", "");
        }
        partMap.put(request.getPartNumber(), request);
        CopyPartResult result = new CopyPartResult();
        result.setPartNumber(request.getPartNumber());
        result.setETag(request.getUploadId());
        return result;
    }

    @Override
    public void deleteObject(String bucketName, String key) {
        Path path = Paths.get(this.baseDir, bucketName, key);
        try {
            Files.delete(path);
        } catch (IOException e) {
            throw new S3Exception("NoSuchKey", HttpStatus.SC_NOT_FOUND, "NoSuchKey", "");
        }
    }

    @Override
    public DeleteObjectsResult deleteObjects(DeleteObjectsRequest request) {
        for (ObjectKey obj : request.getDeleteObjects().getKeys()) {
            this.deleteObject(request.getBucketName(), obj.getKey());
        }
        return new DeleteObjectsResult();
    }

    @Override
    public ListObjectsResult listObjects(String bucketName, String prefix) {
        ListObjectsResult result = new ListObjectsResult();
        ArrayList<S3Object> list = new ArrayList<>();
        Path path = Paths.get(this.baseDir, bucketName, prefix);
        try {
            if (Files.exists(path)) {
                if (Files.isDirectory(path)) {
                    Files.list(path).forEach(file -> {
                        addFileAsObjectToList(file, list, bucketName);
                    });
                } else {
                    addFileAsObjectToList(path, list, bucketName);
                }
            }
        } catch (IOException e) {
            throw new S3Exception("NoSuchKey", HttpStatus.SC_NOT_FOUND, "NoSuchKey", "");
        }
        result.setObjects(list);
        return result;
    }

    @Override
    public S3ObjectMetadata getObjectMetadata(String bucketName, String key) {
        S3ObjectMetadata metadata = new S3ObjectMetadata();
        AclSize data = aclMap.get(key);
        if (data == null) {
            throw new S3Exception("NoSuchKey", HttpStatus.SC_NOT_FOUND, "NoSuchKey", "");
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
            if (range.getFirst() != 0) {
                long bytesSkipped = 0;
                do {
                    bytesSkipped += returnStream.skip(range.getFirst());
                } while (bytesSkipped < range.getFirst());
            }
            StreamHelpers.readAll(returnStream, bytes, 0, bytes.length);
            return new ByteArrayInputStream(bytes);
        } catch (IOException e) {
            throw new S3Exception("NoSuchKey", HttpStatus.SC_NOT_FOUND, "NoSuchKey", "");
        }
    }

    //Adds a single file to the list as an object.
    private void addFileAsObjectToList(Path path, ArrayList<S3Object> list, String bucketName) {
        S3Object object = new S3Object();
        object.setKey(path.toString().replaceFirst(Paths.get(this.baseDir, bucketName).toString(), ""));
        list.add(object);
    }

    @Override
    public String initiateMultipartUpload(String bucketName, String key) {
        multipartUploads.put(key, new ConcurrentHashMap<>());
        return Integer.toString(multipartUploads.size());
    }

    @Synchronized
    @Override
    public CompleteMultipartUploadResult completeMultipartUpload(CompleteMultipartUploadRequest request) {
        Map<Integer, CopyPartRequest> partMap = multipartUploads.get(request.getKey());
        if (partMap == null) {
            throw new S3Exception("NoSuchKey", HttpStatus.SC_NOT_FOUND, "NoSuchKey", "");
        }
        try {
            partMap.forEach((index, copyPart) -> {
                if (!copyPart.getKey().equals(copyPart.getSourceKey())) {
                    Path sourcePath = Paths.get(this.baseDir, copyPart.getBucketName(), copyPart.getSourceKey());
                    Path targetPath = Paths.get(this.baseDir, copyPart.getBucketName(), copyPart.getKey());
                    try (FileChannel sourceChannel = FileChannel.open(sourcePath, StandardOpenOption.READ);
                         FileChannel targetChannel = FileChannel.open(targetPath, StandardOpenOption.WRITE)) {
                        targetChannel.transferFrom(sourceChannel, Files.size(targetPath),
                                copyPart.getSourceRange().getLast() + 1 - copyPart.getSourceRange().getFirst());
                        targetChannel.close();
                        AclSize aclMap = this.aclMap.get(copyPart.getKey());
                        this.aclMap.put(copyPart.getKey(), aclMap.withSize(Files.size(targetPath)));
                    } catch (IOException e) {
                        throw new S3Exception("NoSuchKey", 404, "NoSuchKey", "");
                    }
                }
            });
        } finally {
            multipartUploads.remove(request.getKey());
        }

        return new CompleteMultipartUploadResult();
    }

    @Override
    public GetObjectResult<InputStream> getObject(String bucketName, String key) {
        if (aclMap.containsKey(key)) {
            return new GetObjectResult<>();
        } else {
            return null;
        }
    }
}
