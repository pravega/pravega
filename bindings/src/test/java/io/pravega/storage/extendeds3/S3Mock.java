/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
import com.emc.object.s3.request.AbortMultipartUploadRequest;
import com.emc.object.s3.request.CompleteMultipartUploadRequest;
import com.emc.object.s3.request.CopyPartRequest;
import com.emc.object.s3.request.DeleteObjectsRequest;
import com.emc.object.s3.request.PutObjectRequest;
import com.emc.object.s3.request.SetObjectAclRequest;
import io.pravega.common.io.StreamHelpers;
import io.pravega.common.util.BufferView;
import io.pravega.common.util.ByteArraySegment;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.NotThreadSafe;
import lombok.AllArgsConstructor;
import lombok.val;
import org.apache.http.HttpStatus;

/**
 * In-memory mock for S3.
 */
public class S3Mock {
    //region Private

    @GuardedBy("objects")
    private final Map<String, Map<Integer, CopyPartRequest>> multipartUploads;
    private final AtomicLong multipartNextId = new AtomicLong(0);
    @GuardedBy("objects")
    private final Map<String, ObjectData> objects;

    //endregion

    //region Constructor

    public S3Mock() {
        this.objects = new HashMap<>();
        this.multipartUploads = new HashMap<>();
    }

    //endregion

    //region Mock Implementation

    private String getObjectName(String bucketName, String key) {
        return String.format("%s-%s", bucketName, key);
    }

    public PutObjectResult putObject(PutObjectRequest request) {
        // Fix passed in object metadata.
        if (request.getObjectMetadata() != null) {
            request.setObjectMetadata(new S3ObjectMetadata()
                    .withContentType(request.getObjectMetadata().getContentType())
                    .withContentLength(request.getObjectMetadata().getContentLength()));
        }
        String objectName = getObjectName(request.getBucketName(), request.getKey());
        synchronized (this.objects) {
            if (this.objects.containsKey(objectName)) {
                throw new S3Exception(String.format("Object '%s/%s' exists.", request.getBucketName(), request.getKey()), 0, "PreconditionFailed", "");
            }
            if (null != request.getObject()) {
                try {
                    val bufferView = new ByteArraySegment(((InputStream) request.getObject()).readAllBytes());
                    this.objects.put(objectName, new ObjectData(bufferView, request.getAcl()));
                } catch (IOException ex) {
                    throw new S3Exception("Copy error", HttpStatus.SC_INTERNAL_SERVER_ERROR);
                }
            } else {
                this.objects.put(objectName, new ObjectData(BufferView.empty(), request.getAcl()));
            }
            return new PutObjectResult();
        }
    }

    public void putObject(String bucketName, String key, Range range, Object content) {
        String objectName = getObjectName(bucketName, key);
        synchronized (this.objects) {
            ObjectData od = this.objects.get(objectName);
            if (od == null) {
                throw new S3Exception(String.format("Object '%s/%s' does not exist.", bucketName, key), 404, "NoSuchKey", key);
            }

            final int startOffset = (int) (long) range.getFirst();
            final int length = (int) (range.getLast() + 1 - range.getFirst());
            BufferView objectContent = od.content;
            if (startOffset < objectContent.getLength()) {
                objectContent = new ByteArraySegment(objectContent.slice(0, startOffset).getCopy());
            } else if (startOffset > objectContent.getLength()) {
                throw new S3Exception("", HttpStatus.SC_REQUESTED_RANGE_NOT_SATISFIABLE, "InvalidRange", "");
            }

            InputStream source = (InputStream) content;
            byte[] buffer = new byte[length];
            try {
                int copyLength = StreamHelpers.readAll(source, buffer, 0, length);
                assert copyLength == length;
            } catch (IOException ex) {
                throw new S3Exception("Copy error", HttpStatus.SC_INTERNAL_SERVER_ERROR);
            }

            od.content = BufferView.wrap(Arrays.asList(objectContent, new ByteArraySegment(buffer)));
        }
    }

    public void setObjectAcl(String bucketName, String key, AccessControlList acl) {
        String objectName = getObjectName(bucketName, key);
        synchronized (this.objects) {
            ObjectData od = this.objects.get(objectName);
            if (od == null) {
                throw new S3Exception("NoObject", HttpStatus.SC_NOT_FOUND, "NoSuchKey", key);
            }
            od.acl = acl;
        }
    }

    public void setObjectAcl(SetObjectAclRequest request) {
        String objectName = getObjectName(request.getBucketName(), request.getKey());
        synchronized (this.objects) {
            ObjectData od = this.objects.get(objectName);
            if (od == null) {
                throw new S3Exception("NoObject", HttpStatus.SC_NOT_FOUND, "NoSuchKey", request.getKey());
            }
            od.acl = request.getAcl();
        }
    }

    public AccessControlList getObjectAcl(String bucketName, String key) {
        String objectName = getObjectName(bucketName, key);
        synchronized (this.objects) {
            ObjectData od = this.objects.get(objectName);
            if (od == null) {
                throw new S3Exception("NoObject", HttpStatus.SC_NOT_FOUND, "NoSuchKey", key);
            }

            return od.acl;
        }
    }

    public CopyPartResult copyPart(CopyPartRequest request) {
        String objectName = getObjectName(request.getBucketName(), request.getKey());
        synchronized (this.objects) {
            Map<Integer, CopyPartRequest> partMap = this.multipartUploads.get(objectName);
            if (partMap == null) {
                throw new S3Exception("NoSuchUpload", HttpStatus.SC_NOT_FOUND, "NoSuchUpload", "");
            }
            if (partMap.containsKey(request.getPartNumber())) {
                // Overwriting may or may not be accepted in real S3, but in our case, we consider this as a bug so
                // we want to make sure we don't do it.
                throw new S3Exception("Part exists already.", HttpStatus.SC_BAD_REQUEST, "InvalidArgument", "");
            }
            partMap.put(request.getPartNumber(), request);
            CopyPartResult result = new CopyPartResult();
            result.setPartNumber(request.getPartNumber());
            result.setETag(request.getUploadId());
            return result;
        }
    }

    public void deleteObject(String bucketName, String key) {
        String objectName = getObjectName(bucketName, key);
        synchronized (this.objects) {
            if (this.objects.remove(objectName) == null) {
                throw new S3Exception("NoSuchKey", HttpStatus.SC_NOT_FOUND, "NoSuchKey", "");
            }
        }
    }

    public DeleteObjectsResult deleteObjects(DeleteObjectsRequest request) {
        for (ObjectKey obj : request.getDeleteObjects().getKeys()) {
            deleteObject(request.getBucketName(), obj.getKey());
        }
        return new DeleteObjectsResult();
    }

    public ListObjectsResult listObjects(String bucketName, String prefix) {
        ListObjectsResult result = new ListObjectsResult();
        List<S3Object> list;
        String objectPrefix = getObjectName(bucketName, prefix);
        int bucketPrefixLength = bucketName.length() + 1;
        synchronized (this.objects) {
            list = this.objects.entrySet().stream()
                    .filter(e -> e.getKey().startsWith(objectPrefix))
                    .map(e -> {
                        S3Object object = new S3Object();
                        object.setKey(e.getKey().substring(bucketPrefixLength));
                        object.setSize((long) e.getValue().content.getLength());
                        object.setLastModified(new Date(System.currentTimeMillis()));
                        return object;
                    })
                    .collect(Collectors.toList());
        }
        result.setObjects(list);
        result.setMaxKeys(list.size());
        result.setBucketName(bucketName);
        result.setPrefix(prefix);
        return result;
    }

    public S3ObjectMetadata getObjectMetadata(String bucketName, String key) {
        String objectName = getObjectName(bucketName, key);
        S3ObjectMetadata metadata = new S3ObjectMetadata();
        synchronized (this.objects) {
            ObjectData od = this.objects.get(objectName);
            if (od == null) {
                throw new S3Exception("NoSuchKey", HttpStatus.SC_NOT_FOUND, "NoSuchKey", "");
            }
            metadata.setContentLength((long) od.content.getLength());
            metadata.setLastModified(new Date(System.currentTimeMillis()));
        }
        return metadata;
    }

    public InputStream readObjectStream(String bucketName, String key, Range range) {
        String objectName = getObjectName(bucketName, key);
        synchronized (this.objects) {
            ObjectData od = this.objects.get(objectName);
            if (od == null) {
                throw new S3Exception("NoSuchKey", HttpStatus.SC_NOT_FOUND, "NoSuchKey", "");
            }

            final int offset = (int) (long) range.getFirst();
            final int length = (int) (range.getLast() - range.getFirst() + 1);
            try {
                return od.content.slice(offset, length).getReader();
            } catch (Exception ex) {
                throw new S3Exception("InvalidRange", HttpStatus.SC_REQUESTED_RANGE_NOT_SATISFIABLE, "InvalidRange", "");
            }
        }
    }

    public String initiateMultipartUpload(String bucketName, String key) {
        String objectName = getObjectName(bucketName, key);
        synchronized (this.objects) {
            this.multipartUploads.put(objectName, new HashMap<>());
            return Long.toString(this.multipartNextId.incrementAndGet());
        }
    }

    public CompleteMultipartUploadResult completeMultipartUpload(CompleteMultipartUploadRequest request) {
        String objectName = getObjectName(request.getBucketName(), request.getKey());
        synchronized (this.objects) {
            Map<Integer, CopyPartRequest> partMap = this.multipartUploads.get(objectName);
            if (partMap == null) {
                throw new S3Exception("NoSuchKey", HttpStatus.SC_NOT_FOUND, "NoSuchKey", "");
            }

            if (!this.objects.containsKey(objectName)) {
                throw new S3Exception("NoSuchKey", HttpStatus.SC_NOT_FOUND, "NoSuchKey", "");
            }

            val partObjects = partMap.entrySet().stream().sorted(Comparator.comparingInt(Map.Entry::getKey))
                    .collect(Collectors.toList());
            val builder = BufferView.builder();

            val expectedPartId = new AtomicInteger(0); // They start at 1, but we increment before each check.
            partObjects.forEach(e -> {
                expectedPartId.incrementAndGet();
                if (e.getKey() != expectedPartId.get()) {
                    // Make sure all the parts are there.
                    throw new S3Exception("InvalidPart", HttpStatus.SC_BAD_REQUEST, "InvalidPart", "");
                }
                String partObjectName = getObjectName(e.getValue().getBucketName(), e.getValue().getSourceKey());
                ObjectData od = this.objects.get(partObjectName);
                if (od == null) {
                    throw new S3Exception("NoSuchKey", HttpStatus.SC_NOT_FOUND, "NoSuchKey", "");
                }
                builder.add(od.content);
            });

            this.objects.get(objectName).content = builder.build();
            this.multipartUploads.remove(request.getKey());
        }

        return new CompleteMultipartUploadResult();
    }

    public void abortMultipartUpload(AbortMultipartUploadRequest request) {
        String objectName = getObjectName(request.getBucketName(), request.getKey());
        synchronized (this.objects) {
            Map<Integer, CopyPartRequest> partMap = this.multipartUploads.remove(objectName);
            if (partMap == null) {
                throw new S3Exception("NoSuchKey", HttpStatus.SC_NOT_FOUND, "NoSuchKey", "");
            }
        }
    }

    public GetObjectResult<InputStream> getObject(String bucketName, String key) {
        String objectName = getObjectName(bucketName, key);
        synchronized (this.objects) {
            ObjectData od = this.objects.get(objectName);
            if (od != null) {
                return new GetObjectResult<>();
            } else {
                return null;
            }
        }
    }

    //endregion

    //region Helper classes

    @NotThreadSafe
    @AllArgsConstructor
    private static class ObjectData {
        BufferView content;
        AccessControlList acl;
    }

    //endregion
}
