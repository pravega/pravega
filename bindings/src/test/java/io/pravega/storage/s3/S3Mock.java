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
package io.pravega.storage.s3;

import io.pravega.common.util.BufferView;
import io.pravega.common.util.ByteArraySegment;
import lombok.AllArgsConstructor;
import lombok.val;
import org.apache.http.HttpStatus;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CopyPartResult;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectResponse;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsResponse;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ObjectCannedACL;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.UploadPartCopyRequest;
import software.amazon.awssdk.services.s3.model.UploadPartCopyResponse;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * In-memory mock for S3.
 */
public class S3Mock {
    //region Private

    @GuardedBy("objects")
    private final Map<String, Map<Integer, UploadPartCopyRequest>> multipartUploadParts;
    @GuardedBy("objects")
    private final Map<String, String> multipartUploads;

    private final AtomicLong multipartNextId = new AtomicLong(0);
    private final AtomicLong eTags = new AtomicLong(0);

    @GuardedBy("objects")
    private final Map<String, ObjectData> objects;

    //endregion

    //region Constructor

    public S3Mock() {
        this.objects = new HashMap<>();
        this.multipartUploads = new HashMap<>();
        this.multipartUploadParts = new HashMap<>();
    }

    //endregion

    //region Mock Implementation

    private String getObjectName(String bucketName, String key) {
        return String.format("%s-%s", bucketName, key);
    }

    PutObjectResponse putObject(PutObjectRequest request, RequestBody requestBody) {
        String objectName = getObjectName(request.bucket(), request.key());
        synchronized (this.objects) {
            if (this.objects.containsKey(objectName)) {
                throw S3Exception.builder().build();
            }
            if (null != requestBody) {
                try (val inputStream = requestBody.contentStreamProvider().newStream()) {
                    val bufferView = new ByteArraySegment(inputStream.readAllBytes());
                    this.objects.put(objectName, new ObjectData(bufferView, request.acl()));
                } catch (IOException ex) {
                    throw getException("Copy error", "Copy error", HttpStatus.SC_INTERNAL_SERVER_ERROR);
                }
            } else {
                this.objects.put(objectName, new ObjectData(BufferView.empty(), request.acl()));
            }
            return PutObjectResponse.builder().build();
        }
    }

    DeleteObjectResponse deleteObject(DeleteObjectRequest deleteObjectRequest) {
        String objectName = getObjectName(deleteObjectRequest.bucket(), deleteObjectRequest.key());
        return deleteObject(objectName);
    }

    DeleteObjectResponse deleteObject(String objectName) {
        synchronized (this.objects) {
            if (this.objects.remove(objectName) == null) {
                throw getException(S3ChunkStorage.NO_SUCH_KEY, S3ChunkStorage.NO_SUCH_KEY, HttpStatus.SC_NOT_FOUND);
            }
            return DeleteObjectResponse.builder().build();
        }
    }

    DeleteObjectsResponse deleteObjects(DeleteObjectsRequest deleteObjectsRequest) {
        for (var object: deleteObjectsRequest.delete().objects()) {
            String objectName = getObjectName(deleteObjectsRequest.bucket(), object.key());
            deleteObject(objectName);
        }
        return DeleteObjectsResponse.builder().build();
    }

    HeadObjectResponse headObject(HeadObjectRequest headObjectRequest) throws AwsServiceException,
            SdkClientException {
        ObjectData result;
        String objectName = getObjectName(headObjectRequest.bucket(), headObjectRequest.key());
        synchronized (this.objects) {
            ObjectData od = this.objects.get(objectName);

            if (od == null) {
                throw getException(S3ChunkStorage.NO_SUCH_KEY, S3ChunkStorage.NO_SUCH_KEY, HttpStatus.SC_NOT_FOUND);
            }
            result = od;
            var objectData = result;
            return HeadObjectResponse.builder()
                    .contentLength((long) objectData.content.getLength())
                    .build();
        }
    }

    ResponseBytes<GetObjectResponse> readObjectStream(GetObjectRequest getObjectRequest) {
        String objectName = getObjectName(getObjectRequest.bucket(), getObjectRequest.key());
        synchronized (this.objects) {
            ObjectData od = this.objects.get(objectName);

            if (od == null) {
                throw getException(S3ChunkStorage.NO_SUCH_KEY, S3ChunkStorage.NO_SUCH_KEY, HttpStatus.SC_NOT_FOUND);
            }
            int offset = 0;
            int length = od.content.getLength();
            if (null != getObjectRequest.range()) {
                var parts = getObjectRequest.range().replace("bytes=", "").split("-");
                offset = Integer.parseInt(parts[0]);
                length = Integer.parseInt(parts[1]) - offset + 1;
            }
            try {
                return ResponseBytes.fromInputStream(GetObjectResponse.builder().build(),
                        od.content.slice(offset, length).getReader());
            } catch (Exception ex) {
                throw getException(S3ChunkStorage.INVALID_RANGE, S3ChunkStorage.INVALID_RANGE, HttpStatus.SC_REQUESTED_RANGE_NOT_SATISFIABLE);
            }
        }
    }

    AwsServiceException getException(String message, String errorCode, int status) {
        return S3Exception.builder()
                .message(message)
                .statusCode(status)
                .awsErrorDetails(AwsErrorDetails.builder()
                        .errorCode(errorCode)
                        .sdkHttpResponse(SdkHttpResponse.builder()
                                .statusCode(status)
                                .build())
                        .build())
                .build();
    }

    CreateMultipartUploadResponse createMultipartUpload(CreateMultipartUploadRequest createMultipartUploadRequest) {
        String objectName = getObjectName(createMultipartUploadRequest.bucket(), createMultipartUploadRequest.key());
        synchronized (this.objects) {
            val id = this.multipartNextId.incrementAndGet();
            this.multipartUploads.put(Long.toString(id), objectName);
            this.multipartUploadParts.put(Long.toString(id), new HashMap<>());
            return CreateMultipartUploadResponse.builder()
                    .uploadId(Long.toString(id))
                    .build();
        }
    }

    UploadPartCopyResponse uploadPartCopy(UploadPartCopyRequest uploadPartCopyRequest) {
        synchronized (this.objects) {
            val parts = this.multipartUploadParts.get(uploadPartCopyRequest.uploadId());
            if (null == parts) {
                throw getException(S3ChunkStorage.NO_SUCH_KEY, S3ChunkStorage.NO_SUCH_KEY, HttpStatus.SC_NOT_FOUND);
            }
            parts.put(uploadPartCopyRequest.partNumber(), uploadPartCopyRequest);
            return UploadPartCopyResponse.builder()
                    .copyPartResult(CopyPartResult.builder().eTag(Long.toString(eTags.incrementAndGet())).build())
                    .build();
        }
    }

    CompleteMultipartUploadResponse completeMultipartUpload(CompleteMultipartUploadRequest completeMultipartUploadRequest) {
        String objectName = getObjectName(completeMultipartUploadRequest.bucket(), completeMultipartUploadRequest.key());
        synchronized (this.objects) {
            val partMap = this.multipartUploadParts.get(completeMultipartUploadRequest.uploadId());
            if (partMap == null) {
                throw getException(S3ChunkStorage.NO_SUCH_KEY, S3ChunkStorage.NO_SUCH_KEY, HttpStatus.SC_NOT_FOUND);
            }

            if (!this.objects.containsKey(objectName)) {
                throw getException(S3ChunkStorage.NO_SUCH_KEY, S3ChunkStorage.NO_SUCH_KEY, HttpStatus.SC_NOT_FOUND);
            }

            val builder = BufferView.builder();

            for (int i = 1; i <= partMap.size(); i++) {
                val part = partMap.get(i);
                if (null == part) {
                    // Make sure all the parts are there.
                    throw getException(S3ChunkStorage.INVALID_PART, S3ChunkStorage.INVALID_PART, HttpStatus.SC_BAD_REQUEST);
                }
                String partObjectName = getObjectName(part.sourceBucket(), part.sourceKey());
                ObjectData od = this.objects.get(partObjectName);
                if (od == null) {
                    throw getException(S3ChunkStorage.NO_SUCH_KEY, S3ChunkStorage.NO_SUCH_KEY, HttpStatus.SC_NOT_FOUND);
                }
                builder.add(od.content);
            }

            this.objects.get(objectName).content = builder.build();
            this.multipartUploads.remove(completeMultipartUploadRequest.uploadId());
            this.multipartUploadParts.remove(completeMultipartUploadRequest.uploadId());

            return CompleteMultipartUploadResponse.builder().build();
        }
    }

    AbortMultipartUploadResponse abortMultipartUpload(AbortMultipartUploadRequest request) {
        synchronized (this.objects) {
            val partMap = this.multipartUploads.remove(request.uploadId());
            if (partMap == null) {
                throw getException(S3ChunkStorage.NO_SUCH_KEY, S3ChunkStorage.NO_SUCH_KEY, HttpStatus.SC_NOT_FOUND);
            }
            this.multipartUploadParts.remove(request.uploadId());

            return AbortMultipartUploadResponse.builder().build();
        }
    }

    //endregion

    //region Helper classes

    @NotThreadSafe
    @AllArgsConstructor
    static class ObjectData {
        BufferView content;
        ObjectCannedACL acl;
    }

    //endregion
}

