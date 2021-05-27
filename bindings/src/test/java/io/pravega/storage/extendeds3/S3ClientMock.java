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
import com.emc.object.s3.S3Config;
import com.emc.object.s3.S3ObjectMetadata;
import com.emc.object.s3.bean.AccessControlList;
import com.emc.object.s3.bean.CompleteMultipartUploadResult;
import com.emc.object.s3.bean.CopyPartResult;
import com.emc.object.s3.bean.DeleteObjectsResult;
import com.emc.object.s3.bean.GetObjectResult;
import com.emc.object.s3.bean.ListObjectsResult;
import com.emc.object.s3.bean.PutObjectResult;
import com.emc.object.s3.jersey.S3JerseyClient;
import com.emc.object.s3.request.AbortMultipartUploadRequest;
import com.emc.object.s3.request.CompleteMultipartUploadRequest;
import com.emc.object.s3.request.CopyPartRequest;
import com.emc.object.s3.request.DeleteObjectsRequest;
import com.emc.object.s3.request.PutObjectRequest;
import com.emc.object.s3.request.SetObjectAclRequest;
import java.io.InputStream;
import java.util.Collections;
import lombok.NonNull;
import lombok.Synchronized;

/**
 * {@link S3JerseyClient} implementation that communicates with a {@link S3Mock} storage.
 */
public class S3ClientMock extends S3JerseyClient {
    private final S3Mock s3Impl;

    public S3ClientMock(@NonNull S3Config s3Config, @NonNull S3Mock s3Impl) {
        super(s3Config);
        this.s3Impl = s3Impl;
    }

    @Override
    public DeleteObjectsResult deleteObjects(DeleteObjectsRequest request) {
        return s3Impl.deleteObjects(request);
    }

    @Synchronized
    @Override
    public PutObjectResult putObject(PutObjectRequest request) {
        return s3Impl.putObject(request);
    }

    @Synchronized
    @Override
    public void putObject(String bucketName, String key, Range range, Object content) {
        s3Impl.putObject(bucketName, key, range, content);
    }

    @Synchronized
    @Override
    public void setObjectAcl(String bucketName, String key, AccessControlList acl) {
        s3Impl.setObjectAcl(bucketName, key, acl);
    }

    @Synchronized
    @Override
    public void setObjectAcl(SetObjectAclRequest request) {
        s3Impl.setObjectAcl(request);
    }

    @Synchronized
    @Override
    public AccessControlList getObjectAcl(String bucketName, String key) {
        return s3Impl.getObjectAcl(bucketName, key);
    }

    @Override
    public void deleteObject(String bucketName, String key) {
        s3Impl.deleteObject(bucketName, key);
    }

    @Override
    public ListObjectsResult listObjects(String bucketName, String prefix) {
        return s3Impl.listObjects(bucketName, prefix);
    }

    @Override
    public ListObjectsResult listMoreObjects(ListObjectsResult lastResult) {
        ListObjectsResult result = new ListObjectsResult();
        result.setPrefix(lastResult.getPrefix());
        result.setBucketName(lastResult.getBucketName());
        result.setMaxKeys(lastResult.getMaxKeys());
        result.setObjects(Collections.emptyList());
        return result;
    }

    @Override
    public S3ObjectMetadata getObjectMetadata(String bucketName, String key) {
        return s3Impl.getObjectMetadata(bucketName, key);
    }

    @Override
    public InputStream readObjectStream(String bucketName, String key, Range range) {
        return s3Impl.readObjectStream(bucketName, key, range);
    }

    @Override
    public String initiateMultipartUpload(String bucketName, String key) {
        return s3Impl.initiateMultipartUpload(bucketName, key);
    }

    @Override
    public void abortMultipartUpload(AbortMultipartUploadRequest request) {
        s3Impl.abortMultipartUpload(request);
    }

    @Override
    public CopyPartResult copyPart(CopyPartRequest request) {
        return s3Impl.copyPart(request);
    }

    @Synchronized
    @Override
    public CompleteMultipartUploadResult completeMultipartUpload(CompleteMultipartUploadRequest request) {
        return s3Impl.completeMultipartUpload(request);
    }

    @Override
    public GetObjectResult<InputStream> getObject(String bucketName, String key) {
        return s3Impl.getObject(bucketName, key);
    }
}