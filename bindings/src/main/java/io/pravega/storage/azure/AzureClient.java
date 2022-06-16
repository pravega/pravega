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
package io.pravega.storage.azure;

import com.azure.storage.blob.models.AppendBlobItem;
import com.azure.storage.blob.models.BlobProperties;
import com.azure.storage.blob.options.AppendBlobCreateOptions;
import com.azure.storage.blob.specialized.BlobOutputStream;

import javax.naming.Context;
import java.io.InputStream;
import java.io.OutputStream;
import java.time.Duration;

public interface AzureClient extends AutoCloseable {
    AppendBlobItem create (String blobName);
    boolean exists (String blobName);
    void delete (String blobName);
    InputStream getInputStream (String blobName, long offSet, long length);
    AppendBlobItem appendBlock (String blobName, long offSet, long length, InputStream inputStream);
    BlobProperties getBlobProperties (String blobName, long length);
}
