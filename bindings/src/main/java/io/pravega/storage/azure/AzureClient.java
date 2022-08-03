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

import java.io.InputStream;

/**
 * AzureClient class allows methods to manipulate Azure Storage containers and their blobs.
 */
public interface AzureClient extends AutoCloseable {

    /**
     * Creates a 0-length append blob.
     * @param blobName Name of the blob on which operation is performed.
     * @return The created appended blob.
     */
    AppendBlobItem create(String blobName);

    /**
     * Gets if the blob this client represents exists in the cloud.
     * @param blobName name of the blob on which operation is performed.
     * @return True if the blob exists, false if it doesn't.
     */
    boolean exists(String blobName);

    /**
     * Deletes the specified blob.
     * @param blobName Name of the blob on which operation is performed.
     */
    void delete(String blobName);

    /**
     * Copy fixed bytes of data from the chunk and return the resulting byte[].
     * @param blobName Name of the blob on which operation is performed.
     * @param offSet Blob offset position from which we need to start to copy the bytes.
     * @param length Length of the blob whose data is to be copied.
     * @return Input stream of byte[].
     */
    InputStream getInputStream(String blobName, long offSet, long length);

    /**
     * Commits a new block of data to the end of an existing Append blob.
     * @param blobName Name of the blob on which operation is performed.
     * @param offSet Blob offset position from which we can append the bytes.
     * @param length Length of the blob on which append operation is performed.
     * @param inputStream Input stream of byte[].
     * @return The resulting created appended blob.
     */
    AppendBlobItem appendBlock(String blobName, long offSet, long length, InputStream inputStream);

    /**
     * Gets blob properties and metadata.
     * @param blobName Name of the blob on which operation is performed.
     * @return Returns blob properties and metadata for specified blob.
     */
    BlobProperties getBlobProperties(String blobName);
}
