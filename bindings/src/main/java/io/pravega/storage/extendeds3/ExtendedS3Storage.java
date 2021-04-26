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
import com.emc.object.s3.S3Client;
import com.emc.object.s3.S3Exception;
import com.emc.object.s3.S3ObjectMetadata;
import com.emc.object.s3.bean.AccessControlList;
import com.emc.object.s3.bean.CanonicalUser;
import com.emc.object.s3.bean.CopyPartResult;
import com.emc.object.s3.bean.Grant;
import com.emc.object.s3.bean.ListObjectsResult;
import com.emc.object.s3.bean.MultipartPartETag;
import com.emc.object.s3.bean.Permission;
import com.emc.object.s3.bean.S3Object;
import com.emc.object.s3.request.CompleteMultipartUploadRequest;
import com.emc.object.s3.request.CopyPartRequest;
import com.emc.object.s3.request.PutObjectRequest;
import com.emc.object.s3.request.SetObjectAclRequest;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.pravega.common.Exceptions;
import io.pravega.common.LoggerHelpers;
import io.pravega.common.Timer;
import io.pravega.common.io.StreamHelpers;
import io.pravega.common.util.ImmutableDate;
import io.pravega.segmentstore.contracts.BadOffsetException;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.StreamSegmentException;
import io.pravega.segmentstore.contracts.StreamSegmentExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentInformation;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentSealedException;
import io.pravega.segmentstore.storage.SegmentHandle;
import io.pravega.segmentstore.storage.SyncStorage;
import java.io.BufferedInputStream;
import java.io.InputStream;
import java.time.Duration;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpStatus;

/**
 * Storage adapter for extended S3 based storage.
 *
 * Each segment is represented as a single Object on the underlying storage.
 *
 * This implementation works under the assumption that data is only appended and never modified.
 * Each block of data has an initial offset assigned to it. The data and the initial offset is stored in DurableLog.
 * In case of retries, Pravega always writes the same data to the same offset. As a result the only flow when a write
 * call is made to the same offset twice is when ownership of the segment changes from one host to another and both
 * the hosts are writing to it.
 *
 * As PutObject calls with the same start-offset to an Extended S3 object are idempotent (any attempt to re-write
 * data with the same file offset does not cause any form of inconsistency), fencing is not required.
 *
 * ZkSegmentContainerMonitor watches the shared zk entry that contains the segment container ownership information
 * and starts or stops appropriate segment containers locally. Any access to the segment from the new host causes the
 * ownership change.
 *
 * Here is the expected behavior in case of ownership change: both the hosts will keep writing the same data at the
 * same offset till the time the earlier owner gets a notification that it is not the current owner. Once the earlier
 * owner received this notification, it stops writing to the segment.
 *
 * The concat operation is implemented as multi part copy. This ensures that the objects are copied server side.
 * Multi part copy calls are idempotent too. Copying the same object at the same offset multiple times from different
 * hosts does not cause any form of inconsistency.
 *
 */

@Slf4j
public class ExtendedS3Storage implements SyncStorage {

    //region members
    private static final Permission READ_ONLY_PERMISSION = Permission.READ;
    private static final Permission READ_WRITE_PERMISSION = Permission.FULL_CONTROL;

    private final ExtendedS3StorageConfig config;
    private final S3Client client;
    private final boolean shouldClose;
    private final AtomicBoolean closed;

    //endregion

    //region constructor
    public ExtendedS3Storage(S3Client client, ExtendedS3StorageConfig config, boolean shouldClose) {
        this.config = Preconditions.checkNotNull(config, "config");
        this.client = Preconditions.checkNotNull(client, "client");
        this.closed = new AtomicBoolean(false);
        this.shouldClose = shouldClose;
    }

    //endregion


    //region Storage implementation

    /**
     * Initialize is a no op here as we do not need a locking mechanism in case of file system write.
     * @param containerEpoch The Container Epoch to initialize with (ignored here).
     */
    @Override
    public void initialize(long containerEpoch) {
    }

    @Override
    public SegmentHandle openRead(String streamSegmentName) throws StreamSegmentException {
        return execute(streamSegmentName, () -> doOpenRead(streamSegmentName));
    }

    @Override
    public int read(SegmentHandle handle, long offset, byte[] buffer, int bufferOffset, int length) throws StreamSegmentException {
        return execute(handle.getSegmentName(), () -> doRead(handle, offset, buffer, bufferOffset, length));
    }

    @Override
    public SegmentProperties getStreamSegmentInfo(String streamSegmentName) throws StreamSegmentException {
        return execute(streamSegmentName, () -> doGetStreamSegmentInfo(streamSegmentName));
    }

    @Override
    @SneakyThrows(StreamSegmentException.class)
    public boolean exists(String streamSegmentName) {
        return execute(streamSegmentName, () -> doExists(streamSegmentName));
    }

    @Override
    public SegmentHandle openWrite(String streamSegmentName) throws StreamSegmentException {
        return execute(streamSegmentName, () -> doOpenWrite(streamSegmentName));
    }

    @Override
    public SegmentHandle create(String streamSegmentName) throws StreamSegmentException {
        return execute(streamSegmentName, () -> doCreate(streamSegmentName));
    }

    @Override
    public void write(SegmentHandle handle, long offset, InputStream data, int length) throws StreamSegmentException {
        execute(handle.getSegmentName(), () -> doWrite(handle, offset, data, length));
    }

    @Override
    public void seal(SegmentHandle handle) throws StreamSegmentException {
        execute(handle.getSegmentName(), () -> doSeal(handle));
    }

    @Override
    public void unseal(SegmentHandle handle) throws StreamSegmentException {
        execute(handle.getSegmentName(), () -> doUnseal(handle));

    }

    @Override
    public void concat(SegmentHandle targetHandle, long offset, String sourceSegment) throws StreamSegmentException {
        execute(targetHandle.getSegmentName(), () -> doConcat(targetHandle, offset, sourceSegment));
    }

    @Override
    public void delete(SegmentHandle handle) throws StreamSegmentException {
        execute(handle.getSegmentName(), () -> doDelete(handle));
    }

    @Override
    public void truncate(SegmentHandle handle, long offset) {
        throw new UnsupportedOperationException(getClass().getName() + " does not support Segment truncation.");
    }

    @Override
    public boolean supportsTruncation() {
        return false;
    }

    //endregion

    //region private sync implementation

    private SegmentHandle doOpenRead(String streamSegmentName) {
        long traceId = LoggerHelpers.traceEnter(log, "openRead", streamSegmentName);

        doGetStreamSegmentInfo(streamSegmentName);
        ExtendedS3SegmentHandle retHandle = ExtendedS3SegmentHandle.getReadHandle(streamSegmentName);
        LoggerHelpers.traceLeave(log, "openRead", traceId, streamSegmentName);
        return retHandle;
    }

    private SegmentHandle doOpenWrite(String streamSegmentName) {
        long traceId = LoggerHelpers.traceEnter(log, "openWrite", streamSegmentName);
        StreamSegmentInformation info = doGetStreamSegmentInfo(streamSegmentName);
        ExtendedS3SegmentHandle retHandle;
        if (info.isSealed()) {
            retHandle = ExtendedS3SegmentHandle.getReadHandle(streamSegmentName);
        } else {
            retHandle = ExtendedS3SegmentHandle.getWriteHandle(streamSegmentName);
        }

        LoggerHelpers.traceLeave(log, "openWrite", traceId);
        return retHandle;
    }

    private int doRead(SegmentHandle handle, long offset, byte[] buffer, int bufferOffset, int length) throws Exception {
        long traceId = LoggerHelpers.traceEnter(log, "read", handle.getSegmentName(), offset, bufferOffset, length);
        Timer timer = new Timer();

        if (offset < 0 || bufferOffset < 0 || length < 0) {
            throw new ArrayIndexOutOfBoundsException();
        }

        try (InputStream reader = client.readObjectStream(config.getBucket(),
                config.getPrefix() + handle.getSegmentName(), Range.fromOffsetLength(offset, length))) {
            /*
             * TODO: This implementation assumes that if S3Client.readObjectStream returns null, then
             * the object does not exist and we throw StreamNotExistsException. The javadoc, however,
             * says that this call returns null in case of 304 and 412 responses. We need to
             * investigate what these responses mean precisely and react accordingly.
             *
             * See https://github.com/pravega/pravega/issues/1549
             */
            if (reader == null) {
                throw new StreamSegmentNotExistsException(handle.getSegmentName());
            }

            int bytesRead = StreamHelpers.readAll(reader, buffer, bufferOffset, length);

            Duration elapsed = timer.getElapsed();

            ExtendedS3Metrics.READ_LATENCY.reportSuccessEvent(elapsed);
            ExtendedS3Metrics.READ_BYTES.add(length);

            log.debug("Read segment={} offset={} bytesWritten={} latency={}.", handle.getSegmentName(), offset, length, elapsed.toMillis());

            LoggerHelpers.traceLeave(log, "read", traceId, bytesRead);
            return bytesRead;
        }
    }

    private StreamSegmentInformation doGetStreamSegmentInfo(String streamSegmentName) {
        long traceId = LoggerHelpers.traceEnter(log, "getStreamSegmentInfo", streamSegmentName);
        S3ObjectMetadata result = client.getObjectMetadata(config.getBucket(),
                config.getPrefix() + streamSegmentName);

        AccessControlList acls = client.getObjectAcl(config.getBucket(), config.getPrefix() + streamSegmentName);
        boolean canWrite = acls.getGrants().stream().anyMatch(grant -> grant.getPermission().compareTo(Permission.WRITE) >= 0);
        StreamSegmentInformation information = StreamSegmentInformation.builder()
                .name(streamSegmentName)
                .length(result.getContentLength())
                .sealed(!canWrite)
                .lastModified(new ImmutableDate(result.getLastModified().toInstant().toEpochMilli()))
                .build();

        LoggerHelpers.traceLeave(log, "getStreamSegmentInfo", traceId, streamSegmentName);
        return information;
    }

    private boolean doExists(String streamSegmentName) {
        try {
            client.getObjectMetadata(config.getBucket(), config.getPrefix() + streamSegmentName);
            return true;
        } catch (S3Exception e) {
            if (e.getErrorCode().equals("NoSuchKey")) {
                return false;
            } else {
                throw e;
            }
        }
    }

    private SegmentHandle doCreate(String streamSegmentName) throws StreamSegmentExistsException {
        long traceId = LoggerHelpers.traceEnter(log, "create", streamSegmentName);

        Timer timer = new Timer();

        if (!client.listObjects(config.getBucket(), config.getPrefix() + streamSegmentName).getObjects().isEmpty()) {
            throw new StreamSegmentExistsException(streamSegmentName);
        }

        S3ObjectMetadata metadata = new S3ObjectMetadata();
        metadata.setContentLength((long) 0);

        PutObjectRequest request = new PutObjectRequest(config.getBucket(), config.getPrefix() + streamSegmentName, null);

        AccessControlList acl = new AccessControlList();
        acl.addGrants(new Grant(new CanonicalUser(config.getAccessKey(), config.getAccessKey()), READ_WRITE_PERMISSION));
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

        Duration elapsed = timer.getElapsed();

        ExtendedS3Metrics.CREATE_LATENCY.reportSuccessEvent(elapsed);
        ExtendedS3Metrics.CREATE_COUNT.inc();

        log.debug("Create segment={} latency={}.", streamSegmentName, elapsed.toMillis());

        LoggerHelpers.traceLeave(log, "create", traceId);
        return ExtendedS3SegmentHandle.getWriteHandle(streamSegmentName);
    }

    private Void doWrite(SegmentHandle handle, long offset, InputStream data, int length) throws StreamSegmentException {
        Preconditions.checkArgument(!handle.isReadOnly(), "handle must not be read-only.");
        Timer timer = new Timer();
        long traceId = LoggerHelpers.traceEnter(log, "write", handle.getSegmentName(), offset, length);

        SegmentProperties si = doGetStreamSegmentInfo(handle.getSegmentName());

        if (si.isSealed()) {
            throw new StreamSegmentSealedException(handle.getSegmentName());
        }

        if (si.getLength() != offset) {
            throw new BadOffsetException(handle.getSegmentName(), si.getLength(), offset);
        }

        client.putObject(this.config.getBucket(), this.config.getPrefix() + handle.getSegmentName(),
                Range.fromOffsetLength(offset, length), data);

        Duration elapsed = timer.getElapsed();

        ExtendedS3Metrics.WRITE_LATENCY.reportSuccessEvent(elapsed);
        ExtendedS3Metrics.WRITE_BYTES.add(length);

        log.debug("Write segment={} offset={} bytesWritten={} latency={}.", handle.getSegmentName(), offset, length, elapsed.toMillis());

        LoggerHelpers.traceLeave(log, "write", traceId);
        return null;
    }

    private Void doSeal(SegmentHandle handle) {
        Preconditions.checkArgument(!handle.isReadOnly(), "handle must not be read-only.");
        long traceId = LoggerHelpers.traceEnter(log, "seal", handle.getSegmentName());
        setPermission(handle, READ_ONLY_PERMISSION);
        LoggerHelpers.traceLeave(log, "seal", traceId);
        return null;
    }

    private Void doUnseal(SegmentHandle handle) {
        long traceId = LoggerHelpers.traceEnter(log, "unseal", handle.getSegmentName());
        setPermission(handle, READ_WRITE_PERMISSION);
        LoggerHelpers.traceLeave(log, "unseal", traceId);
        return null;
    }

    private void setPermission(SegmentHandle handle, Permission permission) {
        AccessControlList acl = client.getObjectAcl(config.getBucket(), config.getPrefix() + handle.getSegmentName());
        acl.getGrants().clear();
        acl.addGrants(new Grant(new CanonicalUser(config.getAccessKey(), config.getAccessKey()), permission));

        client.setObjectAcl(
                new SetObjectAclRequest(config.getBucket(), config.getPrefix() + handle.getSegmentName()).withAcl(acl));
    }

    /**
     * The concat is implemented using extended S3 implementation of multipart copy API. Please see here for
     * more detail on multipart copy:
     * http://docs.aws.amazon.com/AmazonS3/latest/dev/CopyingObjctsUsingLLJavaMPUapi.html
     *
     * The multipart copy is an atomic operation. We schedule two parts and commit them atomically using
     * completeMultiPartUpload call. Specifically, to concatenate, we are copying the target segment T and the
     * source segment S to T, so essentially we are doing T <- T + S.
     */
    private Void doConcat(SegmentHandle targetHandle, long offset, String sourceSegment) throws Exception {
        Preconditions.checkArgument(!targetHandle.isReadOnly(), "target handle must not be read-only.");
        long traceId = LoggerHelpers.traceEnter(log, "concat", targetHandle.getSegmentName(), offset, sourceSegment);
        Timer timer = new Timer();

        String targetPath = config.getPrefix() + targetHandle.getSegmentName();
        // check whether the target exists
        if (!doExists(targetHandle.getSegmentName())) {
            throw new StreamSegmentNotExistsException(targetHandle.getSegmentName());
        }
        // check whether the source is sealed
        SegmentProperties si = doGetStreamSegmentInfo(sourceSegment);
        String sourcePath = config.getPrefix() + sourceSegment;
        Preconditions.checkState(si.isSealed(), "Cannot concat segment '%s' into '%s' because it is not sealed.",
                sourceSegment, targetHandle.getSegmentName());

        if (config.getSmallObjectSizeLimitForConcat() < si.getLength()) {
            doConcatWithMultipartUpload(targetPath, sourceSegment, offset);
            ExtendedS3Metrics.LARGE_CONCAT_COUNT.inc();
        } else {
            doConcatWithAppend(targetPath, sourcePath, offset, si.getLength());
        }
        // Now delete the source object.
        client.deleteObject(config.getBucket(), sourcePath);

        Duration elapsed = timer.getElapsed();
        log.debug("Concat target={} source={} offset={} bytesWritten={} latency={}.", targetHandle.getSegmentName(), sourceSegment, offset, si.getLength(), elapsed.toMillis());

        ExtendedS3Metrics.CONCAT_LATENCY.reportSuccessEvent(elapsed);
        ExtendedS3Metrics.CONCAT_BYTES.add(si.getLength());
        ExtendedS3Metrics.CONCAT_COUNT.inc();

        LoggerHelpers.traceLeave(log, "concat", traceId);

        return null;
    }

    private void doConcatWithAppend(String targetPath, String sourcePath, long offset, long length) throws Exception {
        try (InputStream reader = client.readObjectStream(config.getBucket(),
                sourcePath, Range.fromOffsetLength(0, length))) {
            client.putObject(this.config.getBucket(),
                    targetPath,
                    Range.fromOffsetLength(offset, length),
                    new BufferedInputStream(reader, Math.toIntExact(length)));
        }
    }

    private void doConcatWithMultipartUpload(String targetPath, String sourceSegment, long offset) {
        String uploadId = client.initiateMultipartUpload(config.getBucket(), targetPath);

        SortedSet<MultipartPartETag> partEtags = new TreeSet<>();
        //Copy the first part
        CopyPartRequest copyRequest = new CopyPartRequest(config.getBucket(),
                targetPath,
                config.getBucket(),
                targetPath,
                uploadId,
                1).withSourceRange(Range.fromOffsetLength(0, offset));
        CopyPartResult copyResult = client.copyPart(copyRequest);

        partEtags.add(new MultipartPartETag(copyResult.getPartNumber(), copyResult.getETag()));

        //Copy the second part
        S3ObjectMetadata metadataResult = client.getObjectMetadata(config.getBucket(),
                config.getPrefix() + sourceSegment);
        long objectSize = metadataResult.getContentLength(); // in bytes

        copyRequest = new CopyPartRequest(config.getBucket(),
                config.getPrefix() + sourceSegment,
                config.getBucket(),
                targetPath,
                uploadId,
                2).withSourceRange(Range.fromOffsetLength(0, objectSize));

        copyResult = client.copyPart(copyRequest);
        partEtags.add(new MultipartPartETag(copyResult.getPartNumber(), copyResult.getETag()));

        //Close the upload
        client.completeMultipartUpload(new CompleteMultipartUploadRequest(config.getBucket(),
                targetPath, uploadId).withParts(partEtags));
    }

    private Void doDelete(SegmentHandle handle) {
        long traceId = LoggerHelpers.traceEnter(log, "delete", handle.getSegmentName());
        Timer timer = new Timer();
        client.deleteObject(config.getBucket(), config.getPrefix() + handle.getSegmentName());
        Duration elapsed = timer.getElapsed();

        ExtendedS3Metrics.DELETE_LATENCY.reportSuccessEvent(elapsed);
        ExtendedS3Metrics.DELETE_COUNT.inc();

        log.debug("Delete segment={} latency={}.", handle.getSegmentName(), elapsed.toMillis());

        LoggerHelpers.traceLeave(log, "delete", traceId);
        return null;
    }

    private <T> T throwException(String segmentName, Exception e) throws StreamSegmentException {
        if (e instanceof S3Exception) {
            S3Exception s3Exception = (S3Exception) e;
            String errorCode = Strings.nullToEmpty(s3Exception.getErrorCode());

            if (errorCode.equals("NoSuchKey")) {
                throw new StreamSegmentNotExistsException(segmentName);
            }

            if (errorCode.equals("PreconditionFailed")) {
                throw new StreamSegmentExistsException(segmentName);
            }

            if (errorCode.equals("InvalidRange")
                    || errorCode.equals("InvalidArgument")
                    || errorCode.equals("MethodNotAllowed")
                    || s3Exception.getHttpCode() == HttpStatus.SC_REQUESTED_RANGE_NOT_SATISFIABLE) {
                throw new IllegalArgumentException(segmentName, e);
            }

            if (errorCode.equals("AccessDenied")) {
                throw new StreamSegmentSealedException(segmentName, e);
            }
        }

        if (e instanceof IndexOutOfBoundsException) {
            throw new ArrayIndexOutOfBoundsException(e.getMessage());
        }

        throw Exceptions.sneakyThrow(e);
    }

    /**
     * Executes the given Callable and returns its result, while translating any Exceptions bubbling out of it into
     * StreamSegmentExceptions.
     *
     * @param segmentName   Full name of the StreamSegment.
     * @param operation     The function to execute.
     * @param <R>           Return type of the operation.
     * @return Instance of the return type of the operation.
     */
    private <R> R execute(String segmentName, Callable<R> operation) throws StreamSegmentException {
        Exceptions.checkNotClosed(this.closed.get(), this);
        try {
            return operation.call();
        } catch (Exception e) {
            return throwException(segmentName, e);
        }
    }

    //endregion

    //region AutoClosable

    @Override
    @SneakyThrows
    public void close() {
        if (shouldClose && !this.closed.getAndSet(true)) {
            this.client.destroy();
        }
    }

    //endregion

    @Override
    public Iterator<SegmentProperties> listSegments() {
        return new ExtendedS3SegmentIterator(s3object -> true);
    }

    /**
     * Iterator for segments in ExtendedS3Storage.
     */
    private class ExtendedS3SegmentIterator implements Iterator<SegmentProperties> {
        private final java.util.function.Predicate<S3Object> patternMatchPredicate;
        private final ListObjectsResult results;
        private Iterator<SegmentProperties> innerIterator;
        private boolean nextBatch;

        ExtendedS3SegmentIterator(java.util.function.Predicate<S3Object> patternMatchPredicate) {
            this.results = client.listObjects(config.getBucket(), config.getPrefix());
            this.innerIterator = this.results.getObjects().stream()
                    .filter(patternMatchPredicate)
                    .map(this::toSegmentProperties)
                    .iterator();
            this.patternMatchPredicate = patternMatchPredicate;
            this.nextBatch = false;
        }

        /**
         * Transforms a S3Object to SegmentProperties.
         * @param s3Object The S3Object to be transformed.
         * @return A SegmentProperties object.
         */
        public SegmentProperties toSegmentProperties(S3Object s3Object) {
            AccessControlList acls = client.getObjectAcl(config.getBucket(), s3Object.getKey());
            boolean canWrite = acls.getGrants().stream().anyMatch(grant -> grant.getPermission().compareTo(Permission.WRITE) >= 0);
            return StreamSegmentInformation.builder()
                    .name(s3Object.getKey().replaceFirst(config.getPrefix(), ""))
                    .length(s3Object.getSize())
                    .sealed(!canWrite)
                    .lastModified(new ImmutableDate(s3Object.getLastModified().toInstant().toEpochMilli()))
                    .build();
        }

        /**
         * Method to check the presence of next element in the iterator.
         * @return true if the next element is there, else false.
         */
        @Override
        public boolean hasNext() {
            if (innerIterator == null) {
                return false;
            }
            if (innerIterator.hasNext()) {
                return true;
            } else {
                if (nextBatch || results.getObjects().size() < results.getMaxKeys()) {
                    return false;
                }
                innerIterator = client.listMoreObjects(results).getObjects().stream()
                        .filter(patternMatchPredicate)
                        .map(this::toSegmentProperties)
                        .iterator();
                nextBatch = true;
                return innerIterator.hasNext();
            }
        }

        /**
         * Method to return the next element in the iterator.
         * @return A newly created StreamSegmentInformation class.
         * @throws NoSuchElementException in case of an unexpected failure.
         */
        @Override
        public SegmentProperties next() throws NoSuchElementException {
            return innerIterator.next();
        }
    }
}
