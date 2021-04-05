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
package io.pravega.shared.security.auth;

import io.pravega.common.Exceptions;
import io.pravega.shared.NameUtils;
import lombok.NonNull;

/**
 * The main implementation of the {@link AuthorizationResource} class.
 */
public class AuthorizationResourceImpl implements AuthorizationResource {
    public static final String DOMAIN_PART_SUFFIX = "prn::";
    private static final String TAG_SCOPE = "scope";
    private static final String TAG_STREAM = "stream";
    private static final String TAG_READERGROUP = "reader-group";
    private static final String TAG_KEYVALUETABLE = "key-value-table";

    private static final String ROOT_RESOURCE = String.format("%s/", DOMAIN_PART_SUFFIX);

    @Override
    public String ofScopes() {
        return ROOT_RESOURCE;
    }

    @Override
    public String ofScope(String scopeName) {
        Exceptions.checkNotNullOrEmpty(scopeName, "scopeName");
        return String.format("%s/%s:%s", DOMAIN_PART_SUFFIX, TAG_SCOPE, scopeName);
    }

    @Override
    public String ofStreamsInScope(String scopeName) {
        return ofScope(scopeName);
    }

    @Override
    public String ofStreamInScope(String scopeName, String streamName) {
        Exceptions.checkNotNullOrEmpty(scopeName, "scopeName");
        Exceptions.checkNotNullOrEmpty(streamName, "streamName");
        return String.format("%s/%s:%s", ofScope(scopeName), TAG_STREAM, streamName);
    }

    @Override
    public String ofReaderGroupsInScope(String scopeName) {
        return ofScope(scopeName);
    }

    @Override
    public String ofReaderGroupInScope(String scopeName, String readerGroupName) {
        Exceptions.checkNotNullOrEmpty(scopeName, "scopeName");
        Exceptions.checkNotNullOrEmpty(readerGroupName, "readerGroupName");
        return String.format("%s/%s:%s", ofScope(scopeName), TAG_READERGROUP, readerGroupName);
    }

    @Override
    public String ofKeyValueTablesInScope(String scopeName) {
        return ofScope(scopeName);
    }

    @Override
    public String ofKeyValueTableInScope(String scopeName, String keyValueTableName) {
        Exceptions.checkNotNullOrEmpty(scopeName, "scopeName");
        Exceptions.checkNotNullOrEmpty(keyValueTableName, "keyValueTableName");
        return String.format("%s/%s:%s", ofScope(scopeName), TAG_KEYVALUETABLE, keyValueTableName);
    }

    @Override
    public String ofInternalStream(String scopeName, @NonNull String streamName) {
        if (streamName.startsWith(NameUtils.READER_GROUP_STREAM_PREFIX)) {
            return ofReaderGroupInScope(scopeName, streamName.replace(NameUtils.READER_GROUP_STREAM_PREFIX, ""));
        } else if (streamName.startsWith(NameUtils.getMARK_PREFIX())) {
            return ofStreamInScope(scopeName, streamName.replace(NameUtils.getMARK_PREFIX(), ""));
        } else {
            return ofStreamInScope(scopeName, streamName);
        }
    }
}
