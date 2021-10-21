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
package io.pravega.controller.store.task;

import com.google.common.base.Preconditions;
import io.pravega.common.Exceptions;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.UUID;

/**
 * Transaction resource consisting of scope name, stream name, and txn id.
 * The host-transaction index tracks transaction resources a specific host is managing timeouts for.
 * We maintain a list of transaction resources against a host in the host-transaction index.
 */
@Data
@EqualsAndHashCode(callSuper = true)
public class TxnResource extends Resource {
    private final String scope;
    private final String stream;
    private final UUID txnId;

    public TxnResource(String scope, String stream, UUID txnId) {
        super(scope, stream, txnId.toString());
        Exceptions.checkNotNullOrEmpty(scope, "scope");
        Exceptions.checkNotNullOrEmpty(stream, "stream");
        this.scope = scope;
        this.stream = stream;
        this.txnId = txnId;
    }

    public String toString(String delimiter) {
        return String.join(delimiter, scope, stream, txnId.toString());
    }

    public static TxnResource parse(String str, String delimiter) {
        Exceptions.checkNotNullOrEmpty(str, "str");
        Exceptions.checkNotNullOrEmpty(delimiter, "delimiter");
        String[] parts = str.split(delimiter);
        Preconditions.checkArgument(parts.length == 3, "Txn resource requires scope, stream, and txnId");
        return new TxnResource(parts[0], parts[1], UUID.fromString(parts[2]));
    }
}
