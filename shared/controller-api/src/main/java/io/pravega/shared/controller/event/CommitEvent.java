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
package io.pravega.shared.controller.event;

import io.pravega.common.ObjectBuilder;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

@Builder
@Data
@AllArgsConstructor
public class CommitEvent implements ControllerEvent {
    @SuppressWarnings("unused")
    private static final long serialVersionUID = 1L;
    private final String scope;
    private final String stream;
    private final int epoch;

    @Override
    public String getKey() {
        return String.format("%s/%s", scope, stream);
    }

    @Override
    public CompletableFuture<Void> process(RequestProcessor processor) {
        return ((StreamRequestProcessor) processor).processCommitTxnRequest(this);
    }

    //region Serialization

    private static class CommitEventBuilder implements ObjectBuilder<CommitEvent> {
    }

    public static class Serializer extends VersionedSerializer.WithBuilder<CommitEvent, CommitEventBuilder> {
        @Override
        protected CommitEventBuilder newBuilder() {
            return CommitEvent.builder();
        }

        @Override
        protected byte getWriteVersion() {
            return 0;
        }

        @Override
        protected void declareVersions() {
            version(0).revision(0, this::write00, this::read00);
        }

        private void write00(CommitEvent e, RevisionDataOutput target) throws IOException {
            target.writeUTF(e.scope);
            target.writeUTF(e.stream);
            target.writeCompactInt(e.epoch);
        }

        private void read00(RevisionDataInput source, CommitEventBuilder b) throws IOException {
            b.scope(source.readUTF());
            b.stream(source.readUTF());
            b.epoch(source.readCompactInt());
        }
    }

    //endregion
}
