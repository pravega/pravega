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
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

@Builder
@Data
@AllArgsConstructor
public class ScaleOpEvent implements ControllerEvent {
    @SuppressWarnings("unused")
    private static final long serialVersionUID = 1L;
    private final String scope;
    private final String stream;
    private final List<Long> segmentsToSeal;
    private final List<Map.Entry<Double, Double>> newRanges;
    private final boolean runOnlyIfStarted;
    private final long scaleTime;
    private final long requestId;

    @Override
    public String getKey() {
        return String.format("%s/%s", scope, stream);
    }

    @Override
    public CompletableFuture<Void> process(RequestProcessor processor) {
        return ((StreamRequestProcessor) processor).processScaleOpRequest(this);
    }

    //region Serialization

    private static class ScaleOpEventBuilder implements ObjectBuilder<ScaleOpEvent> {
    }

    public static class Serializer extends VersionedSerializer.WithBuilder<ScaleOpEvent, ScaleOpEventBuilder> {
        @Override
        protected ScaleOpEventBuilder newBuilder() {
            return ScaleOpEvent.builder();
        }

        @Override
        protected byte getWriteVersion() {
            return 0;
        }

        @Override
        protected void declareVersions() {
            version(0).revision(0, this::write00, this::read00);
        }

        private void write00(ScaleOpEvent e, RevisionDataOutput target) throws IOException {
            target.writeUTF(e.scope);
            target.writeUTF(e.stream);
            target.writeCollection(e.segmentsToSeal, RevisionDataOutput::writeLong);
            target.writeCollection(e.newRanges, this::writeNewRanges00);
            target.writeBoolean(e.runOnlyIfStarted);
            target.writeLong(e.scaleTime);
            target.writeLong(e.requestId);
        }

        private void read00(RevisionDataInput source, ScaleOpEventBuilder b) throws IOException {
            b.scope(source.readUTF());
            b.stream(source.readUTF());
            b.segmentsToSeal(source.readCollection(RevisionDataInput::readLong, ArrayList::new));
            b.newRanges(source.readCollection(this::readNewRanges00, ArrayList::new));
            b.runOnlyIfStarted(source.readBoolean());
            b.scaleTime(source.readLong());
            b.requestId(source.readLong());
        }

        private void writeNewRanges00(RevisionDataOutput target, Map.Entry<Double, Double> element) throws IOException {
            target.writeDouble(element.getKey());
            target.writeDouble(element.getValue());
        }

        private Map.Entry<Double, Double> readNewRanges00(RevisionDataInput dataInput) throws IOException {
            return new AbstractMap.SimpleImmutableEntry<>(dataInput.readDouble(), dataInput.readDouble());
        }
    }

    //endregion
}
