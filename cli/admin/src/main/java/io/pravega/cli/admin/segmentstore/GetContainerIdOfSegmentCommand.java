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


package io.pravega.cli.admin.segmentstore;


import io.pravega.cli.admin.CommandArgs;
import io.pravega.shared.segment.SegmentToContainerMapper;

/**
 * Gets the container id of the given segment.
 *  The arguments for the command that is the fully qualified segment name.
 */
public class GetContainerIdOfSegmentCommand extends SegmentStoreCommand {

    private final SegmentToContainerMapper segToConMapper;
    private final int containerCount;
    public GetContainerIdOfSegmentCommand(CommandArgs args) {
        super(args);
        this.containerCount = getServiceConfig().getContainerCount();
        this.segToConMapper = new SegmentToContainerMapper(this.containerCount, true);
    }

    @Override
    public void execute() throws Exception {
        final String fullyQualifiedSegmentName = getArg(0);
        int containerId = segToConMapper.getContainerId(fullyQualifiedSegmentName);
        System.out.println("Container Id for the given Segment is :" + containerId);
}

    public static CommandDescriptor descriptor() {

        return new CommandDescriptor(COMPONENT, "get-container-id", "Get the Id of a Container that belongs to a segment.",
                new ArgDescriptor("qualified-segment-name", "Fully qualified name of the Segment to get info from (e.g., scope/stream/0.#epoch.0)."));
    }
}