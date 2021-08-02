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
import io.pravega.cli.admin.AdminCommand;
import io.pravega.cli.admin.CommandArgs;
import io.pravega.controller.server.security.auth.GrpcAuthHelper;

/**
 * Base class for all the Segment Store related commands.
 */
public abstract class SegmentStoreCommand extends AdminCommand {
    static final String COMPONENT = "segmentstore";
    protected final GrpcAuthHelper authHelper;

    public SegmentStoreCommand(CommandArgs args) {
        super(args);

        authHelper = new GrpcAuthHelper(true,
                "secret",
                600);
    }
}
