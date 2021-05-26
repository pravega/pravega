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
package io.pravega.client.stream.notifications;

import io.pravega.client.stream.notifications.notifier.AbstractNotifier;
import java.util.concurrent.ScheduledExecutorService;

public class CustomNotifier extends AbstractNotifier<CustomNotification> {

    public CustomNotifier(final NotificationSystem system, final ScheduledExecutorService executor) {
        super(system, executor);
    }

    @Override
    public String getType() {
        return CustomNotification.class.getSimpleName();
    }
}
