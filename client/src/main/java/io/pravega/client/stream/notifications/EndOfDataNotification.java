/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.stream.notifications;

/**
 * Class to represent a end of data notification. This notification is generated when all the streams(sealed) managed by
 * the reader group are completely read by the readers.
 */
public class EndOfDataNotification extends Notification {
}
