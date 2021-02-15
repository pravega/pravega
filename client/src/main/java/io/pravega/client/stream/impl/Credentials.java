/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.stream.impl;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * This legacy interface represented the credentials passed to Pravega for authentication and authorizing the
 * access. It is retained temporarily for compatibility with older implementations only. Use
 * {@link io.pravega.shared.security.auth.Credentials} instead.
 *
 * @deprecated As of Pravega release 0.9, replaced by {@link io.pravega.shared.security.auth.Credentials}.
 */
@Deprecated
@SuppressFBWarnings(value = "NM_SAME_SIMPLE_NAME_AS_INTERFACE",
        justification = "Interface with same name retained for compatibility with older implementations")
public interface Credentials extends io.pravega.shared.security.auth.Credentials {
}

