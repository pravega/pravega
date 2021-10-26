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
package io.pravega.test.system.framework;

import lombok.extern.slf4j.Slf4j;

/**
 * TestFrameworkException is notify / convey errors (non-recoverable) while interacting with Marathon, Metronome
 * frameworks.
 */
@Slf4j
public class TestFrameworkException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public enum Type {
        ConnectionFailed,
        RequestFailed,
        LoginFailed,
        InternalError,
    }

    private final Type type;

    public TestFrameworkException(Type type, String reason, Throwable cause) {
        super(reason, cause);
        this.type = type;
        log.warn("TestFramework Exception. Type: {}, Details: {}", type, reason, cause);
    }

    public TestFrameworkException(Type type, String reason) {
        super(reason);
        this.type = type;
        log.warn("TestFramework Exception. Type: {}, Details: {}", type, reason);
    }
}
