/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common.util;

/**
 * Exception that is thrown whenever a data formatting error was detected.
 */
public class IllegalDataFormatException extends IllegalArgumentException {
    public IllegalDataFormatException(String message, Object... args) {
        super(String.format(message, args));
    }
}
