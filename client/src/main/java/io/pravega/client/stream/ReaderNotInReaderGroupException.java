/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.stream;

public class ReaderNotInReaderGroupException extends Exception {

    private static final long serialVersionUID = 1L;

    public ReaderNotInReaderGroupException() {
        super();
    }

    public ReaderNotInReaderGroupException(Throwable e) {
        super(e);
    }

    public ReaderNotInReaderGroupException(String msg, Throwable e) {
        super(msg, e);
    }

    public ReaderNotInReaderGroupException(String msg) {
        super(msg);
    }
}
