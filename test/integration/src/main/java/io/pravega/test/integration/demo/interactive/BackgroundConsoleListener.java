/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.integration.demo.interactive;

import java.util.Scanner;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.val;

/**
 * Acquires console input on a background thread and updates its state ({@link #isTriggered()} when a desired token
 * has been typed in.
 */
class BackgroundConsoleListener implements AutoCloseable {
    private final AtomicBoolean triggered = new AtomicBoolean(false);
    private final String token;

    BackgroundConsoleListener() {
        this("q");
    }

    BackgroundConsoleListener(String token) {
        this.token = token.trim().toLowerCase();
    }

    boolean isTriggered() {
        return this.triggered.get();
    }

    void start() {
        this.triggered.set(false);
        val t = new Thread(() -> {
            System.out.println(String.format("Press '%s <enter>' to cancel ongoing operation.", this.token));
            Scanner s = new Scanner(System.in);
            while (!this.triggered.get()) {
                String input = s.next();
                if (input.trim().toLowerCase().equals(this.token)) {
                    stop();
                }
            }
        });
        t.start();
    }

    void stop() {
        this.triggered.set(true);
    }

    @Override
    public void close() {
        stop();
    }
}
