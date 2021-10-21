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
package io.pravega.cli.user.utils;

import lombok.val;

import java.util.Scanner;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Acquires console input on a background thread and updates its state ({@link #isTriggered()} when a desired token
 * has been typed in.
 */
public class BackgroundConsoleListener implements AutoCloseable {
    private final AtomicBoolean triggered = new AtomicBoolean(false);
    private final String token;

    public BackgroundConsoleListener() {
        this("q");
    }

    BackgroundConsoleListener(String token) {
        this.token = token.trim().toLowerCase();
    }

    public boolean isTriggered() {
        return this.triggered.get();
    }

    public void start() {
        this.triggered.set(false);
        val t = new Thread(() -> {
            System.out.println(String.format("Press '%s <enter>' to cancel ongoing operation.", this.token));
            @SuppressWarnings("resource")
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

    public void stop() {
        this.triggered.set(true);
    }

    @Override
    public void close() {
        stop();
    }
}
