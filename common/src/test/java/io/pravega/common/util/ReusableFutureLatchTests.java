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
package io.pravega.common.util;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Test;

import static io.pravega.test.common.AssertExtensions.assertThrows;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ReusableFutureLatchTests {

    @Test(timeout = 5000)
    public void testRelease() throws InterruptedException, ExecutionException {
        ReusableFutureLatch<String> latch = new ReusableFutureLatch<>();
        CompletableFuture<String> str1 = new CompletableFuture<>();
        CompletableFuture<String> str2 = new CompletableFuture<>();
        latch.register(str1);
        latch.register(str2);
        assertFalse(str1.isDone());
        assertFalse(str2.isDone());
        latch.release("Done");
        assertTrue(str1.isDone());
        assertTrue(str2.isDone());
        assertEquals("Done", str1.get());
        assertEquals("Done", str2.get());
    }
    
    @Test(timeout = 5000)
    public void testRunReleaser() throws InterruptedException, ExecutionException {
        AtomicBoolean ran1 = new AtomicBoolean(false);
        AtomicBoolean ran2 = new AtomicBoolean(false);
        ReusableFutureLatch<String> latch = new ReusableFutureLatch<>();
        CompletableFuture<String> str1 = new CompletableFuture<>();
        CompletableFuture<String> str2 = new CompletableFuture<>();
        latch.registerAndRunReleaser(() -> {
            ran1.set(true);
        }, str1);
        latch.registerAndRunReleaser(() -> {
            ran2.set(true);
        }, str2);
        assertFalse(str1.isDone());
        assertFalse(str2.isDone());
        
        latch.release("Done");
        assertTrue(str1.isDone());
        assertTrue(str2.isDone());
        assertEquals("Done", str1.get());
        assertEquals("Done", str2.get());
        assertTrue(ran1.get());
        assertFalse(ran2.get());
    }
    
    @Test(timeout = 5000)
    public void testReleaseExceptionally() {
        ReusableFutureLatch<String> latch = new ReusableFutureLatch<>();
        CompletableFuture<String> str1 = new CompletableFuture<>();
        CompletableFuture<String> str2 = new CompletableFuture<>();
        latch.register(str1);
        latch.register(str2);
        assertFalse(str1.isDone());
        assertFalse(str2.isDone());
        latch.releaseExceptionally(new RuntimeException("Foo"));
        assertTrue(str1.isCompletedExceptionally());
        assertTrue(str2.isCompletedExceptionally());
        assertThrows("Wrong exception", () -> str1.get(),
                     e -> e instanceof RuntimeException && e.getMessage().equals("Foo"));
        assertThrows("Wrong exception", () -> str2.get(),
                     e -> e instanceof RuntimeException && e.getMessage().equals("Foo"));
    }
    
    @Test(timeout = 5000)
    public void testAlreadyRelease() throws InterruptedException, ExecutionException {
        ReusableFutureLatch<String> latch = new ReusableFutureLatch<>();
        CompletableFuture<String> str1 = new CompletableFuture<>();
        CompletableFuture<String> str2 = new CompletableFuture<>();
        latch.release("Done");
        latch.register(str1);
        latch.register(str2);
        assertTrue(str1.isDone());
        assertTrue(str2.isDone());
        assertEquals("Done", str1.get());
        assertEquals("Done", str2.get());
    }
    
    @Test(timeout = 5000)
    public void testReset() throws InterruptedException, ExecutionException {
        ReusableFutureLatch<String> latch = new ReusableFutureLatch<>();
        CompletableFuture<String> str1 = new CompletableFuture<>();
        CompletableFuture<String> str2 = new CompletableFuture<>();
        latch.register(str1);
        latch.release("1");
        latch.reset();
        latch.register(str2);
        assertTrue(str1.isDone());
        assertEquals("1", str1.get());
        assertFalse(str2.isDone());
        latch.release("Done");
        assertTrue(str2.isDone());
        assertEquals("Done", str2.get());
    }
    
    @Test(timeout = 5000)
    public void testReleaseExceptionallyAndReset() {
        ReusableFutureLatch<String> latch = new ReusableFutureLatch<>();
        CompletableFuture<String> str1 = new CompletableFuture<>();
        CompletableFuture<String> str2 = new CompletableFuture<>();
        latch.register(str1);
        assertFalse(str1.isDone());
        latch.releaseExceptionallyAndReset(new RuntimeException("Foo"));
        assertTrue(str1.isCompletedExceptionally());        
        assertThrows("Wrong exception", () -> str1.get(),
                     e -> e instanceof RuntimeException && e.getMessage().equals("Foo"));    
        latch.register(str2);
        assertFalse(str2.isDone());
        latch.releaseExceptionallyAndReset(new RuntimeException("Bar"));
        assertTrue(str2.isCompletedExceptionally());
        assertThrows("Wrong exception", () -> str2.get(),
                     e -> e instanceof RuntimeException && e.getMessage().equals("Bar"));
    }
    
}
