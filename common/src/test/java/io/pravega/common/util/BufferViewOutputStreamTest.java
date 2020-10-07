/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common.util;

import io.pravega.test.common.AssertExtensions;
import java.io.IOException;
import java.io.InputStream;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class BufferViewOutputStreamTest {

    @Test
    public void testSingleByteWrite() throws IOException {
        BufferViewOutputStream out = new BufferViewOutputStream();
        for (int i = 0; i < 3 * BufferViewOutputStream.CHUNK_SIZE + 1; i++) {
            out.write(i);
        }
        BufferView view = out.getView();
        assertEquals(4, view.getContents().size());
        assertEquals(3 * BufferViewOutputStream.CHUNK_SIZE + 1, view.getLength());
        InputStream reader = view.getReader();
        assertEquals(3 * BufferViewOutputStream.CHUNK_SIZE + 1, reader.readAllBytes().length);
        out.close();
        assertEquals(4, view.getContents().size());
        assertEquals(3 * BufferViewOutputStream.CHUNK_SIZE + 1, view.getLength());
    }

    @Test
    public void testEmpty() {
        BufferViewOutputStream out = new BufferViewOutputStream();
        out.write(new byte[0], 0, 0);
        out.write(new byte[0], 0, 0);
        out.write(new byte[0], 0, 0);
        BufferView view = out.getView();
        assertEquals(0, view.getContents().size());
        assertEquals(0, view.getLength());
        out.close();
        view = out.getView();
        assertEquals(0, view.getContents().size());
        assertEquals(0, view.getLength());
    }
    
    @Test
    public void testShort() {
        BufferViewOutputStream out = new BufferViewOutputStream();
        out.write(new byte[10]);
        out.write(new byte[10]);
        out.write(new byte[10]);
        BufferView view = out.getView();
        assertEquals(1, view.getContents().size());
        assertEquals(30, view.getLength());
        out.close();
        view = out.getView();
        assertEquals(1, view.getContents().size());
        assertEquals(30, view.getLength());
    }
    
    @Test
    public void testMedium() {
        BufferViewOutputStream out = new BufferViewOutputStream();
        for (int i = 0; i < 5; i++) {
            out.write(new byte[BufferViewOutputStream.CHUNK_SIZE / 3 + 10]);
        }
        BufferView view = out.getView();
        assertEquals(3, view.getContents().size());
        assertEquals(5 * (BufferViewOutputStream.CHUNK_SIZE / 3) + 50, view.getLength());
        out.close();
        view = out.getView();
        assertEquals(3, view.getContents().size());
        assertEquals(5 * (BufferViewOutputStream.CHUNK_SIZE / 3) + 50, view.getLength());
    }
    
    @Test
    public void testCloseFirst() {
        BufferViewOutputStream out = new BufferViewOutputStream();
        for (int i = 0; i < 5; i++) {
            out.write(new byte[BufferViewOutputStream.CHUNK_SIZE / 3 + 10]);
        }
        out.close();
        BufferView view = out.getView();
        assertEquals(3, view.getContents().size());
        assertEquals(5 * (BufferViewOutputStream.CHUNK_SIZE / 3) + 50, view.getLength());
        AssertExtensions.assertThrows(IllegalStateException.class, () -> out.write(0));
        out.flush();
        out.close();
    }
    
    @Test
    public void testLarge() {
        BufferViewOutputStream out = new BufferViewOutputStream();
        for (int i = 0; i < 5; i++) {
            out.write(new byte[BufferViewOutputStream.CHUNK_SIZE + 10]);
        }
        BufferView view = out.getView();
        assertEquals(5, view.getContents().size());
        assertEquals(5 * BufferViewOutputStream.CHUNK_SIZE + 50, view.getLength());
        out.close();
        view = out.getView();
        assertEquals(5, view.getContents().size());
        assertEquals(5 * BufferViewOutputStream.CHUNK_SIZE + 50, view.getLength());
    }
    
}
