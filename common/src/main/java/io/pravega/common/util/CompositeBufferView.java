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

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.SequenceInputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

/**
 * Provides a unified view of multiple wrapped {@link BufferView} instances.
 */
@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
class CompositeBufferView extends AbstractBufferView implements BufferView {
    //region Members

    private final List<BufferView> components;
    @Getter
    private final int length;
    private volatile int allocatedLength = -1;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the {@link CompositeBufferView} class. It is recommended to use {@link BufferView#wrap}
     * instead.
     *
     * @param components The components to wrap.
     */
    CompositeBufferView(@NonNull List<BufferView> components) {
        this.components = new ArrayList<>();
        int length = 0;
        for (BufferView c : components) {
            if (c instanceof CompositeBufferView) {
                for (BufferView b : ((CompositeBufferView) c).components) {
                    this.components.add(b.slice());
                }
            } else {
                this.components.add(c.slice());
            }
            length += c.getLength();
        }
        this.length = length;
    }

    //endregion

    //region BufferView implementation

    @Override
    public int getAllocatedLength() {
        if (this.allocatedLength < 0) {
            this.allocatedLength = this.components.stream().mapToInt(BufferView::getAllocatedLength).sum();
        }

        return this.allocatedLength;
    }

    @Override
    public Reader getBufferViewReader() {
        return new Reader(this.components.stream().map(BufferView::getBufferViewReader).iterator(), getLength());
    }

    @Override
    public InputStream getReader() {
        return new SequenceInputStream(Iterators.asEnumeration(this.components.stream().map(BufferView::getReader).iterator()));
    }

    @Override
    public InputStream getReader(int offset, int length) {
        return slice(offset, length).getReader();
    }

    @Override
    public BufferView slice(int offset, int length) {
        Preconditions.checkArgument(offset >= 0 && length >= 0 && offset + length <= this.length,
                "offset and length must be non-negative and less than %s. Given %s-%s", this.length, offset, length);
        if (offset == 0 && length == this.length) {
            return this;
        }
        ArrayList<BufferView> components = new ArrayList<>(this.components.size());
        int index = 0;
        int currentOffset = 0;
        while (length > 0) {
            BufferView c = this.components.get(index);
            int lastComponentOffset = currentOffset + c.getLength();
            if (offset < lastComponentOffset) {
                int sliceStart = Math.max(0, offset - currentOffset);
                int sliceLength = Math.min(length, c.getLength() - sliceStart);
                components.add(c.slice(sliceStart, sliceLength));
                length -= sliceLength;
            }

            index++;
            currentOffset += c.getLength();
        }

        return BufferView.wrap(components);
    }

    @Override
    public byte[] getCopy() {
        byte[] result = new byte[getLength()];
        int offset = 0;
        for (BufferView c : this.components) {
            int copiedBytes = c.copyTo(ByteBuffer.wrap(result, offset, c.getLength()));
            assert copiedBytes == c.getLength();
            offset += copiedBytes;
        }
        return result;
    }

    @Override
    public void copyTo(OutputStream target) throws IOException {
        for (BufferView c : this.components) {
            c.copyTo(target);
        }
    }

    @Override
    public int copyTo(ByteBuffer target) {
        int bytesCopied = 0;
        for (BufferView c : this.components) {
            bytesCopied += c.copyTo(target);
        }

        return bytesCopied;
    }

    @Override
    public <ExceptionT extends Exception> void collect(Collector<ExceptionT> bufferCollector) throws ExceptionT {
        for (BufferView bv : this.components) {
            bv.collect(bufferCollector);
        }
    }

    @Override
    public Iterator<ByteBuffer> iterateBuffers() {
        return Iterators.concat(Iterators.transform(this.components.iterator(), BufferView::iterateBuffers));
    }

    @Override
    public void retain() {
        this.components.forEach(BufferView::retain);
    }

    @Override
    public void release() {
        this.components.forEach(BufferView::release);
    }

    List<BufferView> getComponents() {
        return Collections.unmodifiableList(this.components);
    }

    //endregion

    //region Reader

    private static final class Reader extends AbstractReader implements BufferView.Reader {
        private final Iterator<BufferView.Reader> readers;
        private BufferView.Reader current;
        private int available;

        Reader(Iterator<BufferView.Reader> readers, int available) {
            this.readers = readers;
            this.available = available;
        }

        @Override
        public int available() {
            return this.available;
        }

        @Override
        public int readBytes(ByteBuffer byteBuffer) {
            BufferView.Reader current = getCurrent();
            if (current != null) {
                int len = current.readBytes(byteBuffer);
                this.available -= len;
                assert this.available >= 0;
                return len;
            }

            return 0;
        }

        @Override
        public byte readByte() {
            BufferView.Reader current = getCurrent();
            if (current == null) {
                throw new OutOfBoundsException();
            }

            byte result = current.readByte();
            this.available--;
            assert this.available >= 0;
            return result;
        }

        @Override
        public int readInt() {
            BufferView.Reader current = getCurrent();
            if (current != null && current.available() >= Integer.BYTES) {
                this.available -= Integer.BYTES;
                return current.readInt();
            }

            return super.readInt();
        }

        @Override
        public long readLong() {
            BufferView.Reader current = getCurrent();
            if (current != null && current.available() >= Long.BYTES) {
                this.available -= Long.BYTES;
                return current.readLong();
            }

            return super.readLong();
        }

        @Override
        public BufferView readSlice(final int length) {
            if (length > available()) {
                throw new OutOfBoundsException();
            }

            if (length == 0) {
                return BufferView.empty();
            }

            ArrayList<BufferView> components = new ArrayList<>();
            int remaining = length;
            while (remaining > 0) {
                BufferView.Reader current = getCurrent();
                assert current != null;
                int currentLength = Math.min(current.available(), remaining);
                components.add(current.readSlice(currentLength));
                this.available -= currentLength;
                remaining -= currentLength;
            }

            assert !components.isEmpty();
            assert this.available >= 0;
            return new CompositeBufferView(components, length);
        }

        private BufferView.Reader getCurrent() {
            if (this.current == null || this.current.available() == 0) {
                this.current = this.readers.hasNext() ? this.readers.next() : null;
            }

            return this.current;
        }
    }

    //endregion
}
