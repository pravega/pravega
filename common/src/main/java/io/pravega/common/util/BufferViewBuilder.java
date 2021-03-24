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

import java.util.ArrayList;
import java.util.List;
import javax.annotation.concurrent.NotThreadSafe;
import lombok.Getter;
import lombok.NonNull;

/**
 * Builder for {@link BufferView} instances.
 */
@NotThreadSafe
public class BufferViewBuilder {
    /**
     * The length, in bytes, of the accumulated {@link BufferView}.
     */
    @Getter
    private int length;
    private final List<BufferView> components;

    /**
     * Creates a new instance of the {@link BufferViewBuilder} class.
     *
     * @param expectedComponentCount The expected component count.
     */
    BufferViewBuilder(int expectedComponentCount) {
        this.components = new ArrayList<>(expectedComponentCount);
        this.length = 0;
    }

    /**
     * Includes the given {@link BufferView} in the builder.
     *
     * @param bufferView The {@link BufferView} to include. If empty, it will be ignored; if a {@link CompositeBufferView},
     *                   its components will be added; otherwise the buffer itself will be added. No data copies are
     *                   being made as part of this process; the resulting {@link BufferView} after invoking {@link #build()}
     *                   will contain references to the {@link BufferView}s passed via this method and not contain copies
     *                   of their data. Any modifications made to this {@link BufferView} will be reflected in the resulting
     *                   {@link BufferView} and viceversa.
     * @return This instance.
     */
    public BufferViewBuilder add(@NonNull BufferView bufferView) {
        if (bufferView.getLength() == 0) {
            return this;
        }

        if (bufferView instanceof CompositeBufferView) {
            this.components.addAll(((CompositeBufferView) bufferView).getComponents());
        } else {
            this.components.add(bufferView);
        }

        this.length += bufferView.getLength();
        return this;
    }

    /**
     * Generates a {@link BufferView} with the current contents of this {@link BufferViewBuilder} instance.
     *
     * @return A {@link BufferView}. If {@link #getLength()} is 0, returns {@link BufferView#empty()}; otherwise returns
     * either a {@link CompositeBufferView} (if the number of components is greater than 1), or the single
     * {@link BufferView} that it contains (if the number of components equals 1).
     */
    public BufferView build() {
        int cs = this.components.size();
        if (cs == 0) {
            return BufferView.empty();
        } else if (cs == 1) {
            return this.components.get(0).slice();
        } else {
            // Invoke subList as we allow further invocations of add() after this call.
            return new CompositeBufferView(this.components.subList(0, this.components.size()), this.length);
        }
    }
}
