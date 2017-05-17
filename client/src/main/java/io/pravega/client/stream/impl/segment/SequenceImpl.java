/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
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
package io.pravega.client.stream.impl.segment;

import io.pravega.client.stream.Sequence;
import lombok.Data;

@Data
public class SequenceImpl implements Sequence {

    private final long highOrder;
    private final long lowOrder;

    private SequenceImpl(long highOrder, long lowOrder) {
        super();
        this.highOrder = highOrder;
        this.lowOrder = lowOrder;
    }
    
    public static Sequence create(long highOrder, long lowOrder) {
        return new SequenceImpl(highOrder, lowOrder);
    }

    @Override
    public SequenceImpl asImpl() {
        return this;
    }
}
