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
package io.pravega.shared.protocol.netty;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.MessageToByteEncoder;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public abstract class FlushingMessageToByteEncoder<I> extends MessageToByteEncoder<I> {

    private final AtomicBoolean shouldFlush = new AtomicBoolean(false);
    
    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        super.write(ctx, msg, promise);
        if (shouldFlush.compareAndSet(true, false)) {
            ctx.flush();
        }
    }
    
    protected void flushRequired() {
        shouldFlush.set(true);
    }
    
}
