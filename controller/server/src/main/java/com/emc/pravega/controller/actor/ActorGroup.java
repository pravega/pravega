/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.emc.pravega.controller.actor;

import com.emc.pravega.stream.EventStreamReader;
import com.emc.pravega.stream.ReaderConfig;
import com.emc.pravega.stream.ReaderGroupConfig;
import com.emc.pravega.stream.impl.ByteArraySerializer;
import com.google.common.util.concurrent.AbstractService;
import lombok.Getter;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executor;

public final class ActorGroup extends AbstractService {

    private final ActorSystem actorSystem;
    @Getter
    private final Props props;
    @Getter
    private final List<Actor> actors;
    @Getter
    private final ActorGroupRef ref;

    ActorGroup(final ActorSystem actorSystem, final Executor executor, final Props props) throws IllegalAccessException, InvocationTargetException, InstantiationException {
        this.actorSystem = actorSystem;
        this.props = props;
        this.actors = new ArrayList<>();
        this.ref = new ActorGroupRef(actorSystem, props.getConfig().getScope(), props.getConfig().getStreamName());

        actorSystem.streamManager
                .createReaderGroup(props.getConfig().getReaderGroupName(),
                        new ReaderGroupConfig(),
                        Collections.singletonList(props.getConfig().getScope()));

        for (int i = 0; i < props.getConfig().getActorCount(); i++) {
            String readerId = UUID.randomUUID().toString();
            EventStreamReader<byte[]> reader =
                    actorSystem.clientFactory.createReader(readerId,
                            props.getConfig().getReaderGroupName(),
                            new ByteArraySerializer(),
                            new ReaderConfig());

            // create a new actor, and add it to the list
            Actor actor = (Actor) props.getConstructor().newInstance(props.getArgs());
            actor.setReader(reader);
            actor.setProps(props);
            actor.setReaderId(readerId);
            actor.addListener(new ActorFailureListener(actors, i, executor, props), executor);
            actors.add(actor);
        }
    }

    @Override
    final protected void doStart() {
        // If an exception is thrown while starting some actor, it will be processed by the
        actors.stream().forEach(actor -> actor.startAsync());
        notifyStarted();
    }

    @Override
    final protected void doStop() {
        // TODO: what happens on error while stopping some actor? notifyFailed?
        actors.stream().forEach(actor -> actor.stopAsync());
        notifyStopped();
    }

    //    boolean scaleOut(int count) {
    //        throw new NotImplementedException();
    //    }
    //
    //    boolean scaleIn(int count) {
    //        throw new NotImplementedException();
    //    }
}
