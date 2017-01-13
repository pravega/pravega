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

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executor;

public final class ActorGroup extends AbstractService {

    private final ActorSystem actorSystem;
    @Getter
    private ActorGroupConfig config;
    @Getter
    private final List<Actor> actors;
    @Getter
    private final ActorGroupRef ref;

    ActorGroup(final ActorSystem actorSystem, final ActorGroupConfig config, Executor executor, Class clazz, Constructor constructor, Object... args) throws IllegalAccessException, InvocationTargetException, InstantiationException {
        this.actorSystem = actorSystem;
        this.config = config;
        this.actors = new ArrayList<>();
        this.ref = new ActorGroupRef(actorSystem, config.getScope(), config.getStreamName());

        actorSystem.streamManager.createReaderGroup(config.getReaderGroupName(), new ReaderGroupConfig(), Collections.singletonList(config.getScope()));
        for (int i = 0; i < config.getActorCount(); i++) {
            UUID uuid = UUID.randomUUID();
            EventStreamReader<byte[]> reader =
                    actorSystem.clientFactory.createReader(uuid.toString(), config.getReaderGroupName(), new ByteArraySerializer(), new ReaderConfig());

            // create a new actor, and add it to the list
            Actor actor = (Actor) constructor.newInstance(args);
            actor.setReader(reader);
            actor.addListener(new ActorFailureListener(), executor);
            actors.add(actor);
        }
    }

    ActorGroupRef getActorGroupRef() {
        return this.ref;
    }

    @Override
    final protected void doStart() {
        // TODO: what happens on error while starting some actor? notifyFailed?
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
