package com.emc.pravega.controller.actor.impl;

import com.google.common.util.concurrent.Service;
import lombok.extern.log4j.Log4j;

import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.concurrent.Executor;

@Log4j
public class ActorFailureListener extends Service.Listener {

    private final List<Actor> actors;
    private final int actorIndex;
    private final Executor executor;

    public ActorFailureListener(List<Actor> actors, int actorIndex, Executor executor) {
        this.actors = actors;
        this.actorIndex = actorIndex;
        this.executor = executor;
    }

    public void failed(Service.State from, Throwable failure) {
        Actor failedActor = actors.get(actorIndex);
        Props props = failedActor.getProps();
        String readerId = failedActor.getReaderId();
        log.warn("Actor " + failedActor + " failed with exception from state " + from, failure);

        // Default policy: if the actor failed while processing messages, i.e., from running state, then restart it.
        if (from == Service.State.RUNNING) {
            // create a new actor, and add it to the list
            Actor actor = null;

            try {
                actor = (Actor) props.getConstructor().newInstance(props.getArgs());
            } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
                log.error("Error reviving actor " + failedActor, e);
            }

            actor.setReader(failedActor.getReader());
            actor.setProps(props);
            actor.setReaderId(readerId);
            actor.addListener(this, executor);
            actors.add(actorIndex, actor);
            actor.startAsync();
        }
    }
}
