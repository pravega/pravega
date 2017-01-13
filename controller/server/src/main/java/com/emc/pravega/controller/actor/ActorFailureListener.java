package com.emc.pravega.controller.actor;

import com.google.common.util.concurrent.Service;
import lombok.extern.log4j.Log4j;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.concurrent.Executor;

@Log4j
public class ActorFailureListener extends Service.Listener {

    private final List<Actor> actors;
    private final int actorIndex;
    private final Constructor constructor;
    private final Object[] args;
    private final Executor executor;

    public ActorFailureListener(List<Actor> actors, int actorIndex, Executor executor, Constructor constructor, Object... args) {
        this.actors = actors;
        this.actorIndex = actorIndex;
        this.executor = executor;
        this.constructor = constructor;
        this.args = args;
    }

    public void failed(Service.State from, Throwable failure) {
        log.warn("Actor " + actors.get(actorIndex) + " failed with exception. ", failure);
        // create a new actor, and add it to the list
        Actor actor = null;

        try {
            actor = (Actor) constructor.newInstance(args);
        } catch (InstantiationException e) {
            log.error("Error reviving actor " + actor, e);
        } catch (IllegalAccessException e) {
            log.error("Error reviving actor " + actor, e);
        } catch (InvocationTargetException e) {
            log.error("Error reviving actor " + actor, e);
        }

        actor.setReader(actors.get(actorIndex).getReader());
        actor.addListener(this, executor);
        actors.add(actorIndex, actor);
    }
}
