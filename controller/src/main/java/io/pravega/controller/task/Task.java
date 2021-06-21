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
package io.pravega.controller.task;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Method Annotation for marking methods as batch functions
 *
 * The purpose of Task annotation is two-fold
 * 1. It enables task naming and versioning. This allows task's java method names to change across builds/upgrades,
 *    and multiple versions of a task can be present in the build so as to facilitate non-destructive upgrade.
 * 2. It enables boilerplate code for locking and persisting task state to be introduced by Java Annotation Processing
 *    module, ala lombok annotations -- this feature is yet to be built.
 *
 * These tasks do not need to persist intermediate state during task execution because on failure of the host
 * executing this task, it can be equivalently re-executed on any another host beginning from the first statement.
 * All of the tasks in our system have repeated prefix property, which means, for example,
 * a; b; c; == a; b; a; b; a; b; c;
 * i.e, any prefix of the task can be re-executed any number of times with changing the effect.
 *
 * In future, we shall have a mechanism to insert boilerplate code around the task method body that does locking.
 * It shall mandate that the class containing these tasks extend from TaskBase which has lock() and unlock() methods.
 *
 * The boilerplate code replaces the method body as follows.
 *
 * The method
 * <pre><code>
 * {@literal @}Task(name = "a", version="0.1")
 * CompletableFuture{@literal <T>} method(Object... params) {
 *     body;
 * }
 * </code></pre>
 *
 * is replaced with the following method
 * <pre><code>
 * CompletableFuture{@literal <T>} method (Object... params) {
 *   try {
 *     CompletableFuture{@literal <Boolean>} lock = this.lock();
 *     if (lock.get()) {
 *       TaskData data = new TaskData("a", "0.1", params);
 *       persist(data);
 *
 *       body;
 *
 *       delete(data);
 *     } else {
 *       throw new LockFailedException();
 *     }
 *   } finally {
 *     unlock();
 *   }
 * }
 * </code></pre>
 */

@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Task {

    String name();

    String version();

    String resource();
}
