/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.emc.pravega.aspectj.metrics;

import com.emc.pravega.metrics.Counter;
import com.emc.pravega.metrics.OpStatsLogger;
import com.emc.pravega.metrics.StatsLogger;
import com.emc.pravega.metrics.annotate.Countered;
import com.emc.pravega.metrics.annotate.Metrics;

import java.lang.Object;
import java.lang.reflect.Method;
import java.lang.reflect.Field;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.aspectj.lang.reflect.MethodSignature;

public aspect MetricsAspect
              pertypewithin (@Metrics *)
{

    declare parents : (@Metrics *) extends Profiled;

    private final Map<String, Counter> Profiled.counters = new ConcurrentHashMap<String, Counter>();
    private final Map<String, OpStatsLogger> Profiled.opStatsLoggers = new ConcurrentHashMap<String, OpStatsLogger>();

    pointcut profiled(Profiled object) : execution(Profiled+.new(..)) && this(object);

    // Use after, so statsLogger fields has values already. dec
    // currently support one method with one counter.
    after(Profiled object) : profiled(object)  {
        for (Method method : object.getClass().getDeclaredMethods()) {
            if (method.isAnnotationPresent(Countered.class)) {
                // TODO: 1. need add try catch for error usage, e.g. no statsLogger in class
                try {
                Field field = object.getClass().getDeclaredField("statsLogger");
                StatsLogger logger = (StatsLogger)field.get(object);
                System.out.println("statsLogger, name is:" + logger);
                String counterName = method.getAnnotation(Countered.class).name();
                Counter counter = newCounterFromAnnotation(logger, counterName);
                object.counters.put(method.getName() + counterName, counter);
                System.out.println("get a @Countered annotation, name is:" + counterName);
                } catch (NoSuchFieldException e) {
                    System.out.println("ERROR: No such statsLogger " + e);
                } catch (IllegalAccessException e) {
                    System.out.println("ERROR: IllegalAccessException for field statsLogger " + e);
                }
            }
        }
    }

    private Counter newCounterFromAnnotation(StatsLogger statsLogger, String counterName) {
        System.out.println("add a counter: " + counterName);
        return statsLogger.getCounter(counterName);
    }

    private void Profiled.counterAdd(long value) {
        System.out.println("enter Profiled.counterAdd(value), value : " + value);
    }

    // when call counterAdd(value); should replace it with counter added by this method, and the aspect
    pointcut countered(Profiled object) : execution(@Countered !static * (@Metrics Profiled+).counterAdd(long)) && this(object);

    Object around(Profiled object) : countered(object) {
        MethodSignature signature = (MethodSignature)(thisJoinPoint.getSignature());
        Method method = signature.getMethod();
        String counterName = method.getAnnotation(Countered.class).name();
        Counter counter = object.counters.get(method.getName() + counterName);

        System.out.println("In method to be countered, methodName: " + method.getName() + " counterName" + counterName);
        System.out.println("joinpoint args: " + thisJoinPoint.getArgs());

        Object[] arguments = thisJoinPoint.getArgs();
        for (Object arg : arguments) {
            System.out.println("method args is : " + arg);
        }
        long value = (long)(thisJoinPoint.getArgs())[0];
        counter.add(value);
        return null;
    }
}
