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
package com.emc.pravega.metrics.aspectj;

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

import javax.el.ELProcessor;


public aspect MetricsAspect
                pertypewithin (@Metrics *)
{

    declare parents : (@Metrics *) extends Profiled;

    private final Map<String, Counter> Profiled.counters = new ConcurrentHashMap<String, Counter>();
    private final Map<String, OpStatsLogger> Profiled.opStatsLoggers = new ConcurrentHashMap<String, OpStatsLogger>();

    pointcut profiled(Profiled object) : execution(Profiled+.new(..)) && this(object);

    // use after, so statsLogger fields has values already.
    after(Profiled object) : profiled(object)  {
        for (Method method : object.getClass().getDeclaredMethods()) {
            if (method.isAnnotationPresent(Countered.class)) {
                // TODO: 1. need add try catch for error usage, e.g. no statsLogger in class
                // 2. handle mutiple counter usecase. java1.8 support this. call getAnnotationsByType to get a array
                StatsLogger statsLogger = object.getClass().getDeclaredField("statsLogger").get(StatsLogger);
                String counterName = method.getAnnotation(Countered.class).name();
                Counter counter = newCounterFromAnnotation(statsLogger, counterName);
                object.counters.put((method.getName() + counterName), counter);
                System.out.println("get a @Countered annotation, name is:" + counterName);
            }
        }
    }

    private Counter newCounterFromAnnotation(StatsLogger statsLogger, String counterName) {
        System.out.println("add a counter: " + counterName);
        return statsLogger.getCounter(counterName);
    }

    // when call counter(name).add(value); should replace it with counter added by this method
    pointcut countered(Profiled object) : execution(@Countered * Profiled+.*(..)) && this(object) && call(counter(*));

    // or how to define a getcounter() method.

    Object around(Profiled object) : timed(object) {
        Timer timer = object.timers.get(thisJoinPoint.getSignature().getName());
        Timer.Context context = timer.time();
        try {
            return proceed(object);
        } finally {
            context.stop();
        }
    }
}