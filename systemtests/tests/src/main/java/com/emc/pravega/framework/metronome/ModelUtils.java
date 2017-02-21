/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.framework.metronome;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class ModelUtils {
    private static final Gson GSON = new GsonBuilder().setPrettyPrinting()
            .disableHtmlEscaping().create();

    public static String toString(Object o) {
        return GSON.toJson(o);
    }
}