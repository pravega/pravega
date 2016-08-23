package com.emc.pravega.cluster.zkutils.jsonhelper;

import java.util.Map;

/**
 * Created by kandha on 8/23/16.
 */
public final class JSONHelper {
    public static String jsonEncode(Object obj ) {
        final String retVal ="";

        if (obj instanceof Map) {
            retVal.concat("{");
            ((Map)obj).forEach((k,v) -> {
                retVal.concat(jsonEncode(k)+ ":" + jsonEncode(v));
            });
            retVal.concat("}");
        } else if (obj instanceof String) {
            retVal.concat("\"" + obj + "\"");
        } else {
            retVal.concat(obj.toString());
        }

        return retVal;
    }
}
