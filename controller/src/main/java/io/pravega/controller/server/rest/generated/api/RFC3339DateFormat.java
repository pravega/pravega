package io.pravega.controller.server.rest.generated.api;

import java.text.FieldPosition;
import java.util.Date;

@SuppressWarnings("deprecation") //See https://github.com/FasterXML/jackson-databind/issues/1786
public class RFC3339DateFormat extends com.fasterxml.jackson.databind.util.ISO8601DateFormat {

    // Same as ISO8601DateFormat but serializing milliseconds.
    @Override
    public StringBuffer format(Date date, StringBuffer toAppendTo, FieldPosition fieldPosition) {
        String value = com.fasterxml.jackson.databind.util.ISO8601Utils.format(date, true);
        toAppendTo.append(value);
        return toAppendTo;
    }

}