package io.pravega.controller.server.rest.generated.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.annotations.ApiModelProperty;

import java.util.Objects;

public class EventProperty {

    @JsonProperty("eventName")
    private String eventName = null;

    public EventProperty eventName(String eventName) {
        this.eventName = eventName;
        return this;
    }

    /**
     * Get eventName
     * @return eventName
     **/
    @JsonProperty("eventName")
    @ApiModelProperty(value = "")
    public String getEventName() {
        return eventName;
    }

    public void setEventName(String eventName) {
        this.eventName = eventName;
    }


    @Override
    public boolean equals(java.lang.Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        EventProperty eventProperty = (EventProperty) o;
        return Objects.equals(this.eventName, eventProperty.eventName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(eventName);
    }


    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("class EventProperty {\n");

        sb.append("    eventName: ").append(toIndentedString(eventName)).append("\n");
        sb.append("}");
        return sb.toString();
    }

    /**
     * Convert the given object to string with each line indented by 4 spaces
     * (except the first line).
     */
    private String toIndentedString(java.lang.Object o) {
        if (o == null) {
            return "null";
        }
        return o.toString().replace("\n", "\n    ");
    }
}
