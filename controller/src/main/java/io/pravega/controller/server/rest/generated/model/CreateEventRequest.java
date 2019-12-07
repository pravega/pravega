package io.pravega.controller.server.rest.generated.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.annotations.ApiModelProperty;

import java.util.Objects;

public class CreateEventRequest {
    @JsonProperty("eventName")
    private String eventName = null;

    public CreateEventRequest eventName(String routingKey, String scopeName, String streamName) {
        this.eventName =  this.routingKey + "_" + this.scopeName + "_" + this.streamName + "_" + String.valueOf(System.currentTimeMillis());
        return this;
    }

    /**
     * Get eventName
     * @return eventName
     **/
    @JsonProperty("eventName")
    @ApiModelProperty(value = "")
    public String getEventName() {
        return this.eventName;
    }

    public void setEventName(String eventName) {
        this.eventName = eventName;
    }

    @JsonProperty("routingKey")
    private String routingKey = null;

    /**
     * Get routingKey
     * @return routingKey
     **/
    @JsonProperty("routingKey")
    @ApiModelProperty(value = "")
    public String getRoutingKey() {
        return routingKey;
    }

    public void setRoutingKey(String routingKey) {
        this.routingKey = routingKey;
    }

    @JsonProperty("scopeName")
    private String scopeName = null;

    /**
     * Get scopeName
     * @return scopeName
     **/
    @JsonProperty("scopeName")
    @ApiModelProperty(value = "")
    public String getScopeName() {
        return scopeName;
    }

    public void setScopeName(String scopeName) {
        this.scopeName = scopeName;
    }


    @JsonProperty("streamName")
    private String streamName = null;

    /**
     * Get streamName
     * @return streamName
     **/
    @JsonProperty("streamName")
    @ApiModelProperty(value = "")
    public String getStreamName() {
        return streamName;
    }

    public void setStreamName(String streamName) {
        this.streamName = streamName;
    }

    @JsonProperty("message")
    private String message = null;

    /**
     * Get message
     * @return message
     **/
    @JsonProperty("message")
    @ApiModelProperty(value = "")
    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    @Override
    public boolean equals(java.lang.Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CreateEventRequest createEventRequest = (CreateEventRequest) o;
        return Objects.equals(this.eventName, createEventRequest.eventName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(eventName);
    }


    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("class CreateEventRequest {\n");

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
