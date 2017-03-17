package com.emc.pravega.controller.server.rest.generated.model;

import java.util.Objects;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;




/**
 * StreamState
 */

public class StreamState   {
  /**
   * Gets or Sets streamState
   */
  public enum StreamStateEnum {
    SEALED("SEALED");

    private String value;

    StreamStateEnum(String value) {
      this.value = value;
    }

    @Override
    public String toString() {
      return String.valueOf(value);
    }
  }

  private StreamStateEnum streamState = null;

  public StreamState streamState(StreamStateEnum streamState) {
    this.streamState = streamState;
    return this;
  }

   /**
   * Get streamState
   * @return streamState
  **/
  @ApiModelProperty(value = "")
  public StreamStateEnum getStreamState() {
    return streamState;
  }

  public void setStreamState(StreamStateEnum streamState) {
    this.streamState = streamState;
  }


  @Override
  public boolean equals(java.lang.Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    StreamState streamState = (StreamState) o;
    return Objects.equals(this.streamState, streamState.streamState);
  }

  @Override
  public int hashCode() {
    return Objects.hash(streamState);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class StreamState {\n");
    
    sb.append("    streamState: ").append(toIndentedString(streamState)).append("\n");
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

