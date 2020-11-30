/*
 * Pravega Controller APIs
 * List of admin REST APIs for the pravega controller service.
 *
 * OpenAPI spec version: 0.0.1
 * 
 *
 * NOTE: This class is auto generated by the swagger code generator program.
 * https://github.com/swagger-api/swagger-codegen.git
 * Do not edit the class manually.
 */


package io.pravega.controller.server.rest.generated.model;

import java.util.Objects;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import javax.validation.constraints.*;

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
    @JsonValue
    public String toString() {
      return String.valueOf(value);
    }

    @JsonCreator
    public static StreamStateEnum fromValue(String text) {
      for (StreamStateEnum b : StreamStateEnum.values()) {
        if (String.valueOf(b.value).equals(text)) {
          return b;
        }
      }
      return null;
    }
  }

  @JsonProperty("streamState")
  private StreamStateEnum streamState = null;

  public StreamState streamState(StreamStateEnum streamState) {
    this.streamState = streamState;
    return this;
  }

  /**
   * Get streamState
   * @return streamState
   **/
  @JsonProperty("streamState")
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

