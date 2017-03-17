package com.emc.pravega.controller.server.rest.generated.model;

import java.util.Objects;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonCreator;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;




/**
 * UpdateStreamStateRequest
 */

public class UpdateStreamStateRequest   {
  private String streamState = null;

  public UpdateStreamStateRequest streamState(String streamState) {
    this.streamState = streamState;
    return this;
  }

   /**
   * Get streamState
   * @return streamState
  **/
  @ApiModelProperty(value = "")
  public String getStreamState() {
    return streamState;
  }

  public void setStreamState(String streamState) {
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
    UpdateStreamStateRequest updateStreamStateRequest = (UpdateStreamStateRequest) o;
    return Objects.equals(this.streamState, updateStreamStateRequest.streamState);
  }

  @Override
  public int hashCode() {
    return Objects.hash(streamState);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class UpdateStreamStateRequest {\n");
    
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

