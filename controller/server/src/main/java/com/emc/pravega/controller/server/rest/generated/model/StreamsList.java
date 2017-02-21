package com.emc.pravega.controller.server.rest.generated.model;

import java.util.Objects;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonCreator;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import java.util.ArrayList;
import java.util.List;




/**
 * StreamsList
 */

public class StreamsList   {
  private List<String> streamNames = new ArrayList<String>();

  public StreamsList streamNames(List<String> streamNames) {
    this.streamNames = streamNames;
    return this;
  }

  public StreamsList addStreamNamesItem(String streamNamesItem) {
    this.streamNames.add(streamNamesItem);
    return this;
  }

   /**
   * Get streamNames
   * @return streamNames
  **/
  @ApiModelProperty(value = "")
  public List<String> getStreamNames() {
    return streamNames;
  }

  public void setStreamNames(List<String> streamNames) {
    this.streamNames = streamNames;
  }


  @Override
  public boolean equals(java.lang.Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    StreamsList streamsList = (StreamsList) o;
    return Objects.equals(this.streamNames, streamsList.streamNames);
  }

  @Override
  public int hashCode() {
    return Objects.hash(streamNames);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class StreamsList {\n");
    
    sb.append("    streamNames: ").append(toIndentedString(streamNames)).append("\n");
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

