package io.pravega.controller.server.rest.generated.model;

import java.util.Objects;

import io.swagger.annotations.ApiModelProperty;
import java.util.ArrayList;
import java.util.List;




/**
 * StreamsList
 */

public class StreamsList   {
  private List<StreamProperty> streams = new ArrayList<StreamProperty>();

  public StreamsList streams(List<StreamProperty> streams) {
    this.streams = streams;
    return this;
  }

  public StreamsList addStreamsItem(StreamProperty streamsItem) {
    this.streams.add(streamsItem);
    return this;
  }

   /**
   * Get streams
   * @return streams
  **/
  @ApiModelProperty(value = "")
  public List<StreamProperty> getStreams() {
    return streams;
  }

  public void setStreams(List<StreamProperty> streams) {
    this.streams = streams;
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
    return Objects.equals(this.streams, streamsList.streams);
  }

  @Override
  public int hashCode() {
    return Objects.hash(streams);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class StreamsList {\n");
    
    sb.append("    streams: ").append(toIndentedString(streams)).append("\n");
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

