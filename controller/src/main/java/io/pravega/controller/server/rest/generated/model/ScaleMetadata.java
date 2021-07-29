/*
 * Pravega Controller APIs
 * List of admin REST APIs for the Pravega controller service.
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
import io.swagger.annotations.ApiModelProperty;
import java.util.ArrayList;
import java.util.List;

/**
 * ScaleMetadata
 */

public class ScaleMetadata   {
  @JsonProperty("timestamp")
  private Long timestamp = null;

  @JsonProperty("segmentList")
  private List<Segment> segmentList = null;

  public ScaleMetadata timestamp(Long timestamp) {
    this.timestamp = timestamp;
    return this;
  }

  /**
   * Get timestamp
   * @return timestamp
   **/
  @JsonProperty("timestamp")
  @ApiModelProperty(value = "")
  public Long getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(Long timestamp) {
    this.timestamp = timestamp;
  }

  public ScaleMetadata segmentList(List<Segment> segmentList) {
    this.segmentList = segmentList;
    return this;
  }

  public ScaleMetadata addSegmentListItem(Segment segmentListItem) {
    if (this.segmentList == null) {
      this.segmentList = new ArrayList<Segment>();
    }
    this.segmentList.add(segmentListItem);
    return this;
  }

  /**
   * Get segmentList
   * @return segmentList
   **/
  @JsonProperty("segmentList")
  @ApiModelProperty(value = "")
  public List<Segment> getSegmentList() {
    return segmentList;
  }

  public void setSegmentList(List<Segment> segmentList) {
    this.segmentList = segmentList;
  }


  @Override
  public boolean equals(java.lang.Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ScaleMetadata scaleMetadata = (ScaleMetadata) o;
    return Objects.equals(this.timestamp, scaleMetadata.timestamp) &&
        Objects.equals(this.segmentList, scaleMetadata.segmentList);
  }

  @Override
  public int hashCode() {
    return Objects.hash(timestamp, segmentList);
  }


  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class ScaleMetadata {\n");
    
    sb.append("    timestamp: ").append(toIndentedString(timestamp)).append("\n");
    sb.append("    segmentList: ").append(toIndentedString(segmentList)).append("\n");
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

