package io.pravega.controller.server.rest.generated.model;

import java.util.Objects;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonCreator;
import io.pravega.controller.server.rest.generated.model.Segment;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import java.util.ArrayList;
import java.util.List;




/**
 * ScaleMetadata
 */

public class ScaleMetadata   {
  private Long timestamp = null;

  private List<Segment> segmentList = new ArrayList<Segment>();

  public ScaleMetadata timestamp(Long timestamp) {
    this.timestamp = timestamp;
    return this;
  }

   /**
   * Get timestamp
   * @return timestamp
  **/
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
    this.segmentList.add(segmentListItem);
    return this;
  }

   /**
   * Get segmentList
   * @return segmentList
  **/
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

