package io.pravega.controller.server.rest.generated.model;

import java.util.Objects;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonCreator;
import io.pravega.controller.server.rest.generated.model.ScaleMetadata;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import java.util.ArrayList;
import java.util.List;




/**
 * ScalingEventList
 */

public class ScalingEventList   {
  private List<ScaleMetadata> scalingEvents = new ArrayList<ScaleMetadata>();

  public ScalingEventList scalingEvents(List<ScaleMetadata> scalingEvents) {
    this.scalingEvents = scalingEvents;
    return this;
  }

  public ScalingEventList addScalingEventsItem(ScaleMetadata scalingEventsItem) {
    this.scalingEvents.add(scalingEventsItem);
    return this;
  }

   /**
   * Get scalingEvents
   * @return scalingEvents
  **/
  @ApiModelProperty(value = "")
  public List<ScaleMetadata> getScalingEvents() {
    return scalingEvents;
  }

  public void setScalingEvents(List<ScaleMetadata> scalingEvents) {
    this.scalingEvents = scalingEvents;
  }


  @Override
  public boolean equals(java.lang.Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ScalingEventList scalingEventList = (ScalingEventList) o;
    return Objects.equals(this.scalingEvents, scalingEventList.scalingEvents);
  }

  @Override
  public int hashCode() {
    return Objects.hash(scalingEvents);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class ScalingEventList {\n");
    
    sb.append("    scalingEvents: ").append(toIndentedString(scalingEvents)).append("\n");
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

