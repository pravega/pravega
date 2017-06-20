package io.pravega.controller.server.rest.generated.model;

import java.util.Objects;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonCreator;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;




/**
 * Segment
 */

public class Segment   {
  private Integer number = null;

  private Long startTime = null;

  private Integer keyStart = null;

  private Integer keyEnd = null;

  public Segment number(Integer number) {
    this.number = number;
    return this;
  }

   /**
   * Get number
   * @return number
  **/
  @ApiModelProperty(value = "")
  public Integer getNumber() {
    return number;
  }

  public void setNumber(Integer number) {
    this.number = number;
  }

  public Segment startTime(Long startTime) {
    this.startTime = startTime;
    return this;
  }

   /**
   * Get startTime
   * @return startTime
  **/
  @ApiModelProperty(value = "")
  public Long getStartTime() {
    return startTime;
  }

  public void setStartTime(Long startTime) {
    this.startTime = startTime;
  }

  public Segment keyStart(Integer keyStart) {
    this.keyStart = keyStart;
    return this;
  }

   /**
   * Get keyStart
   * @return keyStart
  **/
  @ApiModelProperty(value = "")
  public Integer getKeyStart() {
    return keyStart;
  }

  public void setKeyStart(Integer keyStart) {
    this.keyStart = keyStart;
  }

  public Segment keyEnd(Integer keyEnd) {
    this.keyEnd = keyEnd;
    return this;
  }

   /**
   * Get keyEnd
   * @return keyEnd
  **/
  @ApiModelProperty(value = "")
  public Integer getKeyEnd() {
    return keyEnd;
  }

  public void setKeyEnd(Integer keyEnd) {
    this.keyEnd = keyEnd;
  }


  @Override
  public boolean equals(java.lang.Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Segment segment = (Segment) o;
    return Objects.equals(this.number, segment.number) &&
        Objects.equals(this.startTime, segment.startTime) &&
        Objects.equals(this.keyStart, segment.keyStart) &&
        Objects.equals(this.keyEnd, segment.keyEnd);
  }

  @Override
  public int hashCode() {
    return Objects.hash(number, startTime, keyStart, keyEnd);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class Segment {\n");
    
    sb.append("    number: ").append(toIndentedString(number)).append("\n");
    sb.append("    startTime: ").append(toIndentedString(startTime)).append("\n");
    sb.append("    keyStart: ").append(toIndentedString(keyStart)).append("\n");
    sb.append("    keyEnd: ").append(toIndentedString(keyEnd)).append("\n");
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

