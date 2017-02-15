package com.emc.pravega.controller.server.rest.generated.model;

import java.util.Objects;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;




/**
 * ScalingPolicy
 */

public class ScalingPolicy   {
  /**
   * Gets or Sets type
   */
  public enum TypeEnum {
    FIXED_NUM_SEGMENTS("FIXED_NUM_SEGMENTS"),
    
    BY_RATE_IN_BYTES("BY_RATE_IN_BYTES"),
    
    BY_RATE_IN_EVENTS("BY_RATE_IN_EVENTS");

    private String value;

    TypeEnum(String value) {
      this.value = value;
    }

    @Override
    public String toString() {
      return String.valueOf(value);
    }
  }

  private TypeEnum type = null;

  private Long targetRate = null;

  private Integer scaleFactor = null;

  private Integer minNumSegments = null;

  public ScalingPolicy type(TypeEnum type) {
    this.type = type;
    return this;
  }

   /**
   * Get type
   * @return type
  **/
  @ApiModelProperty(value = "")
  public TypeEnum getType() {
    return type;
  }

  public void setType(TypeEnum type) {
    this.type = type;
  }

  public ScalingPolicy targetRate(Long targetRate) {
    this.targetRate = targetRate;
    return this;
  }

   /**
   * Get targetRate
   * @return targetRate
  **/
  @ApiModelProperty(value = "")
  public Long getTargetRate() {
    return targetRate;
  }

  public void setTargetRate(Long targetRate) {
    this.targetRate = targetRate;
  }

  public ScalingPolicy scaleFactor(Integer scaleFactor) {
    this.scaleFactor = scaleFactor;
    return this;
  }

   /**
   * Get scaleFactor
   * @return scaleFactor
  **/
  @ApiModelProperty(value = "")
  public Integer getScaleFactor() {
    return scaleFactor;
  }

  public void setScaleFactor(Integer scaleFactor) {
    this.scaleFactor = scaleFactor;
  }

  public ScalingPolicy minNumSegments(Integer minNumSegments) {
    this.minNumSegments = minNumSegments;
    return this;
  }

   /**
   * Get minNumSegments
   * @return minNumSegments
  **/
  @ApiModelProperty(value = "")
  public Integer getMinNumSegments() {
    return minNumSegments;
  }

  public void setMinNumSegments(Integer minNumSegments) {
    this.minNumSegments = minNumSegments;
  }


  @Override
  public boolean equals(java.lang.Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ScalingPolicy scalingPolicy = (ScalingPolicy) o;
    return Objects.equals(this.type, scalingPolicy.type) &&
        Objects.equals(this.targetRate, scalingPolicy.targetRate) &&
        Objects.equals(this.scaleFactor, scalingPolicy.scaleFactor) &&
        Objects.equals(this.minNumSegments, scalingPolicy.minNumSegments);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, targetRate, scaleFactor, minNumSegments);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class ScalingPolicy {\n");
    
    sb.append("    type: ").append(toIndentedString(type)).append("\n");
    sb.append("    targetRate: ").append(toIndentedString(targetRate)).append("\n");
    sb.append("    scaleFactor: ").append(toIndentedString(scaleFactor)).append("\n");
    sb.append("    minNumSegments: ").append(toIndentedString(minNumSegments)).append("\n");
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

