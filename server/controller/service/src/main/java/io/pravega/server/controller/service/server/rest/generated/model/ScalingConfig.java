package io.pravega.server.controller.service.server.rest.generated.model;

import java.util.Objects;

import io.swagger.annotations.ApiModelProperty;




/**
 * ScalingConfig
 */

public class ScalingConfig   {
  /**
   * Gets or Sets type
   */
  public enum TypeEnum {
    FIXED_NUM_SEGMENTS("FIXED_NUM_SEGMENTS"),
    
    BY_RATE_IN_KBYTES_PER_SEC("BY_RATE_IN_KBYTES_PER_SEC"),
    
    BY_RATE_IN_EVENTS_PER_SEC("BY_RATE_IN_EVENTS_PER_SEC");

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

  private Integer targetRate = null;

  private Integer scaleFactor = null;

  private Integer minSegments = null;

  public ScalingConfig type(TypeEnum type) {
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

  public ScalingConfig targetRate(Integer targetRate) {
    this.targetRate = targetRate;
    return this;
  }

   /**
   * Get targetRate
   * @return targetRate
  **/
  @ApiModelProperty(value = "")
  public Integer getTargetRate() {
    return targetRate;
  }

  public void setTargetRate(Integer targetRate) {
    this.targetRate = targetRate;
  }

  public ScalingConfig scaleFactor(Integer scaleFactor) {
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

  public ScalingConfig minSegments(Integer minSegments) {
    this.minSegments = minSegments;
    return this;
  }

   /**
   * Get minSegments
   * @return minSegments
  **/
  @ApiModelProperty(value = "")
  public Integer getMinSegments() {
    return minSegments;
  }

  public void setMinSegments(Integer minSegments) {
    this.minSegments = minSegments;
  }


  @Override
  public boolean equals(java.lang.Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ScalingConfig scalingConfig = (ScalingConfig) o;
    return Objects.equals(this.type, scalingConfig.type) &&
        Objects.equals(this.targetRate, scalingConfig.targetRate) &&
        Objects.equals(this.scaleFactor, scalingConfig.scaleFactor) &&
        Objects.equals(this.minSegments, scalingConfig.minSegments);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, targetRate, scaleFactor, minSegments);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class ScalingConfig {\n");
    
    sb.append("    type: ").append(toIndentedString(type)).append("\n");
    sb.append("    targetRate: ").append(toIndentedString(targetRate)).append("\n");
    sb.append("    scaleFactor: ").append(toIndentedString(scaleFactor)).append("\n");
    sb.append("    minSegments: ").append(toIndentedString(minSegments)).append("\n");
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

