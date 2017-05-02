package io.pravega.server.controller.service.server.rest.generated.model;

import java.util.Objects;

import io.swagger.annotations.ApiModelProperty;




/**
 * UpdateStreamRequest
 */

public class UpdateStreamRequest   {
  private ScalingConfig scalingPolicy = null;

  private RetentionConfig retentionPolicy = null;

  public UpdateStreamRequest scalingPolicy(ScalingConfig scalingPolicy) {
    this.scalingPolicy = scalingPolicy;
    return this;
  }

   /**
   * Get scalingPolicy
   * @return scalingPolicy
  **/
  @ApiModelProperty(value = "")
  public ScalingConfig getScalingPolicy() {
    return scalingPolicy;
  }

  public void setScalingPolicy(ScalingConfig scalingPolicy) {
    this.scalingPolicy = scalingPolicy;
  }

  public UpdateStreamRequest retentionPolicy(RetentionConfig retentionPolicy) {
    this.retentionPolicy = retentionPolicy;
    return this;
  }

   /**
   * Get retentionPolicy
   * @return retentionPolicy
  **/
  @ApiModelProperty(value = "")
  public RetentionConfig getRetentionPolicy() {
    return retentionPolicy;
  }

  public void setRetentionPolicy(RetentionConfig retentionPolicy) {
    this.retentionPolicy = retentionPolicy;
  }


  @Override
  public boolean equals(java.lang.Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    UpdateStreamRequest updateStreamRequest = (UpdateStreamRequest) o;
    return Objects.equals(this.scalingPolicy, updateStreamRequest.scalingPolicy) &&
        Objects.equals(this.retentionPolicy, updateStreamRequest.retentionPolicy);
  }

  @Override
  public int hashCode() {
    return Objects.hash(scalingPolicy, retentionPolicy);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class UpdateStreamRequest {\n");
    
    sb.append("    scalingPolicy: ").append(toIndentedString(scalingPolicy)).append("\n");
    sb.append("    retentionPolicy: ").append(toIndentedString(retentionPolicy)).append("\n");
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

