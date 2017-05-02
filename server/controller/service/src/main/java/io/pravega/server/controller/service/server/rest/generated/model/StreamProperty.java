package io.pravega.server.controller.service.server.rest.generated.model;

import java.util.Objects;

import io.swagger.annotations.ApiModelProperty;




/**
 * StreamProperty
 */

public class StreamProperty   {
  private String scopeName = null;

  private String streamName = null;

  private ScalingConfig scalingPolicy = null;

  private RetentionConfig retentionPolicy = null;

  public StreamProperty scopeName(String scopeName) {
    this.scopeName = scopeName;
    return this;
  }

   /**
   * Get scopeName
   * @return scopeName
  **/
  @ApiModelProperty(value = "")
  public String getScopeName() {
    return scopeName;
  }

  public void setScopeName(String scopeName) {
    this.scopeName = scopeName;
  }

  public StreamProperty streamName(String streamName) {
    this.streamName = streamName;
    return this;
  }

   /**
   * Get streamName
   * @return streamName
  **/
  @ApiModelProperty(value = "")
  public String getStreamName() {
    return streamName;
  }

  public void setStreamName(String streamName) {
    this.streamName = streamName;
  }

  public StreamProperty scalingPolicy(ScalingConfig scalingPolicy) {
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

  public StreamProperty retentionPolicy(RetentionConfig retentionPolicy) {
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
    StreamProperty streamProperty = (StreamProperty) o;
    return Objects.equals(this.scopeName, streamProperty.scopeName) &&
        Objects.equals(this.streamName, streamProperty.streamName) &&
        Objects.equals(this.scalingPolicy, streamProperty.scalingPolicy) &&
        Objects.equals(this.retentionPolicy, streamProperty.retentionPolicy);
  }

  @Override
  public int hashCode() {
    return Objects.hash(scopeName, streamName, scalingPolicy, retentionPolicy);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class StreamProperty {\n");
    
    sb.append("    scopeName: ").append(toIndentedString(scopeName)).append("\n");
    sb.append("    streamName: ").append(toIndentedString(streamName)).append("\n");
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

