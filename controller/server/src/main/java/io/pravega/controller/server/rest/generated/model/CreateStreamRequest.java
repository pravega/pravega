package io.pravega.controller.server.rest.generated.model;

import java.util.Objects;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonCreator;
import io.pravega.controller.server.rest.generated.model.RetentionConfig;
import io.pravega.controller.server.rest.generated.model.ScalingConfig;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;




/**
 * CreateStreamRequest
 */

public class CreateStreamRequest   {
  private String streamName = null;

  private ScalingConfig scalingPolicy = null;

  private RetentionConfig retentionPolicy = null;

  public CreateStreamRequest streamName(String streamName) {
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

  public CreateStreamRequest scalingPolicy(ScalingConfig scalingPolicy) {
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

  public CreateStreamRequest retentionPolicy(RetentionConfig retentionPolicy) {
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
    CreateStreamRequest createStreamRequest = (CreateStreamRequest) o;
    return Objects.equals(this.streamName, createStreamRequest.streamName) &&
        Objects.equals(this.scalingPolicy, createStreamRequest.scalingPolicy) &&
        Objects.equals(this.retentionPolicy, createStreamRequest.retentionPolicy);
  }

  @Override
  public int hashCode() {
    return Objects.hash(streamName, scalingPolicy, retentionPolicy);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class CreateStreamRequest {\n");
    
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

