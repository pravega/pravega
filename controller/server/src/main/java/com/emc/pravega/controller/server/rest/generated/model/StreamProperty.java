package com.emc.pravega.controller.server.rest.generated.model;

import java.util.Objects;
import com.emc.pravega.controller.server.rest.generated.model.RetentionPolicy;
import com.emc.pravega.controller.server.rest.generated.model.ScalingPolicy;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonCreator;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;




/**
 * StreamProperty
 */
@javax.annotation.Generated(value = "class io.swagger.codegen.languages.JavaJerseyServerCodegen", date = "2017-02-15T05:33:34.934-08:00")
public class StreamProperty   {
  private String scope = null;

  private String name = null;

  private ScalingPolicy scalingPolicy = null;

  private RetentionPolicy retentionPolicy = null;

  public StreamProperty scope(String scope) {
    this.scope = scope;
    return this;
  }

   /**
   * Get scope
   * @return scope
  **/
  @ApiModelProperty(value = "")
  public String getScope() {
    return scope;
  }

  public void setScope(String scope) {
    this.scope = scope;
  }

  public StreamProperty name(String name) {
    this.name = name;
    return this;
  }

   /**
   * Get name
   * @return name
  **/
  @ApiModelProperty(value = "")
  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public StreamProperty scalingPolicy(ScalingPolicy scalingPolicy) {
    this.scalingPolicy = scalingPolicy;
    return this;
  }

   /**
   * Get scalingPolicy
   * @return scalingPolicy
  **/
  @ApiModelProperty(value = "")
  public ScalingPolicy getScalingPolicy() {
    return scalingPolicy;
  }

  public void setScalingPolicy(ScalingPolicy scalingPolicy) {
    this.scalingPolicy = scalingPolicy;
  }

  public StreamProperty retentionPolicy(RetentionPolicy retentionPolicy) {
    this.retentionPolicy = retentionPolicy;
    return this;
  }

   /**
   * Get retentionPolicy
   * @return retentionPolicy
  **/
  @ApiModelProperty(value = "")
  public RetentionPolicy getRetentionPolicy() {
    return retentionPolicy;
  }

  public void setRetentionPolicy(RetentionPolicy retentionPolicy) {
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
    return Objects.equals(this.scope, streamProperty.scope) &&
        Objects.equals(this.name, streamProperty.name) &&
        Objects.equals(this.scalingPolicy, streamProperty.scalingPolicy) &&
        Objects.equals(this.retentionPolicy, streamProperty.retentionPolicy);
  }

  @Override
  public int hashCode() {
    return Objects.hash(scope, name, scalingPolicy, retentionPolicy);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class StreamProperty {\n");
    
    sb.append("    scope: ").append(toIndentedString(scope)).append("\n");
    sb.append("    name: ").append(toIndentedString(name)).append("\n");
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

