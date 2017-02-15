package com.emc.pravega.controller.server.rest.generated.model;

import java.util.Objects;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonCreator;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;




/**
 * RetentionPolicy
 */

public class RetentionPolicy   {
  private Long retentionTimeMillis = null;

  public RetentionPolicy retentionTimeMillis(Long retentionTimeMillis) {
    this.retentionTimeMillis = retentionTimeMillis;
    return this;
  }

   /**
   * Get retentionTimeMillis
   * @return retentionTimeMillis
  **/
  @ApiModelProperty(value = "")
  public Long getRetentionTimeMillis() {
    return retentionTimeMillis;
  }

  public void setRetentionTimeMillis(Long retentionTimeMillis) {
    this.retentionTimeMillis = retentionTimeMillis;
  }


  @Override
  public boolean equals(java.lang.Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    RetentionPolicy retentionPolicy = (RetentionPolicy) o;
    return Objects.equals(this.retentionTimeMillis, retentionPolicy.retentionTimeMillis);
  }

  @Override
  public int hashCode() {
    return Objects.hash(retentionTimeMillis);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class RetentionPolicy {\n");
    
    sb.append("    retentionTimeMillis: ").append(toIndentedString(retentionTimeMillis)).append("\n");
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

