package io.pravega.controller.server.rest.generated.model;

import java.util.Objects;

import io.swagger.annotations.ApiModelProperty;




/**
 * CreateScopeRequest
 */

public class CreateScopeRequest   {
  private String scopeName = null;

  public CreateScopeRequest scopeName(String scopeName) {
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


  @Override
  public boolean equals(java.lang.Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    CreateScopeRequest createScopeRequest = (CreateScopeRequest) o;
    return Objects.equals(this.scopeName, createScopeRequest.scopeName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(scopeName);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class CreateScopeRequest {\n");
    
    sb.append("    scopeName: ").append(toIndentedString(scopeName)).append("\n");
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

