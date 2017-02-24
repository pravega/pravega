package com.emc.pravega.controller.server.rest.generated.model;

import java.util.Objects;
import com.emc.pravega.controller.server.rest.generated.model.ScopeProperty;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonCreator;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import java.util.ArrayList;
import java.util.List;




/**
 * ScopesList
 */

public class ScopesList   {
  private List<ScopeProperty> scopes = new ArrayList<ScopeProperty>();

  public ScopesList scopes(List<ScopeProperty> scopes) {
    this.scopes = scopes;
    return this;
  }

  public ScopesList addScopesItem(ScopeProperty scopesItem) {
    this.scopes.add(scopesItem);
    return this;
  }

   /**
   * Get scopes
   * @return scopes
  **/
  @ApiModelProperty(value = "")
  public List<ScopeProperty> getScopes() {
    return scopes;
  }

  public void setScopes(List<ScopeProperty> scopes) {
    this.scopes = scopes;
  }


  @Override
  public boolean equals(java.lang.Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ScopesList scopesList = (ScopesList) o;
    return Objects.equals(this.scopes, scopesList.scopes);
  }

  @Override
  public int hashCode() {
    return Objects.hash(scopes);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class ScopesList {\n");
    
    sb.append("    scopes: ").append(toIndentedString(scopes)).append("\n");
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

