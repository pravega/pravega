package com.emc.pravega.controller.server.rest.generated.model;

import java.util.Objects;
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
  private List<String> scopeNames = new ArrayList<String>();

  public ScopesList scopeNames(List<String> scopeNames) {
    this.scopeNames = scopeNames;
    return this;
  }

  public ScopesList addScopeNamesItem(String scopeNamesItem) {
    this.scopeNames.add(scopeNamesItem);
    return this;
  }

   /**
   * Get scopeNames
   * @return scopeNames
  **/
  @ApiModelProperty(value = "")
  public List<String> getScopeNames() {
    return scopeNames;
  }

  public void setScopeNames(List<String> scopeNames) {
    this.scopeNames = scopeNames;
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
    return Objects.equals(this.scopeNames, scopesList.scopeNames);
  }

  @Override
  public int hashCode() {
    return Objects.hash(scopeNames);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class ScopesList {\n");
    
    sb.append("    scopeNames: ").append(toIndentedString(scopeNames)).append("\n");
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

