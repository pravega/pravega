package io.pravega.controller.server.rest.generated.model;

import java.util.Objects;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonCreator;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;




/**
 * ReaderGroupsListReaderGroups
 */

public class ReaderGroupsListReaderGroups   {
  private String readerGroupName = null;

  public ReaderGroupsListReaderGroups readerGroupName(String readerGroupName) {
    this.readerGroupName = readerGroupName;
    return this;
  }

   /**
   * Get readerGroupName
   * @return readerGroupName
  **/
  @ApiModelProperty(value = "")
  public String getReaderGroupName() {
    return readerGroupName;
  }

  public void setReaderGroupName(String readerGroupName) {
    this.readerGroupName = readerGroupName;
  }


  @Override
  public boolean equals(java.lang.Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ReaderGroupsListReaderGroups readerGroupsListReaderGroups = (ReaderGroupsListReaderGroups) o;
    return Objects.equals(this.readerGroupName, readerGroupsListReaderGroups.readerGroupName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(readerGroupName);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class ReaderGroupsListReaderGroups {\n");
    
    sb.append("    readerGroupName: ").append(toIndentedString(readerGroupName)).append("\n");
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

