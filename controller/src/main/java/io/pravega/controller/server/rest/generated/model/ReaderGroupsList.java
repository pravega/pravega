package io.pravega.controller.server.rest.generated.model;

import java.util.Objects;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonCreator;
import io.pravega.controller.server.rest.generated.model.ReaderGroupsListReaderGroups;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import java.util.ArrayList;
import java.util.List;




/**
 * ReaderGroupsList
 */

public class ReaderGroupsList   {
  private List<ReaderGroupsListReaderGroups> readerGroups = new ArrayList<ReaderGroupsListReaderGroups>();

  public ReaderGroupsList readerGroups(List<ReaderGroupsListReaderGroups> readerGroups) {
    this.readerGroups = readerGroups;
    return this;
  }

  public ReaderGroupsList addReaderGroupsItem(ReaderGroupsListReaderGroups readerGroupsItem) {
    this.readerGroups.add(readerGroupsItem);
    return this;
  }

   /**
   * Get readerGroups
   * @return readerGroups
  **/
  @ApiModelProperty(value = "")
  public List<ReaderGroupsListReaderGroups> getReaderGroups() {
    return readerGroups;
  }

  public void setReaderGroups(List<ReaderGroupsListReaderGroups> readerGroups) {
    this.readerGroups = readerGroups;
  }


  @Override
  public boolean equals(java.lang.Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ReaderGroupsList readerGroupsList = (ReaderGroupsList) o;
    return Objects.equals(this.readerGroups, readerGroupsList.readerGroups);
  }

  @Override
  public int hashCode() {
    return Objects.hash(readerGroups);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class ReaderGroupsList {\n");
    
    sb.append("    readerGroups: ").append(toIndentedString(readerGroups)).append("\n");
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

