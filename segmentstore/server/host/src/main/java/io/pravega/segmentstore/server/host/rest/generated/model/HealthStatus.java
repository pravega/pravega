/*
 * Pravega Controller APIs
 * List of admin REST APIs for the pravega controller service.
 *
 * OpenAPI spec version: 0.0.1
 * 
 *
 * NOTE: This class is auto generated by the swagger code generator program.
 * https://github.com/swagger-api/swagger-codegen.git
 * Do not edit the class manually.
 */


package io.pravega.segmentstore.server.host.rest.generated.model;

import com.fasterxml.jackson.annotation.JsonCreator;

/**
 * Gets or Sets HealthStatus
 */
public enum HealthStatus {
  
  UP("UP"),
  
  STARTING("STARTING"),
  
  NEW("NEW"),
  
  UNKNOWN("UNKNOWN"),
  
  FAILED("FAILED"),
  
  DOWN("DOWN");

  private String value;

  HealthStatus(String value) {
    this.value = value;
  }

  @Override
  public String toString() {
    return String.valueOf(value);
  }

  @JsonCreator
  public static HealthStatus fromValue(String text) {
    for (HealthStatus b : HealthStatus.values()) {
      if (String.valueOf(b.value).equals(text)) {
        return b;
      }
    }
    return null;
  }
}

