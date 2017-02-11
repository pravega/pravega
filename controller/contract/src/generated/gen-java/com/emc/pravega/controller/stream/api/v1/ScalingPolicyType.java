/**
 *  Copyright (c) 2016 Dell Inc. or its subsidiaries. All Rights Reserved
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
/**
 * Autogenerated by Thrift Compiler (0.9.3)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package com.emc.pravega.controller.stream.api.v1;


import java.util.Map;
import java.util.HashMap;
import org.apache.thrift.TEnum;

public enum ScalingPolicyType implements org.apache.thrift.TEnum {
  FIXED_NUM_SEGMENTS(0),
  BY_RATE_IN_BYTES(1),
  BY_RATE_IN_EVENTS(2);

  private final int value;

  private ScalingPolicyType(int value) {
    this.value = value;
  }

  /**
   * Get the integer value of this enum value, as defined in the Thrift IDL.
   */
  public int getValue() {
    return value;
  }

  /**
   * Find a the enum type by its integer value, as defined in the Thrift IDL.
   * @return null if the value is not found.
   */
  public static ScalingPolicyType findByValue(int value) { 
    switch (value) {
      case 0:
        return FIXED_NUM_SEGMENTS;
      case 1:
        return BY_RATE_IN_BYTES;
      case 2:
        return BY_RATE_IN_EVENTS;
      default:
        return null;
    }
  }
}
