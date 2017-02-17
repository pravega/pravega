/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
/**
 * Autogenerated by Thrift Compiler (0.9.3)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package com.emc.pravega.controller.stream.api.v1;

import org.apache.thrift.scheme.IScheme;
import org.apache.thrift.scheme.SchemeFactory;
import org.apache.thrift.scheme.StandardScheme;

import org.apache.thrift.scheme.TupleScheme;
import org.apache.thrift.protocol.TTupleProtocol;
import org.apache.thrift.protocol.TProtocolException;
import org.apache.thrift.EncodingUtils;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.server.AbstractNonblockingServer.*;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.EnumMap;
import java.util.Set;
import java.util.HashSet;
import java.util.EnumSet;
import java.util.Collections;
import java.util.BitSet;
import java.nio.ByteBuffer;
import java.util.Arrays;
import javax.annotation.Generated;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings({"cast", "rawtypes", "serial", "unchecked"})
@Generated(value = "Autogenerated by Thrift Compiler (0.9.3)", date = "2017-01-28")
public class ScalingPolicy implements org.apache.thrift.TBase<ScalingPolicy, ScalingPolicy._Fields>, java.io.Serializable, Cloneable, Comparable<ScalingPolicy> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("ScalingPolicy");

  private static final org.apache.thrift.protocol.TField TYPE_FIELD_DESC = new org.apache.thrift.protocol.TField("type", org.apache.thrift.protocol.TType.I32, (short)1);
  private static final org.apache.thrift.protocol.TField TARGET_RATE_FIELD_DESC = new org.apache.thrift.protocol.TField("targetRate", org.apache.thrift.protocol.TType.I64, (short)2);
  private static final org.apache.thrift.protocol.TField SCALE_FACTOR_FIELD_DESC = new org.apache.thrift.protocol.TField("scaleFactor", org.apache.thrift.protocol.TType.I32, (short)3);
  private static final org.apache.thrift.protocol.TField MIN_NUM_SEGMENTS_FIELD_DESC = new org.apache.thrift.protocol.TField("minNumSegments", org.apache.thrift.protocol.TType.I32, (short)4);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new ScalingPolicyStandardSchemeFactory());
    schemes.put(TupleScheme.class, new ScalingPolicyTupleSchemeFactory());
  }

  private ScalingPolicyType type; // required
  private long targetRate; // required
  private int scaleFactor; // required
  private int minNumSegments; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    /**
     * 
     * @see ScalingPolicyType
     */
    TYPE((short)1, "type"),
    TARGET_RATE((short)2, "targetRate"),
    SCALE_FACTOR((short)3, "scaleFactor"),
    MIN_NUM_SEGMENTS((short)4, "minNumSegments");

    private static final Map<String, _Fields> byName = new HashMap<String, _Fields>();

    static {
      for (_Fields field : EnumSet.allOf(_Fields.class)) {
        byName.put(field.getFieldName(), field);
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, or null if its not found.
     */
    public static _Fields findByThriftId(int fieldId) {
      switch(fieldId) {
        case 1: // TYPE
          return TYPE;
        case 2: // TARGET_RATE
          return TARGET_RATE;
        case 3: // SCALE_FACTOR
          return SCALE_FACTOR;
        case 4: // MIN_NUM_SEGMENTS
          return MIN_NUM_SEGMENTS;
        default:
          return null;
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, throwing an exception
     * if it is not found.
     */
    public static _Fields findByThriftIdOrThrow(int fieldId) {
      _Fields fields = findByThriftId(fieldId);
      if (fields == null) throw new IllegalArgumentException("Field " + fieldId + " doesn't exist!");
      return fields;
    }

    /**
     * Find the _Fields constant that matches name, or null if its not found.
     */
    public static _Fields findByName(String name) {
      return byName.get(name);
    }

    private final short _thriftId;
    private final String _fieldName;

    _Fields(short thriftId, String fieldName) {
      _thriftId = thriftId;
      _fieldName = fieldName;
    }

    public short getThriftFieldId() {
      return _thriftId;
    }

    public String getFieldName() {
      return _fieldName;
    }
  }

  // isset id assignments
  private static final int __TARGETRATE_ISSET_ID = 0;
  private static final int __SCALEFACTOR_ISSET_ID = 1;
  private static final int __MINNUMSEGMENTS_ISSET_ID = 2;
  private byte __isset_bitfield = 0;
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.TYPE, new org.apache.thrift.meta_data.FieldMetaData("type", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.EnumMetaData(org.apache.thrift.protocol.TType.ENUM, ScalingPolicyType.class)));
    tmpMap.put(_Fields.TARGET_RATE, new org.apache.thrift.meta_data.FieldMetaData("targetRate", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64)));
    tmpMap.put(_Fields.SCALE_FACTOR, new org.apache.thrift.meta_data.FieldMetaData("scaleFactor", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I32)));
    tmpMap.put(_Fields.MIN_NUM_SEGMENTS, new org.apache.thrift.meta_data.FieldMetaData("minNumSegments", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I32)));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(ScalingPolicy.class, metaDataMap);
  }

  public ScalingPolicy() {
  }

  public ScalingPolicy(
    ScalingPolicyType type,
    long targetRate,
    int scaleFactor,
    int minNumSegments)
  {
    this();
    this.type = type;
    this.targetRate = targetRate;
    setTargetRateIsSet(true);
    this.scaleFactor = scaleFactor;
    setScaleFactorIsSet(true);
    this.minNumSegments = minNumSegments;
    setMinNumSegmentsIsSet(true);
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public ScalingPolicy(ScalingPolicy other) {
    __isset_bitfield = other.__isset_bitfield;
    if (other.isSetType()) {
      this.type = other.type;
    }
    this.targetRate = other.targetRate;
    this.scaleFactor = other.scaleFactor;
    this.minNumSegments = other.minNumSegments;
  }

  public ScalingPolicy deepCopy() {
    return new ScalingPolicy(this);
  }

  @Override
  public void clear() {
    this.type = null;
    setTargetRateIsSet(false);
    this.targetRate = 0;
    setScaleFactorIsSet(false);
    this.scaleFactor = 0;
    setMinNumSegmentsIsSet(false);
    this.minNumSegments = 0;
  }

  /**
   * 
   * @see ScalingPolicyType
   */
  public ScalingPolicyType getType() {
    return this.type;
  }

  /**
   * 
   * @see ScalingPolicyType
   */
  public ScalingPolicy setType(ScalingPolicyType type) {
    this.type = type;
    return this;
  }

  public void unsetType() {
    this.type = null;
  }

  /** Returns true if field type is set (has been assigned a value) and false otherwise */
  public boolean isSetType() {
    return this.type != null;
  }

  public void setTypeIsSet(boolean value) {
    if (!value) {
      this.type = null;
    }
  }

  public long getTargetRate() {
    return this.targetRate;
  }

  public ScalingPolicy setTargetRate(long targetRate) {
    this.targetRate = targetRate;
    setTargetRateIsSet(true);
    return this;
  }

  public void unsetTargetRate() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __TARGETRATE_ISSET_ID);
  }

  /** Returns true if field targetRate is set (has been assigned a value) and false otherwise */
  public boolean isSetTargetRate() {
    return EncodingUtils.testBit(__isset_bitfield, __TARGETRATE_ISSET_ID);
  }

  public void setTargetRateIsSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __TARGETRATE_ISSET_ID, value);
  }

  public int getScaleFactor() {
    return this.scaleFactor;
  }

  public ScalingPolicy setScaleFactor(int scaleFactor) {
    this.scaleFactor = scaleFactor;
    setScaleFactorIsSet(true);
    return this;
  }

  public void unsetScaleFactor() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __SCALEFACTOR_ISSET_ID);
  }

  /** Returns true if field scaleFactor is set (has been assigned a value) and false otherwise */
  public boolean isSetScaleFactor() {
    return EncodingUtils.testBit(__isset_bitfield, __SCALEFACTOR_ISSET_ID);
  }

  public void setScaleFactorIsSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __SCALEFACTOR_ISSET_ID, value);
  }

  public int getMinNumSegments() {
    return this.minNumSegments;
  }

  public ScalingPolicy setMinNumSegments(int minNumSegments) {
    this.minNumSegments = minNumSegments;
    setMinNumSegmentsIsSet(true);
    return this;
  }

  public void unsetMinNumSegments() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __MINNUMSEGMENTS_ISSET_ID);
  }

  /** Returns true if field minNumSegments is set (has been assigned a value) and false otherwise */
  public boolean isSetMinNumSegments() {
    return EncodingUtils.testBit(__isset_bitfield, __MINNUMSEGMENTS_ISSET_ID);
  }

  public void setMinNumSegmentsIsSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __MINNUMSEGMENTS_ISSET_ID, value);
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case TYPE:
      if (value == null) {
        unsetType();
      } else {
        setType((ScalingPolicyType)value);
      }
      break;

    case TARGET_RATE:
      if (value == null) {
        unsetTargetRate();
      } else {
        setTargetRate((Long)value);
      }
      break;

    case SCALE_FACTOR:
      if (value == null) {
        unsetScaleFactor();
      } else {
        setScaleFactor((Integer)value);
      }
      break;

    case MIN_NUM_SEGMENTS:
      if (value == null) {
        unsetMinNumSegments();
      } else {
        setMinNumSegments((Integer)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case TYPE:
      return getType();

    case TARGET_RATE:
      return getTargetRate();

    case SCALE_FACTOR:
      return getScaleFactor();

    case MIN_NUM_SEGMENTS:
      return getMinNumSegments();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case TYPE:
      return isSetType();
    case TARGET_RATE:
      return isSetTargetRate();
    case SCALE_FACTOR:
      return isSetScaleFactor();
    case MIN_NUM_SEGMENTS:
      return isSetMinNumSegments();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof ScalingPolicy)
      return this.equals((ScalingPolicy)that);
    return false;
  }

  public boolean equals(ScalingPolicy that) {
    if (that == null)
      return false;

    boolean this_present_type = true && this.isSetType();
    boolean that_present_type = true && that.isSetType();
    if (this_present_type || that_present_type) {
      if (!(this_present_type && that_present_type))
        return false;
      if (!this.type.equals(that.type))
        return false;
    }

    boolean this_present_targetRate = true;
    boolean that_present_targetRate = true;
    if (this_present_targetRate || that_present_targetRate) {
      if (!(this_present_targetRate && that_present_targetRate))
        return false;
      if (this.targetRate != that.targetRate)
        return false;
    }

    boolean this_present_scaleFactor = true;
    boolean that_present_scaleFactor = true;
    if (this_present_scaleFactor || that_present_scaleFactor) {
      if (!(this_present_scaleFactor && that_present_scaleFactor))
        return false;
      if (this.scaleFactor != that.scaleFactor)
        return false;
    }

    boolean this_present_minNumSegments = true;
    boolean that_present_minNumSegments = true;
    if (this_present_minNumSegments || that_present_minNumSegments) {
      if (!(this_present_minNumSegments && that_present_minNumSegments))
        return false;
      if (this.minNumSegments != that.minNumSegments)
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    List<Object> list = new ArrayList<Object>();

    boolean present_type = true && (isSetType());
    list.add(present_type);
    if (present_type)
      list.add(type.getValue());

    boolean present_targetRate = true;
    list.add(present_targetRate);
    if (present_targetRate)
      list.add(targetRate);

    boolean present_scaleFactor = true;
    list.add(present_scaleFactor);
    if (present_scaleFactor)
      list.add(scaleFactor);

    boolean present_minNumSegments = true;
    list.add(present_minNumSegments);
    if (present_minNumSegments)
      list.add(minNumSegments);

    return list.hashCode();
  }

  @Override
  public int compareTo(ScalingPolicy other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = Boolean.valueOf(isSetType()).compareTo(other.isSetType());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetType()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.type, other.type);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetTargetRate()).compareTo(other.isSetTargetRate());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetTargetRate()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.targetRate, other.targetRate);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetScaleFactor()).compareTo(other.isSetScaleFactor());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetScaleFactor()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.scaleFactor, other.scaleFactor);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetMinNumSegments()).compareTo(other.isSetMinNumSegments());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetMinNumSegments()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.minNumSegments, other.minNumSegments);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    return 0;
  }

  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
  }

  public void read(org.apache.thrift.protocol.TProtocol iprot) throws org.apache.thrift.TException {
    schemes.get(iprot.getScheme()).getScheme().read(iprot, this);
  }

  public void write(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
    schemes.get(oprot.getScheme()).getScheme().write(oprot, this);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("ScalingPolicy(");
    boolean first = true;

    sb.append("type:");
    if (this.type == null) {
      sb.append("null");
    } else {
      sb.append(this.type);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("targetRate:");
    sb.append(this.targetRate);
    first = false;
    if (!first) sb.append(", ");
    sb.append("scaleFactor:");
    sb.append(this.scaleFactor);
    first = false;
    if (!first) sb.append(", ");
    sb.append("minNumSegments:");
    sb.append(this.minNumSegments);
    first = false;
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    if (type == null) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'type' was not present! Struct: " + toString());
    }
    // alas, we cannot check 'targetRate' because it's a primitive and you chose the non-beans generator.
    // alas, we cannot check 'scaleFactor' because it's a primitive and you chose the non-beans generator.
    // alas, we cannot check 'minNumSegments' because it's a primitive and you chose the non-beans generator.
    // check for sub-struct validity
  }

  private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
    try {
      write(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(out)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, ClassNotFoundException {
    try {
      // it doesn't seem like you should have to do this, but java serialization is wacky, and doesn't call the default constructor.
      __isset_bitfield = 0;
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class ScalingPolicyStandardSchemeFactory implements SchemeFactory {
    public ScalingPolicyStandardScheme getScheme() {
      return new ScalingPolicyStandardScheme();
    }
  }

  private static class ScalingPolicyStandardScheme extends StandardScheme<ScalingPolicy> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, ScalingPolicy struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // TYPE
            if (schemeField.type == org.apache.thrift.protocol.TType.I32) {
              struct.type = com.emc.pravega.controller.stream.api.v1.ScalingPolicyType.findByValue(iprot.readI32());
              struct.setTypeIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // TARGET_RATE
            if (schemeField.type == org.apache.thrift.protocol.TType.I64) {
              struct.targetRate = iprot.readI64();
              struct.setTargetRateIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 3: // SCALE_FACTOR
            if (schemeField.type == org.apache.thrift.protocol.TType.I32) {
              struct.scaleFactor = iprot.readI32();
              struct.setScaleFactorIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 4: // MIN_NUM_SEGMENTS
            if (schemeField.type == org.apache.thrift.protocol.TType.I32) {
              struct.minNumSegments = iprot.readI32();
              struct.setMinNumSegmentsIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          default:
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();

      // check for required fields of primitive type, which can't be checked in the validate method
      if (!struct.isSetTargetRate()) {
        throw new org.apache.thrift.protocol.TProtocolException("Required field 'targetRate' was not found in serialized data! Struct: " + toString());
      }
      if (!struct.isSetScaleFactor()) {
        throw new org.apache.thrift.protocol.TProtocolException("Required field 'scaleFactor' was not found in serialized data! Struct: " + toString());
      }
      if (!struct.isSetMinNumSegments()) {
        throw new org.apache.thrift.protocol.TProtocolException("Required field 'minNumSegments' was not found in serialized data! Struct: " + toString());
      }
      struct.validate();
    }

    public void write(org.apache.thrift.protocol.TProtocol oprot, ScalingPolicy struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.type != null) {
        oprot.writeFieldBegin(TYPE_FIELD_DESC);
        oprot.writeI32(struct.type.getValue());
        oprot.writeFieldEnd();
      }
      oprot.writeFieldBegin(TARGET_RATE_FIELD_DESC);
      oprot.writeI64(struct.targetRate);
      oprot.writeFieldEnd();
      oprot.writeFieldBegin(SCALE_FACTOR_FIELD_DESC);
      oprot.writeI32(struct.scaleFactor);
      oprot.writeFieldEnd();
      oprot.writeFieldBegin(MIN_NUM_SEGMENTS_FIELD_DESC);
      oprot.writeI32(struct.minNumSegments);
      oprot.writeFieldEnd();
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class ScalingPolicyTupleSchemeFactory implements SchemeFactory {
    public ScalingPolicyTupleScheme getScheme() {
      return new ScalingPolicyTupleScheme();
    }
  }

  private static class ScalingPolicyTupleScheme extends TupleScheme<ScalingPolicy> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, ScalingPolicy struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      oprot.writeI32(struct.type.getValue());
      oprot.writeI64(struct.targetRate);
      oprot.writeI32(struct.scaleFactor);
      oprot.writeI32(struct.minNumSegments);
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, ScalingPolicy struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      struct.type = com.emc.pravega.controller.stream.api.v1.ScalingPolicyType.findByValue(iprot.readI32());
      struct.setTypeIsSet(true);
      struct.targetRate = iprot.readI64();
      struct.setTargetRateIsSet(true);
      struct.scaleFactor = iprot.readI32();
      struct.setScaleFactorIsSet(true);
      struct.minNumSegments = iprot.readI32();
      struct.setMinNumSegmentsIsSet(true);
    }
  }

}

