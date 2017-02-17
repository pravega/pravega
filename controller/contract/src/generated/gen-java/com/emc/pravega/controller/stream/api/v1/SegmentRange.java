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
public class SegmentRange implements org.apache.thrift.TBase<SegmentRange, SegmentRange._Fields>, java.io.Serializable, Cloneable, Comparable<SegmentRange> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("SegmentRange");

  private static final org.apache.thrift.protocol.TField SEGMENT_ID_FIELD_DESC = new org.apache.thrift.protocol.TField("segmentId", org.apache.thrift.protocol.TType.STRUCT, (short)1);
  private static final org.apache.thrift.protocol.TField MIN_KEY_FIELD_DESC = new org.apache.thrift.protocol.TField("minKey", org.apache.thrift.protocol.TType.DOUBLE, (short)2);
  private static final org.apache.thrift.protocol.TField MAX_KEY_FIELD_DESC = new org.apache.thrift.protocol.TField("maxKey", org.apache.thrift.protocol.TType.DOUBLE, (short)3);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new SegmentRangeStandardSchemeFactory());
    schemes.put(TupleScheme.class, new SegmentRangeTupleSchemeFactory());
  }

  private SegmentId segmentId; // required
  private double minKey; // required
  private double maxKey; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    SEGMENT_ID((short)1, "segmentId"),
    MIN_KEY((short)2, "minKey"),
    MAX_KEY((short)3, "maxKey");

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
        case 1: // SEGMENT_ID
          return SEGMENT_ID;
        case 2: // MIN_KEY
          return MIN_KEY;
        case 3: // MAX_KEY
          return MAX_KEY;
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
  private static final int __MINKEY_ISSET_ID = 0;
  private static final int __MAXKEY_ISSET_ID = 1;
  private byte __isset_bitfield = 0;
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.SEGMENT_ID, new org.apache.thrift.meta_data.FieldMetaData("segmentId", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, SegmentId.class)));
    tmpMap.put(_Fields.MIN_KEY, new org.apache.thrift.meta_data.FieldMetaData("minKey", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.DOUBLE)));
    tmpMap.put(_Fields.MAX_KEY, new org.apache.thrift.meta_data.FieldMetaData("maxKey", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.DOUBLE)));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(SegmentRange.class, metaDataMap);
  }

  public SegmentRange() {
  }

  public SegmentRange(
    SegmentId segmentId,
    double minKey,
    double maxKey)
  {
    this();
    this.segmentId = segmentId;
    this.minKey = minKey;
    setMinKeyIsSet(true);
    this.maxKey = maxKey;
    setMaxKeyIsSet(true);
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public SegmentRange(SegmentRange other) {
    __isset_bitfield = other.__isset_bitfield;
    if (other.isSetSegmentId()) {
      this.segmentId = new SegmentId(other.segmentId);
    }
    this.minKey = other.minKey;
    this.maxKey = other.maxKey;
  }

  public SegmentRange deepCopy() {
    return new SegmentRange(this);
  }

  @Override
  public void clear() {
    this.segmentId = null;
    setMinKeyIsSet(false);
    this.minKey = 0.0;
    setMaxKeyIsSet(false);
    this.maxKey = 0.0;
  }

  public SegmentId getSegmentId() {
    return this.segmentId;
  }

  public SegmentRange setSegmentId(SegmentId segmentId) {
    this.segmentId = segmentId;
    return this;
  }

  public void unsetSegmentId() {
    this.segmentId = null;
  }

  /** Returns true if field segmentId is set (has been assigned a value) and false otherwise */
  public boolean isSetSegmentId() {
    return this.segmentId != null;
  }

  public void setSegmentIdIsSet(boolean value) {
    if (!value) {
      this.segmentId = null;
    }
  }

  public double getMinKey() {
    return this.minKey;
  }

  public SegmentRange setMinKey(double minKey) {
    this.minKey = minKey;
    setMinKeyIsSet(true);
    return this;
  }

  public void unsetMinKey() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __MINKEY_ISSET_ID);
  }

  /** Returns true if field minKey is set (has been assigned a value) and false otherwise */
  public boolean isSetMinKey() {
    return EncodingUtils.testBit(__isset_bitfield, __MINKEY_ISSET_ID);
  }

  public void setMinKeyIsSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __MINKEY_ISSET_ID, value);
  }

  public double getMaxKey() {
    return this.maxKey;
  }

  public SegmentRange setMaxKey(double maxKey) {
    this.maxKey = maxKey;
    setMaxKeyIsSet(true);
    return this;
  }

  public void unsetMaxKey() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __MAXKEY_ISSET_ID);
  }

  /** Returns true if field maxKey is set (has been assigned a value) and false otherwise */
  public boolean isSetMaxKey() {
    return EncodingUtils.testBit(__isset_bitfield, __MAXKEY_ISSET_ID);
  }

  public void setMaxKeyIsSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __MAXKEY_ISSET_ID, value);
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case SEGMENT_ID:
      if (value == null) {
        unsetSegmentId();
      } else {
        setSegmentId((SegmentId)value);
      }
      break;

    case MIN_KEY:
      if (value == null) {
        unsetMinKey();
      } else {
        setMinKey((Double)value);
      }
      break;

    case MAX_KEY:
      if (value == null) {
        unsetMaxKey();
      } else {
        setMaxKey((Double)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case SEGMENT_ID:
      return getSegmentId();

    case MIN_KEY:
      return getMinKey();

    case MAX_KEY:
      return getMaxKey();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case SEGMENT_ID:
      return isSetSegmentId();
    case MIN_KEY:
      return isSetMinKey();
    case MAX_KEY:
      return isSetMaxKey();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof SegmentRange)
      return this.equals((SegmentRange)that);
    return false;
  }

  public boolean equals(SegmentRange that) {
    if (that == null)
      return false;

    boolean this_present_segmentId = true && this.isSetSegmentId();
    boolean that_present_segmentId = true && that.isSetSegmentId();
    if (this_present_segmentId || that_present_segmentId) {
      if (!(this_present_segmentId && that_present_segmentId))
        return false;
      if (!this.segmentId.equals(that.segmentId))
        return false;
    }

    boolean this_present_minKey = true;
    boolean that_present_minKey = true;
    if (this_present_minKey || that_present_minKey) {
      if (!(this_present_minKey && that_present_minKey))
        return false;
      if (this.minKey != that.minKey)
        return false;
    }

    boolean this_present_maxKey = true;
    boolean that_present_maxKey = true;
    if (this_present_maxKey || that_present_maxKey) {
      if (!(this_present_maxKey && that_present_maxKey))
        return false;
      if (this.maxKey != that.maxKey)
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    List<Object> list = new ArrayList<Object>();

    boolean present_segmentId = true && (isSetSegmentId());
    list.add(present_segmentId);
    if (present_segmentId)
      list.add(segmentId);

    boolean present_minKey = true;
    list.add(present_minKey);
    if (present_minKey)
      list.add(minKey);

    boolean present_maxKey = true;
    list.add(present_maxKey);
    if (present_maxKey)
      list.add(maxKey);

    return list.hashCode();
  }

  @Override
  public int compareTo(SegmentRange other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = Boolean.valueOf(isSetSegmentId()).compareTo(other.isSetSegmentId());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetSegmentId()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.segmentId, other.segmentId);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetMinKey()).compareTo(other.isSetMinKey());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetMinKey()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.minKey, other.minKey);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetMaxKey()).compareTo(other.isSetMaxKey());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetMaxKey()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.maxKey, other.maxKey);
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
    StringBuilder sb = new StringBuilder("SegmentRange(");
    boolean first = true;

    sb.append("segmentId:");
    if (this.segmentId == null) {
      sb.append("null");
    } else {
      sb.append(this.segmentId);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("minKey:");
    sb.append(this.minKey);
    first = false;
    if (!first) sb.append(", ");
    sb.append("maxKey:");
    sb.append(this.maxKey);
    first = false;
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    if (segmentId == null) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'segmentId' was not present! Struct: " + toString());
    }
    // alas, we cannot check 'minKey' because it's a primitive and you chose the non-beans generator.
    // alas, we cannot check 'maxKey' because it's a primitive and you chose the non-beans generator.
    // check for sub-struct validity
    if (segmentId != null) {
      segmentId.validate();
    }
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

  private static class SegmentRangeStandardSchemeFactory implements SchemeFactory {
    public SegmentRangeStandardScheme getScheme() {
      return new SegmentRangeStandardScheme();
    }
  }

  private static class SegmentRangeStandardScheme extends StandardScheme<SegmentRange> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, SegmentRange struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // SEGMENT_ID
            if (schemeField.type == org.apache.thrift.protocol.TType.STRUCT) {
              struct.segmentId = new SegmentId();
              struct.segmentId.read(iprot);
              struct.setSegmentIdIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // MIN_KEY
            if (schemeField.type == org.apache.thrift.protocol.TType.DOUBLE) {
              struct.minKey = iprot.readDouble();
              struct.setMinKeyIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 3: // MAX_KEY
            if (schemeField.type == org.apache.thrift.protocol.TType.DOUBLE) {
              struct.maxKey = iprot.readDouble();
              struct.setMaxKeyIsSet(true);
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
      if (!struct.isSetMinKey()) {
        throw new org.apache.thrift.protocol.TProtocolException("Required field 'minKey' was not found in serialized data! Struct: " + toString());
      }
      if (!struct.isSetMaxKey()) {
        throw new org.apache.thrift.protocol.TProtocolException("Required field 'maxKey' was not found in serialized data! Struct: " + toString());
      }
      struct.validate();
    }

    public void write(org.apache.thrift.protocol.TProtocol oprot, SegmentRange struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.segmentId != null) {
        oprot.writeFieldBegin(SEGMENT_ID_FIELD_DESC);
        struct.segmentId.write(oprot);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldBegin(MIN_KEY_FIELD_DESC);
      oprot.writeDouble(struct.minKey);
      oprot.writeFieldEnd();
      oprot.writeFieldBegin(MAX_KEY_FIELD_DESC);
      oprot.writeDouble(struct.maxKey);
      oprot.writeFieldEnd();
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class SegmentRangeTupleSchemeFactory implements SchemeFactory {
    public SegmentRangeTupleScheme getScheme() {
      return new SegmentRangeTupleScheme();
    }
  }

  private static class SegmentRangeTupleScheme extends TupleScheme<SegmentRange> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, SegmentRange struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      struct.segmentId.write(oprot);
      oprot.writeDouble(struct.minKey);
      oprot.writeDouble(struct.maxKey);
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, SegmentRange struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      struct.segmentId = new SegmentId();
      struct.segmentId.read(iprot);
      struct.setSegmentIdIsSet(true);
      struct.minKey = iprot.readDouble();
      struct.setMinKeyIsSet(true);
      struct.maxKey = iprot.readDouble();
      struct.setMaxKeyIsSet(true);
    }
  }

}

