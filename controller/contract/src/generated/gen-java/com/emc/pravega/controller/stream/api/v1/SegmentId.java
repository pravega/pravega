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
@Generated(value = "Autogenerated by Thrift Compiler (0.9.3)", date = "2017-01-25")
public class SegmentId implements org.apache.thrift.TBase<SegmentId, SegmentId._Fields>, java.io.Serializable, Cloneable, Comparable<SegmentId> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("SegmentId");

  private static final org.apache.thrift.protocol.TField SCOPE_FIELD_DESC = new org.apache.thrift.protocol.TField("scope", org.apache.thrift.protocol.TType.STRING, (short)1);
  private static final org.apache.thrift.protocol.TField STREAM_NAME_FIELD_DESC = new org.apache.thrift.protocol.TField("streamName", org.apache.thrift.protocol.TType.STRING, (short)2);
  private static final org.apache.thrift.protocol.TField NUMBER_FIELD_DESC = new org.apache.thrift.protocol.TField("number", org.apache.thrift.protocol.TType.I32, (short)3);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new SegmentIdStandardSchemeFactory());
    schemes.put(TupleScheme.class, new SegmentIdTupleSchemeFactory());
  }

  public String scope; // required
  public String streamName; // required
  public int number; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    SCOPE((short)1, "scope"),
    STREAM_NAME((short)2, "streamName"),
    NUMBER((short)3, "number");

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
        case 1: // SCOPE
          return SCOPE;
        case 2: // STREAM_NAME
          return STREAM_NAME;
        case 3: // NUMBER
          return NUMBER;
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
  private static final int __NUMBER_ISSET_ID = 0;
  private byte __isset_bitfield = 0;
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.SCOPE, new org.apache.thrift.meta_data.FieldMetaData("scope", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.STREAM_NAME, new org.apache.thrift.meta_data.FieldMetaData("streamName", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.NUMBER, new org.apache.thrift.meta_data.FieldMetaData("number", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I32)));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(SegmentId.class, metaDataMap);
  }

  public SegmentId() {
  }

  public SegmentId(
    String scope,
    String streamName,
    int number)
  {
    this();
    this.scope = scope;
    this.streamName = streamName;
    this.number = number;
    setNumberIsSet(true);
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public SegmentId(SegmentId other) {
    __isset_bitfield = other.__isset_bitfield;
    if (other.isSetScope()) {
      this.scope = other.scope;
    }
    if (other.isSetStreamName()) {
      this.streamName = other.streamName;
    }
    this.number = other.number;
  }

  public SegmentId deepCopy() {
    return new SegmentId(this);
  }

  @Override
  public void clear() {
    this.scope = null;
    this.streamName = null;
    setNumberIsSet(false);
    this.number = 0;
  }

  public String getScope() {
    return this.scope;
  }

  public SegmentId setScope(String scope) {
    this.scope = scope;
    return this;
  }

  public void unsetScope() {
    this.scope = null;
  }

  /** Returns true if field scope is set (has been assigned a value) and false otherwise */
  public boolean isSetScope() {
    return this.scope != null;
  }

  public void setScopeIsSet(boolean value) {
    if (!value) {
      this.scope = null;
    }
  }

  public String getStreamName() {
    return this.streamName;
  }

  public SegmentId setStreamName(String streamName) {
    this.streamName = streamName;
    return this;
  }

  public void unsetStreamName() {
    this.streamName = null;
  }

  /** Returns true if field streamName is set (has been assigned a value) and false otherwise */
  public boolean isSetStreamName() {
    return this.streamName != null;
  }

  public void setStreamNameIsSet(boolean value) {
    if (!value) {
      this.streamName = null;
    }
  }

  public int getNumber() {
    return this.number;
  }

  public SegmentId setNumber(int number) {
    this.number = number;
    setNumberIsSet(true);
    return this;
  }

  public void unsetNumber() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __NUMBER_ISSET_ID);
  }

  /** Returns true if field number is set (has been assigned a value) and false otherwise */
  public boolean isSetNumber() {
    return EncodingUtils.testBit(__isset_bitfield, __NUMBER_ISSET_ID);
  }

  public void setNumberIsSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __NUMBER_ISSET_ID, value);
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case SCOPE:
      if (value == null) {
        unsetScope();
      } else {
        setScope((String)value);
      }
      break;

    case STREAM_NAME:
      if (value == null) {
        unsetStreamName();
      } else {
        setStreamName((String)value);
      }
      break;

    case NUMBER:
      if (value == null) {
        unsetNumber();
      } else {
        setNumber((Integer)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case SCOPE:
      return getScope();

    case STREAM_NAME:
      return getStreamName();

    case NUMBER:
      return getNumber();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case SCOPE:
      return isSetScope();
    case STREAM_NAME:
      return isSetStreamName();
    case NUMBER:
      return isSetNumber();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof SegmentId)
      return this.equals((SegmentId)that);
    return false;
  }

  public boolean equals(SegmentId that) {
    if (that == null)
      return false;

    boolean this_present_scope = true && this.isSetScope();
    boolean that_present_scope = true && that.isSetScope();
    if (this_present_scope || that_present_scope) {
      if (!(this_present_scope && that_present_scope))
        return false;
      if (!this.scope.equals(that.scope))
        return false;
    }

    boolean this_present_streamName = true && this.isSetStreamName();
    boolean that_present_streamName = true && that.isSetStreamName();
    if (this_present_streamName || that_present_streamName) {
      if (!(this_present_streamName && that_present_streamName))
        return false;
      if (!this.streamName.equals(that.streamName))
        return false;
    }

    boolean this_present_number = true;
    boolean that_present_number = true;
    if (this_present_number || that_present_number) {
      if (!(this_present_number && that_present_number))
        return false;
      if (this.number != that.number)
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    List<Object> list = new ArrayList<Object>();

    boolean present_scope = true && (isSetScope());
    list.add(present_scope);
    if (present_scope)
      list.add(scope);

    boolean present_streamName = true && (isSetStreamName());
    list.add(present_streamName);
    if (present_streamName)
      list.add(streamName);

    boolean present_number = true;
    list.add(present_number);
    if (present_number)
      list.add(number);

    return list.hashCode();
  }

  @Override
  public int compareTo(SegmentId other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = Boolean.valueOf(isSetScope()).compareTo(other.isSetScope());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetScope()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.scope, other.scope);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetStreamName()).compareTo(other.isSetStreamName());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetStreamName()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.streamName, other.streamName);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetNumber()).compareTo(other.isSetNumber());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetNumber()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.number, other.number);
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
    StringBuilder sb = new StringBuilder("SegmentId(");
    boolean first = true;

    sb.append("scope:");
    if (this.scope == null) {
      sb.append("null");
    } else {
      sb.append(this.scope);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("streamName:");
    if (this.streamName == null) {
      sb.append("null");
    } else {
      sb.append(this.streamName);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("number:");
    sb.append(this.number);
    first = false;
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    if (scope == null) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'scope' was not present! Struct: " + toString());
    }
    if (streamName == null) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'streamName' was not present! Struct: " + toString());
    }
    // alas, we cannot check 'number' because it's a primitive and you chose the non-beans generator.
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

  private static class SegmentIdStandardSchemeFactory implements SchemeFactory {
    public SegmentIdStandardScheme getScheme() {
      return new SegmentIdStandardScheme();
    }
  }

  private static class SegmentIdStandardScheme extends StandardScheme<SegmentId> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, SegmentId struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // SCOPE
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.scope = iprot.readString();
              struct.setScopeIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // STREAM_NAME
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.streamName = iprot.readString();
              struct.setStreamNameIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 3: // NUMBER
            if (schemeField.type == org.apache.thrift.protocol.TType.I32) {
              struct.number = iprot.readI32();
              struct.setNumberIsSet(true);
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
      if (!struct.isSetNumber()) {
        throw new org.apache.thrift.protocol.TProtocolException("Required field 'number' was not found in serialized data! Struct: " + toString());
      }
      struct.validate();
    }

    public void write(org.apache.thrift.protocol.TProtocol oprot, SegmentId struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.scope != null) {
        oprot.writeFieldBegin(SCOPE_FIELD_DESC);
        oprot.writeString(struct.scope);
        oprot.writeFieldEnd();
      }
      if (struct.streamName != null) {
        oprot.writeFieldBegin(STREAM_NAME_FIELD_DESC);
        oprot.writeString(struct.streamName);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldBegin(NUMBER_FIELD_DESC);
      oprot.writeI32(struct.number);
      oprot.writeFieldEnd();
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class SegmentIdTupleSchemeFactory implements SchemeFactory {
    public SegmentIdTupleScheme getScheme() {
      return new SegmentIdTupleScheme();
    }
  }

  private static class SegmentIdTupleScheme extends TupleScheme<SegmentId> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, SegmentId struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      oprot.writeString(struct.scope);
      oprot.writeString(struct.streamName);
      oprot.writeI32(struct.number);
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, SegmentId struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      struct.scope = iprot.readString();
      struct.setScopeIsSet(true);
      struct.streamName = iprot.readString();
      struct.setStreamNameIsSet(true);
      struct.number = iprot.readI32();
      struct.setNumberIsSet(true);
    }
  }

}

