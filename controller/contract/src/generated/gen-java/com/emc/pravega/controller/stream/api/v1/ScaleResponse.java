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
@Generated(value = "Autogenerated by Thrift Compiler (0.9.3)", date = "2017-02-11")
public class ScaleResponse implements org.apache.thrift.TBase<ScaleResponse, ScaleResponse._Fields>, java.io.Serializable, Cloneable, Comparable<ScaleResponse> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("ScaleResponse");

  private static final org.apache.thrift.protocol.TField STATUS_FIELD_DESC = new org.apache.thrift.protocol.TField("status", org.apache.thrift.protocol.TType.I32, (short)1);
  private static final org.apache.thrift.protocol.TField SEGMENTS_FIELD_DESC = new org.apache.thrift.protocol.TField("segments", org.apache.thrift.protocol.TType.LIST, (short)2);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new ScaleResponseStandardSchemeFactory());
    schemes.put(TupleScheme.class, new ScaleResponseTupleSchemeFactory());
  }

  private ScaleStreamStatus status; // required
  private List<SegmentRange> segments; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    /**
     * 
     * @see ScaleStreamStatus
     */
    STATUS((short)1, "status"),
    SEGMENTS((short)2, "segments");

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
        case 1: // STATUS
          return STATUS;
        case 2: // SEGMENTS
          return SEGMENTS;
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
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.STATUS, new org.apache.thrift.meta_data.FieldMetaData("status", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.EnumMetaData(org.apache.thrift.protocol.TType.ENUM, ScaleStreamStatus.class)));
    tmpMap.put(_Fields.SEGMENTS, new org.apache.thrift.meta_data.FieldMetaData("segments", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.ListMetaData(org.apache.thrift.protocol.TType.LIST, 
            new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, SegmentRange.class))));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(ScaleResponse.class, metaDataMap);
  }

  public ScaleResponse() {
  }

  public ScaleResponse(
    ScaleStreamStatus status,
    List<SegmentRange> segments)
  {
    this();
    this.status = status;
    this.segments = segments;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public ScaleResponse(ScaleResponse other) {
    if (other.isSetStatus()) {
      this.status = other.status;
    }
    if (other.isSetSegments()) {
      List<SegmentRange> __this__segments = new ArrayList<SegmentRange>(other.segments.size());
      for (SegmentRange other_element : other.segments) {
        __this__segments.add(new SegmentRange(other_element));
      }
      this.segments = __this__segments;
    }
  }

  public ScaleResponse deepCopy() {
    return new ScaleResponse(this);
  }

  @Override
  public void clear() {
    this.status = null;
    this.segments = null;
  }

  /**
   * 
   * @see ScaleStreamStatus
   */
  public ScaleStreamStatus getStatus() {
    return this.status;
  }

  /**
   * 
   * @see ScaleStreamStatus
   */
  public ScaleResponse setStatus(ScaleStreamStatus status) {
    this.status = status;
    return this;
  }

  public void unsetStatus() {
    this.status = null;
  }

  /** Returns true if field status is set (has been assigned a value) and false otherwise */
  public boolean isSetStatus() {
    return this.status != null;
  }

  public void setStatusIsSet(boolean value) {
    if (!value) {
      this.status = null;
    }
  }

  public int getSegmentsSize() {
    return (this.segments == null) ? 0 : this.segments.size();
  }

  public java.util.Iterator<SegmentRange> getSegmentsIterator() {
    return (this.segments == null) ? null : this.segments.iterator();
  }

  public void addToSegments(SegmentRange elem) {
    if (this.segments == null) {
      this.segments = new ArrayList<SegmentRange>();
    }
    this.segments.add(elem);
  }

  public List<SegmentRange> getSegments() {
    return this.segments;
  }

  public ScaleResponse setSegments(List<SegmentRange> segments) {
    this.segments = segments;
    return this;
  }

  public void unsetSegments() {
    this.segments = null;
  }

  /** Returns true if field segments is set (has been assigned a value) and false otherwise */
  public boolean isSetSegments() {
    return this.segments != null;
  }

  public void setSegmentsIsSet(boolean value) {
    if (!value) {
      this.segments = null;
    }
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case STATUS:
      if (value == null) {
        unsetStatus();
      } else {
        setStatus((ScaleStreamStatus)value);
      }
      break;

    case SEGMENTS:
      if (value == null) {
        unsetSegments();
      } else {
        setSegments((List<SegmentRange>)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case STATUS:
      return getStatus();

    case SEGMENTS:
      return getSegments();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case STATUS:
      return isSetStatus();
    case SEGMENTS:
      return isSetSegments();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof ScaleResponse)
      return this.equals((ScaleResponse)that);
    return false;
  }

  public boolean equals(ScaleResponse that) {
    if (that == null)
      return false;

    boolean this_present_status = true && this.isSetStatus();
    boolean that_present_status = true && that.isSetStatus();
    if (this_present_status || that_present_status) {
      if (!(this_present_status && that_present_status))
        return false;
      if (!this.status.equals(that.status))
        return false;
    }

    boolean this_present_segments = true && this.isSetSegments();
    boolean that_present_segments = true && that.isSetSegments();
    if (this_present_segments || that_present_segments) {
      if (!(this_present_segments && that_present_segments))
        return false;
      if (!this.segments.equals(that.segments))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    List<Object> list = new ArrayList<Object>();

    boolean present_status = true && (isSetStatus());
    list.add(present_status);
    if (present_status)
      list.add(status.getValue());

    boolean present_segments = true && (isSetSegments());
    list.add(present_segments);
    if (present_segments)
      list.add(segments);

    return list.hashCode();
  }

  @Override
  public int compareTo(ScaleResponse other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = Boolean.valueOf(isSetStatus()).compareTo(other.isSetStatus());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetStatus()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.status, other.status);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetSegments()).compareTo(other.isSetSegments());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetSegments()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.segments, other.segments);
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
    StringBuilder sb = new StringBuilder("ScaleResponse(");
    boolean first = true;

    sb.append("status:");
    if (this.status == null) {
      sb.append("null");
    } else {
      sb.append(this.status);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("segments:");
    if (this.segments == null) {
      sb.append("null");
    } else {
      sb.append(this.segments);
    }
    first = false;
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    if (status == null) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'status' was not present! Struct: " + toString());
    }
    if (segments == null) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'segments' was not present! Struct: " + toString());
    }
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
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class ScaleResponseStandardSchemeFactory implements SchemeFactory {
    public ScaleResponseStandardScheme getScheme() {
      return new ScaleResponseStandardScheme();
    }
  }

  private static class ScaleResponseStandardScheme extends StandardScheme<ScaleResponse> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, ScaleResponse struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // STATUS
            if (schemeField.type == org.apache.thrift.protocol.TType.I32) {
              struct.status = com.emc.pravega.controller.stream.api.v1.ScaleStreamStatus.findByValue(iprot.readI32());
              struct.setStatusIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // SEGMENTS
            if (schemeField.type == org.apache.thrift.protocol.TType.LIST) {
              {
                org.apache.thrift.protocol.TList _list10 = iprot.readListBegin();
                struct.segments = new ArrayList<SegmentRange>(_list10.size);
                SegmentRange _elem11;
                for (int _i12 = 0; _i12 < _list10.size; ++_i12)
                {
                  _elem11 = new SegmentRange();
                  _elem11.read(iprot);
                  struct.segments.add(_elem11);
                }
                iprot.readListEnd();
              }
              struct.setSegmentsIsSet(true);
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
      struct.validate();
    }

    public void write(org.apache.thrift.protocol.TProtocol oprot, ScaleResponse struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.status != null) {
        oprot.writeFieldBegin(STATUS_FIELD_DESC);
        oprot.writeI32(struct.status.getValue());
        oprot.writeFieldEnd();
      }
      if (struct.segments != null) {
        oprot.writeFieldBegin(SEGMENTS_FIELD_DESC);
        {
          oprot.writeListBegin(new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRUCT, struct.segments.size()));
          for (SegmentRange _iter13 : struct.segments)
          {
            _iter13.write(oprot);
          }
          oprot.writeListEnd();
        }
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class ScaleResponseTupleSchemeFactory implements SchemeFactory {
    public ScaleResponseTupleScheme getScheme() {
      return new ScaleResponseTupleScheme();
    }
  }

  private static class ScaleResponseTupleScheme extends TupleScheme<ScaleResponse> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, ScaleResponse struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      oprot.writeI32(struct.status.getValue());
      {
        oprot.writeI32(struct.segments.size());
        for (SegmentRange _iter14 : struct.segments)
        {
          _iter14.write(oprot);
        }
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, ScaleResponse struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      struct.status = com.emc.pravega.controller.stream.api.v1.ScaleStreamStatus.findByValue(iprot.readI32());
      struct.setStatusIsSet(true);
      {
        org.apache.thrift.protocol.TList _list15 = new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRUCT, iprot.readI32());
        struct.segments = new ArrayList<SegmentRange>(_list15.size);
        SegmentRange _elem16;
        for (int _i17 = 0; _i17 < _list15.size; ++_i17)
        {
          _elem16 = new SegmentRange();
          _elem16.read(iprot);
          struct.segments.add(_elem16);
        }
      }
      struct.setSegmentsIsSet(true);
    }
  }

}

