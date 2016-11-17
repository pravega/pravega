/**
 * Autogenerated by Thrift Compiler (0.9.3)
 * <p>
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *
 * @generated
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
@Generated(value = "Autogenerated by Thrift Compiler (0.9.3)", date = "2016-11-04")
public class NodeUri implements org.apache.thrift.TBase<NodeUri, NodeUri._Fields>, java.io.Serializable, Cloneable,
                                Comparable<NodeUri> {
    private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct(
            "NodeUri");

    private static final org.apache.thrift.protocol.TField ENDPOINT_FIELD_DESC = new org.apache.thrift.protocol.TField(
            "endpoint", org.apache.thrift.protocol.TType.STRING, (short) 1);
    private static final org.apache.thrift.protocol.TField PORT_FIELD_DESC = new org.apache.thrift.protocol.TField(
            "port", org.apache.thrift.protocol.TType.I32, (short) 2);

    private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>,
            SchemeFactory>();

    static {
        schemes.put(StandardScheme.class, new NodeUriStandardSchemeFactory());
        schemes.put(TupleScheme.class, new NodeUriTupleSchemeFactory());
    }

    private String endpoint; // required
    private int port; // required

    /**
     * The set of fields this struct contains, along with convenience methods for finding and manipulating them.
     */
    public enum _Fields implements org.apache.thrift.TFieldIdEnum {
        ENDPOINT((short) 1, "endpoint"),
        PORT((short) 2, "port");

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
            switch (fieldId) {
                case 1: // ENDPOINT
                    return ENDPOINT;
                case 2: // PORT
                    return PORT;
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
    private static final int __PORT_ISSET_ID = 0;
    private byte __isset_bitfield = 0;
    public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;

    static {
        Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift
                .meta_data.FieldMetaData>(
                _Fields.class);
        tmpMap.put(_Fields.ENDPOINT, new org.apache.thrift.meta_data.FieldMetaData("endpoint",
                org.apache.thrift.TFieldRequirementType.REQUIRED,
                new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
        tmpMap.put(_Fields.PORT,
                new org.apache.thrift.meta_data.FieldMetaData("port", org.apache.thrift.TFieldRequirementType.REQUIRED,
                        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I32)));
        metaDataMap = Collections.unmodifiableMap(tmpMap);
        org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(NodeUri.class, metaDataMap);
    }

    public NodeUri() {
    }

    public NodeUri(String endpoint, int port) {
        this();
        this.endpoint = endpoint;
        this.port = port;
        setPortIsSet(true);
    }

    /**
     * Performs a deep copy on <i>other</i>.
     */
    public NodeUri(NodeUri other) {
        __isset_bitfield = other.__isset_bitfield;
        if (other.isSetEndpoint()) {
            this.endpoint = other.endpoint;
        }
        this.port = other.port;
    }

    public NodeUri deepCopy() {
        return new NodeUri(this);
    }

    @Override
    public void clear() {
        this.endpoint = null;
        setPortIsSet(false);
        this.port = 0;
    }

    public String getEndpoint() {
        return this.endpoint;
    }

    public NodeUri setEndpoint(String endpoint) {
        this.endpoint = endpoint;
        return this;
    }

    public void unsetEndpoint() {
        this.endpoint = null;
    }

    /**
     * Returns true if field endpoint is set (has been assigned a value) and false otherwise
     */
    public boolean isSetEndpoint() {
        return this.endpoint != null;
    }

    public void setEndpointIsSet(boolean value) {
        if (!value) {
            this.endpoint = null;
        }
    }

    public int getPort() {
        return this.port;
    }

    public NodeUri setPort(int port) {
        this.port = port;
        setPortIsSet(true);
        return this;
    }

    public void unsetPort() {
        __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __PORT_ISSET_ID);
    }

    /**
     * Returns true if field port is set (has been assigned a value) and false otherwise
     */
    public boolean isSetPort() {
        return EncodingUtils.testBit(__isset_bitfield, __PORT_ISSET_ID);
    }

    public void setPortIsSet(boolean value) {
        __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __PORT_ISSET_ID, value);
    }

    public void setFieldValue(_Fields field, Object value) {
        switch (field) {
            case ENDPOINT:
                if (value == null) {
                    unsetEndpoint();
                } else {
                    setEndpoint((String) value);
                }
                break;

            case PORT:
                if (value == null) {
                    unsetPort();
                } else {
                    setPort((Integer) value);
                }
                break;

        }
    }

    public Object getFieldValue(_Fields field) {
        switch (field) {
            case ENDPOINT:
                return getEndpoint();

            case PORT:
                return getPort();

        }
        throw new IllegalStateException();
    }

    /**
     * Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise
     */
    public boolean isSet(_Fields field) {
        if (field == null) {
            throw new IllegalArgumentException();
        }

        switch (field) {
            case ENDPOINT:
                return isSetEndpoint();
            case PORT:
                return isSetPort();
        }
        throw new IllegalStateException();
    }

    @Override
    public boolean equals(Object that) {
        if (that == null) return false;
        if (that instanceof NodeUri) return this.equals((NodeUri) that);
        return false;
    }

    public boolean equals(NodeUri that) {
        if (that == null) return false;

        boolean this_present_endpoint = true && this.isSetEndpoint();
        boolean that_present_endpoint = true && that.isSetEndpoint();
        if (this_present_endpoint || that_present_endpoint) {
            if (!(this_present_endpoint && that_present_endpoint)) return false;
            if (!this.endpoint.equals(that.endpoint)) return false;
        }

        boolean this_present_port = true;
        boolean that_present_port = true;
        if (this_present_port || that_present_port) {
            if (!(this_present_port && that_present_port)) return false;
            if (this.port != that.port) return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        List<Object> list = new ArrayList<Object>();

        boolean present_endpoint = true && (isSetEndpoint());
        list.add(present_endpoint);
        if (present_endpoint) list.add(endpoint);

        boolean present_port = true;
        list.add(present_port);
        if (present_port) list.add(port);

        return list.hashCode();
    }

    @Override
    public int compareTo(NodeUri other) {
        if (!getClass().equals(other.getClass())) {
            return getClass().getName().compareTo(other.getClass().getName());
        }

        int lastComparison = 0;

        lastComparison = Boolean.valueOf(isSetEndpoint()).compareTo(other.isSetEndpoint());
        if (lastComparison != 0) {
            return lastComparison;
        }
        if (isSetEndpoint()) {
            lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.endpoint, other.endpoint);
            if (lastComparison != 0) {
                return lastComparison;
            }
        }
        lastComparison = Boolean.valueOf(isSetPort()).compareTo(other.isSetPort());
        if (lastComparison != 0) {
            return lastComparison;
        }
        if (isSetPort()) {
            lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.port, other.port);
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
        StringBuilder sb = new StringBuilder("NodeUri(");
        boolean first = true;

        sb.append("endpoint:");
        if (this.endpoint == null) {
            sb.append("null");
        } else {
            sb.append(this.endpoint);
        }
        first = false;
        if (!first) sb.append(", ");
        sb.append("port:");
        sb.append(this.port);
        first = false;
        sb.append(")");
        return sb.toString();
    }

    public void validate() throws org.apache.thrift.TException {
        // check for required fields
        if (endpoint == null) {
            throw new org.apache.thrift.protocol.TProtocolException(
                    "Required field 'endpoint' was not present! Struct: " + toString());
        }
        // alas, we cannot check 'port' because it's a primitive and you chose the non-beans generator.
        // check for sub-struct validity
    }

    private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
        try {
            write(new org.apache.thrift.protocol.TCompactProtocol(
                    new org.apache.thrift.transport.TIOStreamTransport(out)));
        } catch (org.apache.thrift.TException te) {
            throw new java.io.IOException(te);
        }
    }

    private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, ClassNotFoundException {
        try {
            // it doesn't seem like you should have to do this, but java serialization is wacky, and doesn't call the
            // default constructor.
            __isset_bitfield = 0;
            read(new org.apache.thrift.protocol.TCompactProtocol(
                    new org.apache.thrift.transport.TIOStreamTransport(in)));
        } catch (org.apache.thrift.TException te) {
            throw new java.io.IOException(te);
        }
    }

    private static class NodeUriStandardSchemeFactory implements SchemeFactory {
        public NodeUriStandardScheme getScheme() {
            return new NodeUriStandardScheme();
        }
    }

    private static class NodeUriStandardScheme extends StandardScheme<NodeUri> {

        public void read(org.apache.thrift.protocol.TProtocol iprot, NodeUri struct) throws org.apache.thrift
                .TException {
            org.apache.thrift.protocol.TField schemeField;
            iprot.readStructBegin();
            while (true) {
                schemeField = iprot.readFieldBegin();
                if (schemeField.type == org.apache.thrift.protocol.TType.STOP) {
                    break;
                }
                switch (schemeField.id) {
                    case 1: // ENDPOINT
                        if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
                            struct.endpoint = iprot.readString();
                            struct.setEndpointIsSet(true);
                        } else {
                            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
                        }
                        break;
                    case 2: // PORT
                        if (schemeField.type == org.apache.thrift.protocol.TType.I32) {
                            struct.port = iprot.readI32();
                            struct.setPortIsSet(true);
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
            if (!struct.isSetPort()) {
                throw new org.apache.thrift.protocol.TProtocolException(
                        "Required field 'port' was not found in serialized data! Struct: " + toString());
            }
            struct.validate();
        }

        public void write(org.apache.thrift.protocol.TProtocol oprot, NodeUri struct) throws org.apache.thrift
                .TException {
            struct.validate();

            oprot.writeStructBegin(STRUCT_DESC);
            if (struct.endpoint != null) {
                oprot.writeFieldBegin(ENDPOINT_FIELD_DESC);
                oprot.writeString(struct.endpoint);
                oprot.writeFieldEnd();
            }
            oprot.writeFieldBegin(PORT_FIELD_DESC);
            oprot.writeI32(struct.port);
            oprot.writeFieldEnd();
            oprot.writeFieldStop();
            oprot.writeStructEnd();
        }

    }

    private static class NodeUriTupleSchemeFactory implements SchemeFactory {
        public NodeUriTupleScheme getScheme() {
            return new NodeUriTupleScheme();
        }
    }

    private static class NodeUriTupleScheme extends TupleScheme<NodeUri> {

        @Override
        public void write(org.apache.thrift.protocol.TProtocol prot, NodeUri struct) throws org.apache.thrift
                .TException {
            TTupleProtocol oprot = (TTupleProtocol) prot;
            oprot.writeString(struct.endpoint);
            oprot.writeI32(struct.port);
        }

        @Override
        public void read(org.apache.thrift.protocol.TProtocol prot, NodeUri struct) throws org.apache.thrift
                .TException {
            TTupleProtocol iprot = (TTupleProtocol) prot;
            struct.endpoint = iprot.readString();
            struct.setEndpointIsSet(true);
            struct.port = iprot.readI32();
            struct.setPortIsSet(true);
        }
    }

}

