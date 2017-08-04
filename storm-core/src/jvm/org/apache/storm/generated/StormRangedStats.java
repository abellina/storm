/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * Autogenerated by Thrift Compiler (0.9.3)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package org.apache.storm.generated;

import org.apache.thrift.EncodingUtils;
import org.apache.thrift.protocol.TTupleProtocol;
import org.apache.thrift.scheme.IScheme;
import org.apache.thrift.scheme.SchemeFactory;
import org.apache.thrift.scheme.StandardScheme;
import org.apache.thrift.scheme.TupleScheme;

import javax.annotation.Generated;
import java.util.*;

@SuppressWarnings({"cast", "rawtypes", "serial", "unchecked"})
@Generated(value = "Autogenerated by Thrift Compiler (0.9.3)")
public class StormRangedStats implements org.apache.thrift.TBase<StormRangedStats, StormRangedStats._Fields>, java.io.Serializable, Cloneable, Comparable<StormRangedStats> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("StormRangedStats");

  private static final org.apache.thrift.protocol.TField START_TIME_FIELD_DESC = new org.apache.thrift.protocol.TField("start_time", org.apache.thrift.protocol.TType.I64, (short)1);
  private static final org.apache.thrift.protocol.TField END_TIME_FIELD_DESC = new org.apache.thrift.protocol.TField("end_time", org.apache.thrift.protocol.TType.I64, (short)2);
  private static final org.apache.thrift.protocol.TField VALUES_FIELD_DESC = new org.apache.thrift.protocol.TField("values", org.apache.thrift.protocol.TType.MAP, (short)4);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new StormRangedStatsStandardSchemeFactory());
    schemes.put(TupleScheme.class, new StormRangedStatsTupleSchemeFactory());
  }

  private long start_time; // required
  private long end_time; // required
  private Map<String,Double> values; // optional

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    START_TIME((short)1, "start_time"),
    END_TIME((short)2, "end_time"),
    VALUES((short)4, "values");

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
        case 1: // START_TIME
          return START_TIME;
        case 2: // END_TIME
          return END_TIME;
        case 4: // VALUES
          return VALUES;
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
  private static final int __START_TIME_ISSET_ID = 0;
  private static final int __END_TIME_ISSET_ID = 1;
  private byte __isset_bitfield = 0;
  private static final _Fields optionals[] = {_Fields.VALUES};
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.START_TIME, new org.apache.thrift.meta_data.FieldMetaData("start_time", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64)));
    tmpMap.put(_Fields.END_TIME, new org.apache.thrift.meta_data.FieldMetaData("end_time", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64)));
    tmpMap.put(_Fields.VALUES, new org.apache.thrift.meta_data.FieldMetaData("values", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.MapMetaData(org.apache.thrift.protocol.TType.MAP, 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING), 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.DOUBLE))));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(StormRangedStats.class, metaDataMap);
  }

  public StormRangedStats() {
  }

  public StormRangedStats(
    long start_time,
    long end_time)
  {
    this();
    this.start_time = start_time;
    set_start_time_isSet(true);
    this.end_time = end_time;
    set_end_time_isSet(true);
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public StormRangedStats(StormRangedStats other) {
    __isset_bitfield = other.__isset_bitfield;
    this.start_time = other.start_time;
    this.end_time = other.end_time;
    if (other.is_set_values()) {
      Map<String,Double> __this__values = new HashMap<String,Double>(other.values);
      this.values = __this__values;
    }
  }

  public StormRangedStats deepCopy() {
    return new StormRangedStats(this);
  }

  @Override
  public void clear() {
    set_start_time_isSet(false);
    this.start_time = 0;
    set_end_time_isSet(false);
    this.end_time = 0;
    this.values = null;
  }

  public long get_start_time() {
    return this.start_time;
  }

  public void set_start_time(long start_time) {
    this.start_time = start_time;
    set_start_time_isSet(true);
  }

  public void unset_start_time() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __START_TIME_ISSET_ID);
  }

  /** Returns true if field start_time is set (has been assigned a value) and false otherwise */
  public boolean is_set_start_time() {
    return EncodingUtils.testBit(__isset_bitfield, __START_TIME_ISSET_ID);
  }

  public void set_start_time_isSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __START_TIME_ISSET_ID, value);
  }

  public long get_end_time() {
    return this.end_time;
  }

  public void set_end_time(long end_time) {
    this.end_time = end_time;
    set_end_time_isSet(true);
  }

  public void unset_end_time() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __END_TIME_ISSET_ID);
  }

  /** Returns true if field end_time is set (has been assigned a value) and false otherwise */
  public boolean is_set_end_time() {
    return EncodingUtils.testBit(__isset_bitfield, __END_TIME_ISSET_ID);
  }

  public void set_end_time_isSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __END_TIME_ISSET_ID, value);
  }

  public int get_values_size() {
    return (this.values == null) ? 0 : this.values.size();
  }

  public void put_to_values(String key, double val) {
    if (this.values == null) {
      this.values = new HashMap<String,Double>();
    }
    this.values.put(key, val);
  }

  public Map<String,Double> get_values() {
    return this.values;
  }

  public void set_values(Map<String,Double> values) {
    this.values = values;
  }

  public void unset_values() {
    this.values = null;
  }

  /** Returns true if field values is set (has been assigned a value) and false otherwise */
  public boolean is_set_values() {
    return this.values != null;
  }

  public void set_values_isSet(boolean value) {
    if (!value) {
      this.values = null;
    }
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case START_TIME:
      if (value == null) {
        unset_start_time();
      } else {
        set_start_time((Long)value);
      }
      break;

    case END_TIME:
      if (value == null) {
        unset_end_time();
      } else {
        set_end_time((Long)value);
      }
      break;

    case VALUES:
      if (value == null) {
        unset_values();
      } else {
        set_values((Map<String,Double>)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case START_TIME:
      return get_start_time();

    case END_TIME:
      return get_end_time();

    case VALUES:
      return get_values();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case START_TIME:
      return is_set_start_time();
    case END_TIME:
      return is_set_end_time();
    case VALUES:
      return is_set_values();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof StormRangedStats)
      return this.equals((StormRangedStats)that);
    return false;
  }

  public boolean equals(StormRangedStats that) {
    if (that == null)
      return false;

    boolean this_present_start_time = true;
    boolean that_present_start_time = true;
    if (this_present_start_time || that_present_start_time) {
      if (!(this_present_start_time && that_present_start_time))
        return false;
      if (this.start_time != that.start_time)
        return false;
    }

    boolean this_present_end_time = true;
    boolean that_present_end_time = true;
    if (this_present_end_time || that_present_end_time) {
      if (!(this_present_end_time && that_present_end_time))
        return false;
      if (this.end_time != that.end_time)
        return false;
    }

    boolean this_present_values = true && this.is_set_values();
    boolean that_present_values = true && that.is_set_values();
    if (this_present_values || that_present_values) {
      if (!(this_present_values && that_present_values))
        return false;
      if (!this.values.equals(that.values))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    List<Object> list = new ArrayList<Object>();

    boolean present_start_time = true;
    list.add(present_start_time);
    if (present_start_time)
      list.add(start_time);

    boolean present_end_time = true;
    list.add(present_end_time);
    if (present_end_time)
      list.add(end_time);

    boolean present_values = true && (is_set_values());
    list.add(present_values);
    if (present_values)
      list.add(values);

    return list.hashCode();
  }

  @Override
  public int compareTo(StormRangedStats other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = Boolean.valueOf(is_set_start_time()).compareTo(other.is_set_start_time());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (is_set_start_time()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.start_time, other.start_time);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(is_set_end_time()).compareTo(other.is_set_end_time());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (is_set_end_time()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.end_time, other.end_time);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(is_set_values()).compareTo(other.is_set_values());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (is_set_values()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.values, other.values);
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
    StringBuilder sb = new StringBuilder("StormRangedStats(");
    boolean first = true;

    sb.append("start_time:");
    sb.append(this.start_time);
    first = false;
    if (!first) sb.append(", ");
    sb.append("end_time:");
    sb.append(this.end_time);
    first = false;
    if (is_set_values()) {
      if (!first) sb.append(", ");
      sb.append("values:");
      if (this.values == null) {
        sb.append("null");
      } else {
        sb.append(this.values);
      }
      first = false;
    }
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
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

  private static class StormRangedStatsStandardSchemeFactory implements SchemeFactory {
    public StormRangedStatsStandardScheme getScheme() {
      return new StormRangedStatsStandardScheme();
    }
  }

  private static class StormRangedStatsStandardScheme extends StandardScheme<StormRangedStats> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, StormRangedStats struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // START_TIME
            if (schemeField.type == org.apache.thrift.protocol.TType.I64) {
              struct.start_time = iprot.readI64();
              struct.set_start_time_isSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // END_TIME
            if (schemeField.type == org.apache.thrift.protocol.TType.I64) {
              struct.end_time = iprot.readI64();
              struct.set_end_time_isSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 4: // VALUES
            if (schemeField.type == org.apache.thrift.protocol.TType.MAP) {
              {
                org.apache.thrift.protocol.TMap _map908 = iprot.readMapBegin();
                struct.values = new HashMap<String,Double>(2*_map908.size);
                String _key909;
                double _val910;
                for (int _i911 = 0; _i911 < _map908.size; ++_i911)
                {
                  _key909 = iprot.readString();
                  _val910 = iprot.readDouble();
                  struct.values.put(_key909, _val910);
                }
                iprot.readMapEnd();
              }
              struct.set_values_isSet(true);
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
      struct.validate();
    }

    public void write(org.apache.thrift.protocol.TProtocol oprot, StormRangedStats struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      oprot.writeFieldBegin(START_TIME_FIELD_DESC);
      oprot.writeI64(struct.start_time);
      oprot.writeFieldEnd();
      oprot.writeFieldBegin(END_TIME_FIELD_DESC);
      oprot.writeI64(struct.end_time);
      oprot.writeFieldEnd();
      if (struct.values != null) {
        if (struct.is_set_values()) {
          oprot.writeFieldBegin(VALUES_FIELD_DESC);
          {
            oprot.writeMapBegin(new org.apache.thrift.protocol.TMap(org.apache.thrift.protocol.TType.STRING, org.apache.thrift.protocol.TType.DOUBLE, struct.values.size()));
            for (Map.Entry<String, Double> _iter912 : struct.values.entrySet())
            {
              oprot.writeString(_iter912.getKey());
              oprot.writeDouble(_iter912.getValue());
            }
            oprot.writeMapEnd();
          }
          oprot.writeFieldEnd();
        }
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class StormRangedStatsTupleSchemeFactory implements SchemeFactory {
    public StormRangedStatsTupleScheme getScheme() {
      return new StormRangedStatsTupleScheme();
    }
  }

  private static class StormRangedStatsTupleScheme extends TupleScheme<StormRangedStats> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, StormRangedStats struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      BitSet optionals = new BitSet();
      if (struct.is_set_start_time()) {
        optionals.set(0);
      }
      if (struct.is_set_end_time()) {
        optionals.set(1);
      }
      if (struct.is_set_values()) {
        optionals.set(2);
      }
      oprot.writeBitSet(optionals, 3);
      if (struct.is_set_start_time()) {
        oprot.writeI64(struct.start_time);
      }
      if (struct.is_set_end_time()) {
        oprot.writeI64(struct.end_time);
      }
      if (struct.is_set_values()) {
        {
          oprot.writeI32(struct.values.size());
          for (Map.Entry<String, Double> _iter913 : struct.values.entrySet())
          {
            oprot.writeString(_iter913.getKey());
            oprot.writeDouble(_iter913.getValue());
          }
        }
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, StormRangedStats struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      BitSet incoming = iprot.readBitSet(3);
      if (incoming.get(0)) {
        struct.start_time = iprot.readI64();
        struct.set_start_time_isSet(true);
      }
      if (incoming.get(1)) {
        struct.end_time = iprot.readI64();
        struct.set_end_time_isSet(true);
      }
      if (incoming.get(2)) {
        {
          org.apache.thrift.protocol.TMap _map914 = new org.apache.thrift.protocol.TMap(org.apache.thrift.protocol.TType.STRING, org.apache.thrift.protocol.TType.DOUBLE, iprot.readI32());
          struct.values = new HashMap<String,Double>(2*_map914.size);
          String _key915;
          double _val916;
          for (int _i917 = 0; _i917 < _map914.size; ++_i917)
          {
            _key915 = iprot.readString();
            _val916 = iprot.readDouble();
            struct.values.put(_key915, _val916);
          }
        }
        struct.set_values_isSet(true);
      }
    }
  }

}

