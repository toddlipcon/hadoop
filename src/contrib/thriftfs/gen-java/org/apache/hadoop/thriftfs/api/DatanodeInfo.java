/**
 * Autogenerated by Thrift
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 */
package org.apache.hadoop.thriftfs.api;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
import java.util.Collections;

import org.apache.thrift.*;
import org.apache.thrift.meta_data.*;
import org.apache.thrift.protocol.*;

/**
 * Information and state of a data node.
 * 
 * Modelled after org.apache.hadoop.hdfs.protocol.DatanodeInfo
 */
public class DatanodeInfo implements TBase, java.io.Serializable, Cloneable {
  private static final TStruct STRUCT_DESC = new TStruct("DatanodeInfo");
  private static final TField NAME_FIELD_DESC = new TField("name", TType.STRING, (short)1);
  private static final TField STORAGE_ID_FIELD_DESC = new TField("storageID", TType.STRING, (short)2);
  private static final TField HOST_FIELD_DESC = new TField("host", TType.STRING, (short)3);
  private static final TField THRIFT_PORT_FIELD_DESC = new TField("thriftPort", TType.I32, (short)4);
  private static final TField CAPACITY_FIELD_DESC = new TField("capacity", TType.I64, (short)5);
  private static final TField DFS_USED_FIELD_DESC = new TField("dfsUsed", TType.I64, (short)6);
  private static final TField REMAINING_FIELD_DESC = new TField("remaining", TType.I64, (short)7);
  private static final TField XCEIVER_COUNT_FIELD_DESC = new TField("xceiverCount", TType.I32, (short)8);
  private static final TField STATE_FIELD_DESC = new TField("state", TType.I32, (short)9);

  /**
   * HDFS name of the datanode (equals to <host>:<datanode port>)
   */
  public String name;
  public static final int NAME = 1;
  /**
   * Unique ID within a HDFS cluster
   */
  public String storageID;
  public static final int STORAGEID = 2;
  /**
   * Host name of the Thrift server socket.
   */
  public String host;
  public static final int HOST = 3;
  /**
   * Port number of the Thrift server socket, or UNKNOWN_THRIFT_PORT
   * if the Thrift port for this datanode is not known.
   */
  public int thriftPort;
  public static final int THRIFTPORT = 4;
  /**
   * Raw capacity of the data node (in bytes).
   */
  public long capacity;
  public static final int CAPACITY = 5;
  /**
   * Space used by the data node (in bytes).
   */
  public long dfsUsed;
  public static final int DFSUSED = 6;
  /**
   * Raw free space in the data node (in bytes).
   */
  public long remaining;
  public static final int REMAINING = 7;
  /**
   * Number of active connections to the data node.
   */
  public int xceiverCount;
  public static final int XCEIVERCOUNT = 8;
  /**
   * State of this data node.
   */
  public int state;
  public static final int STATE = 9;

  private final Isset __isset = new Isset();
  private static final class Isset implements java.io.Serializable {
    public boolean thriftPort = false;
    public boolean capacity = false;
    public boolean dfsUsed = false;
    public boolean remaining = false;
    public boolean xceiverCount = false;
    public boolean state = false;
  }

  public static final Map<Integer, FieldMetaData> metaDataMap = Collections.unmodifiableMap(new HashMap<Integer, FieldMetaData>() {{
    put(NAME, new FieldMetaData("name", TFieldRequirementType.DEFAULT, 
        new FieldValueMetaData(TType.STRING)));
    put(STORAGEID, new FieldMetaData("storageID", TFieldRequirementType.DEFAULT, 
        new FieldValueMetaData(TType.STRING)));
    put(HOST, new FieldMetaData("host", TFieldRequirementType.DEFAULT, 
        new FieldValueMetaData(TType.STRING)));
    put(THRIFTPORT, new FieldMetaData("thriftPort", TFieldRequirementType.DEFAULT, 
        new FieldValueMetaData(TType.I32)));
    put(CAPACITY, new FieldMetaData("capacity", TFieldRequirementType.DEFAULT, 
        new FieldValueMetaData(TType.I64)));
    put(DFSUSED, new FieldMetaData("dfsUsed", TFieldRequirementType.DEFAULT, 
        new FieldValueMetaData(TType.I64)));
    put(REMAINING, new FieldMetaData("remaining", TFieldRequirementType.DEFAULT, 
        new FieldValueMetaData(TType.I64)));
    put(XCEIVERCOUNT, new FieldMetaData("xceiverCount", TFieldRequirementType.DEFAULT, 
        new FieldValueMetaData(TType.I32)));
    put(STATE, new FieldMetaData("state", TFieldRequirementType.DEFAULT, 
        new FieldValueMetaData(TType.I32)));
  }});

  static {
    FieldMetaData.addStructMetaDataMap(DatanodeInfo.class, metaDataMap);
  }

  public DatanodeInfo() {
  }

  public DatanodeInfo(
    String name,
    String storageID,
    String host,
    int thriftPort,
    long capacity,
    long dfsUsed,
    long remaining,
    int xceiverCount,
    int state)
  {
    this();
    this.name = name;
    this.storageID = storageID;
    this.host = host;
    this.thriftPort = thriftPort;
    this.__isset.thriftPort = true;
    this.capacity = capacity;
    this.__isset.capacity = true;
    this.dfsUsed = dfsUsed;
    this.__isset.dfsUsed = true;
    this.remaining = remaining;
    this.__isset.remaining = true;
    this.xceiverCount = xceiverCount;
    this.__isset.xceiverCount = true;
    this.state = state;
    this.__isset.state = true;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public DatanodeInfo(DatanodeInfo other) {
    if (other.isSetName()) {
      this.name = other.name;
    }
    if (other.isSetStorageID()) {
      this.storageID = other.storageID;
    }
    if (other.isSetHost()) {
      this.host = other.host;
    }
    __isset.thriftPort = other.__isset.thriftPort;
    this.thriftPort = other.thriftPort;
    __isset.capacity = other.__isset.capacity;
    this.capacity = other.capacity;
    __isset.dfsUsed = other.__isset.dfsUsed;
    this.dfsUsed = other.dfsUsed;
    __isset.remaining = other.__isset.remaining;
    this.remaining = other.remaining;
    __isset.xceiverCount = other.__isset.xceiverCount;
    this.xceiverCount = other.xceiverCount;
    __isset.state = other.__isset.state;
    this.state = other.state;
  }

  @Override
  public DatanodeInfo clone() {
    return new DatanodeInfo(this);
  }

  /**
   * HDFS name of the datanode (equals to <host>:<datanode port>)
   */
  public String getName() {
    return this.name;
  }

  /**
   * HDFS name of the datanode (equals to <host>:<datanode port>)
   */
  public void setName(String name) {
    this.name = name;
  }

  public void unsetName() {
    this.name = null;
  }

  // Returns true if field name is set (has been asigned a value) and false otherwise
  public boolean isSetName() {
    return this.name != null;
  }

  public void setNameIsSet(boolean value) {
    if (!value) {
      this.name = null;
    }
  }

  /**
   * Unique ID within a HDFS cluster
   */
  public String getStorageID() {
    return this.storageID;
  }

  /**
   * Unique ID within a HDFS cluster
   */
  public void setStorageID(String storageID) {
    this.storageID = storageID;
  }

  public void unsetStorageID() {
    this.storageID = null;
  }

  // Returns true if field storageID is set (has been asigned a value) and false otherwise
  public boolean isSetStorageID() {
    return this.storageID != null;
  }

  public void setStorageIDIsSet(boolean value) {
    if (!value) {
      this.storageID = null;
    }
  }

  /**
   * Host name of the Thrift server socket.
   */
  public String getHost() {
    return this.host;
  }

  /**
   * Host name of the Thrift server socket.
   */
  public void setHost(String host) {
    this.host = host;
  }

  public void unsetHost() {
    this.host = null;
  }

  // Returns true if field host is set (has been asigned a value) and false otherwise
  public boolean isSetHost() {
    return this.host != null;
  }

  public void setHostIsSet(boolean value) {
    if (!value) {
      this.host = null;
    }
  }

  /**
   * Port number of the Thrift server socket, or UNKNOWN_THRIFT_PORT
   * if the Thrift port for this datanode is not known.
   */
  public int getThriftPort() {
    return this.thriftPort;
  }

  /**
   * Port number of the Thrift server socket, or UNKNOWN_THRIFT_PORT
   * if the Thrift port for this datanode is not known.
   */
  public void setThriftPort(int thriftPort) {
    this.thriftPort = thriftPort;
    this.__isset.thriftPort = true;
  }

  public void unsetThriftPort() {
    this.__isset.thriftPort = false;
  }

  // Returns true if field thriftPort is set (has been asigned a value) and false otherwise
  public boolean isSetThriftPort() {
    return this.__isset.thriftPort;
  }

  public void setThriftPortIsSet(boolean value) {
    this.__isset.thriftPort = value;
  }

  /**
   * Raw capacity of the data node (in bytes).
   */
  public long getCapacity() {
    return this.capacity;
  }

  /**
   * Raw capacity of the data node (in bytes).
   */
  public void setCapacity(long capacity) {
    this.capacity = capacity;
    this.__isset.capacity = true;
  }

  public void unsetCapacity() {
    this.__isset.capacity = false;
  }

  // Returns true if field capacity is set (has been asigned a value) and false otherwise
  public boolean isSetCapacity() {
    return this.__isset.capacity;
  }

  public void setCapacityIsSet(boolean value) {
    this.__isset.capacity = value;
  }

  /**
   * Space used by the data node (in bytes).
   */
  public long getDfsUsed() {
    return this.dfsUsed;
  }

  /**
   * Space used by the data node (in bytes).
   */
  public void setDfsUsed(long dfsUsed) {
    this.dfsUsed = dfsUsed;
    this.__isset.dfsUsed = true;
  }

  public void unsetDfsUsed() {
    this.__isset.dfsUsed = false;
  }

  // Returns true if field dfsUsed is set (has been asigned a value) and false otherwise
  public boolean isSetDfsUsed() {
    return this.__isset.dfsUsed;
  }

  public void setDfsUsedIsSet(boolean value) {
    this.__isset.dfsUsed = value;
  }

  /**
   * Raw free space in the data node (in bytes).
   */
  public long getRemaining() {
    return this.remaining;
  }

  /**
   * Raw free space in the data node (in bytes).
   */
  public void setRemaining(long remaining) {
    this.remaining = remaining;
    this.__isset.remaining = true;
  }

  public void unsetRemaining() {
    this.__isset.remaining = false;
  }

  // Returns true if field remaining is set (has been asigned a value) and false otherwise
  public boolean isSetRemaining() {
    return this.__isset.remaining;
  }

  public void setRemainingIsSet(boolean value) {
    this.__isset.remaining = value;
  }

  /**
   * Number of active connections to the data node.
   */
  public int getXceiverCount() {
    return this.xceiverCount;
  }

  /**
   * Number of active connections to the data node.
   */
  public void setXceiverCount(int xceiverCount) {
    this.xceiverCount = xceiverCount;
    this.__isset.xceiverCount = true;
  }

  public void unsetXceiverCount() {
    this.__isset.xceiverCount = false;
  }

  // Returns true if field xceiverCount is set (has been asigned a value) and false otherwise
  public boolean isSetXceiverCount() {
    return this.__isset.xceiverCount;
  }

  public void setXceiverCountIsSet(boolean value) {
    this.__isset.xceiverCount = value;
  }

  /**
   * State of this data node.
   */
  public int getState() {
    return this.state;
  }

  /**
   * State of this data node.
   */
  public void setState(int state) {
    this.state = state;
    this.__isset.state = true;
  }

  public void unsetState() {
    this.__isset.state = false;
  }

  // Returns true if field state is set (has been asigned a value) and false otherwise
  public boolean isSetState() {
    return this.__isset.state;
  }

  public void setStateIsSet(boolean value) {
    this.__isset.state = value;
  }

  public void setFieldValue(int fieldID, Object value) {
    switch (fieldID) {
    case NAME:
      if (value == null) {
        unsetName();
      } else {
        setName((String)value);
      }
      break;

    case STORAGEID:
      if (value == null) {
        unsetStorageID();
      } else {
        setStorageID((String)value);
      }
      break;

    case HOST:
      if (value == null) {
        unsetHost();
      } else {
        setHost((String)value);
      }
      break;

    case THRIFTPORT:
      if (value == null) {
        unsetThriftPort();
      } else {
        setThriftPort((Integer)value);
      }
      break;

    case CAPACITY:
      if (value == null) {
        unsetCapacity();
      } else {
        setCapacity((Long)value);
      }
      break;

    case DFSUSED:
      if (value == null) {
        unsetDfsUsed();
      } else {
        setDfsUsed((Long)value);
      }
      break;

    case REMAINING:
      if (value == null) {
        unsetRemaining();
      } else {
        setRemaining((Long)value);
      }
      break;

    case XCEIVERCOUNT:
      if (value == null) {
        unsetXceiverCount();
      } else {
        setXceiverCount((Integer)value);
      }
      break;

    case STATE:
      if (value == null) {
        unsetState();
      } else {
        setState((Integer)value);
      }
      break;

    default:
      throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
    }
  }

  public Object getFieldValue(int fieldID) {
    switch (fieldID) {
    case NAME:
      return getName();

    case STORAGEID:
      return getStorageID();

    case HOST:
      return getHost();

    case THRIFTPORT:
      return new Integer(getThriftPort());

    case CAPACITY:
      return new Long(getCapacity());

    case DFSUSED:
      return new Long(getDfsUsed());

    case REMAINING:
      return new Long(getRemaining());

    case XCEIVERCOUNT:
      return new Integer(getXceiverCount());

    case STATE:
      return getState();

    default:
      throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
    }
  }

  // Returns true if field corresponding to fieldID is set (has been asigned a value) and false otherwise
  public boolean isSet(int fieldID) {
    switch (fieldID) {
    case NAME:
      return isSetName();
    case STORAGEID:
      return isSetStorageID();
    case HOST:
      return isSetHost();
    case THRIFTPORT:
      return isSetThriftPort();
    case CAPACITY:
      return isSetCapacity();
    case DFSUSED:
      return isSetDfsUsed();
    case REMAINING:
      return isSetRemaining();
    case XCEIVERCOUNT:
      return isSetXceiverCount();
    case STATE:
      return isSetState();
    default:
      throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
    }
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof DatanodeInfo)
      return this.equals((DatanodeInfo)that);
    return false;
  }

  public boolean equals(DatanodeInfo that) {
    if (that == null)
      return false;

    boolean this_present_name = true && this.isSetName();
    boolean that_present_name = true && that.isSetName();
    if (this_present_name || that_present_name) {
      if (!(this_present_name && that_present_name))
        return false;
      if (!this.name.equals(that.name))
        return false;
    }

    boolean this_present_storageID = true && this.isSetStorageID();
    boolean that_present_storageID = true && that.isSetStorageID();
    if (this_present_storageID || that_present_storageID) {
      if (!(this_present_storageID && that_present_storageID))
        return false;
      if (!this.storageID.equals(that.storageID))
        return false;
    }

    boolean this_present_host = true && this.isSetHost();
    boolean that_present_host = true && that.isSetHost();
    if (this_present_host || that_present_host) {
      if (!(this_present_host && that_present_host))
        return false;
      if (!this.host.equals(that.host))
        return false;
    }

    boolean this_present_thriftPort = true;
    boolean that_present_thriftPort = true;
    if (this_present_thriftPort || that_present_thriftPort) {
      if (!(this_present_thriftPort && that_present_thriftPort))
        return false;
      if (this.thriftPort != that.thriftPort)
        return false;
    }

    boolean this_present_capacity = true;
    boolean that_present_capacity = true;
    if (this_present_capacity || that_present_capacity) {
      if (!(this_present_capacity && that_present_capacity))
        return false;
      if (this.capacity != that.capacity)
        return false;
    }

    boolean this_present_dfsUsed = true;
    boolean that_present_dfsUsed = true;
    if (this_present_dfsUsed || that_present_dfsUsed) {
      if (!(this_present_dfsUsed && that_present_dfsUsed))
        return false;
      if (this.dfsUsed != that.dfsUsed)
        return false;
    }

    boolean this_present_remaining = true;
    boolean that_present_remaining = true;
    if (this_present_remaining || that_present_remaining) {
      if (!(this_present_remaining && that_present_remaining))
        return false;
      if (this.remaining != that.remaining)
        return false;
    }

    boolean this_present_xceiverCount = true;
    boolean that_present_xceiverCount = true;
    if (this_present_xceiverCount || that_present_xceiverCount) {
      if (!(this_present_xceiverCount && that_present_xceiverCount))
        return false;
      if (this.xceiverCount != that.xceiverCount)
        return false;
    }

    boolean this_present_state = true;
    boolean that_present_state = true;
    if (this_present_state || that_present_state) {
      if (!(this_present_state && that_present_state))
        return false;
      if (this.state != that.state)
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return 0;
  }

  public void read(TProtocol iprot) throws TException {
    TField field;
    iprot.readStructBegin();
    while (true)
    {
      field = iprot.readFieldBegin();
      if (field.type == TType.STOP) { 
        break;
      }
      switch (field.id)
      {
        case NAME:
          if (field.type == TType.STRING) {
            this.name = iprot.readString();
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case STORAGEID:
          if (field.type == TType.STRING) {
            this.storageID = iprot.readString();
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case HOST:
          if (field.type == TType.STRING) {
            this.host = iprot.readString();
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case THRIFTPORT:
          if (field.type == TType.I32) {
            this.thriftPort = iprot.readI32();
            this.__isset.thriftPort = true;
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case CAPACITY:
          if (field.type == TType.I64) {
            this.capacity = iprot.readI64();
            this.__isset.capacity = true;
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case DFSUSED:
          if (field.type == TType.I64) {
            this.dfsUsed = iprot.readI64();
            this.__isset.dfsUsed = true;
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case REMAINING:
          if (field.type == TType.I64) {
            this.remaining = iprot.readI64();
            this.__isset.remaining = true;
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case XCEIVERCOUNT:
          if (field.type == TType.I32) {
            this.xceiverCount = iprot.readI32();
            this.__isset.xceiverCount = true;
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case STATE:
          if (field.type == TType.I32) {
            this.state = iprot.readI32();
            this.__isset.state = true;
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        default:
          TProtocolUtil.skip(iprot, field.type);
          break;
      }
      iprot.readFieldEnd();
    }
    iprot.readStructEnd();


    // check for required fields of primitive type, which can't be checked in the validate method
    validate();
  }

  public void write(TProtocol oprot) throws TException {
    validate();

    oprot.writeStructBegin(STRUCT_DESC);
    if (this.name != null) {
      oprot.writeFieldBegin(NAME_FIELD_DESC);
      oprot.writeString(this.name);
      oprot.writeFieldEnd();
    }
    if (this.storageID != null) {
      oprot.writeFieldBegin(STORAGE_ID_FIELD_DESC);
      oprot.writeString(this.storageID);
      oprot.writeFieldEnd();
    }
    if (this.host != null) {
      oprot.writeFieldBegin(HOST_FIELD_DESC);
      oprot.writeString(this.host);
      oprot.writeFieldEnd();
    }
    oprot.writeFieldBegin(THRIFT_PORT_FIELD_DESC);
    oprot.writeI32(this.thriftPort);
    oprot.writeFieldEnd();
    oprot.writeFieldBegin(CAPACITY_FIELD_DESC);
    oprot.writeI64(this.capacity);
    oprot.writeFieldEnd();
    oprot.writeFieldBegin(DFS_USED_FIELD_DESC);
    oprot.writeI64(this.dfsUsed);
    oprot.writeFieldEnd();
    oprot.writeFieldBegin(REMAINING_FIELD_DESC);
    oprot.writeI64(this.remaining);
    oprot.writeFieldEnd();
    oprot.writeFieldBegin(XCEIVER_COUNT_FIELD_DESC);
    oprot.writeI32(this.xceiverCount);
    oprot.writeFieldEnd();
    oprot.writeFieldBegin(STATE_FIELD_DESC);
    oprot.writeI32(this.state);
    oprot.writeFieldEnd();
    oprot.writeFieldStop();
    oprot.writeStructEnd();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("DatanodeInfo(");
    boolean first = true;

    sb.append("name:");
    if (this.name == null) {
      sb.append("null");
    } else {
      sb.append(this.name);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("storageID:");
    if (this.storageID == null) {
      sb.append("null");
    } else {
      sb.append(this.storageID);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("host:");
    if (this.host == null) {
      sb.append("null");
    } else {
      sb.append(this.host);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("thriftPort:");
    sb.append(this.thriftPort);
    first = false;
    if (!first) sb.append(", ");
    sb.append("capacity:");
    sb.append(this.capacity);
    first = false;
    if (!first) sb.append(", ");
    sb.append("dfsUsed:");
    sb.append(this.dfsUsed);
    first = false;
    if (!first) sb.append(", ");
    sb.append("remaining:");
    sb.append(this.remaining);
    first = false;
    if (!first) sb.append(", ");
    sb.append("xceiverCount:");
    sb.append(this.xceiverCount);
    first = false;
    if (!first) sb.append(", ");
    sb.append("state:");
    String state_name = DatanodeState.VALUES_TO_NAMES.get(this.state);
    if (state_name != null) {
      sb.append(state_name);
      sb.append(" (");
    }
    sb.append(this.state);
    if (state_name != null) {
      sb.append(")");
    }
    first = false;
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws TException {
    // check for required fields
    // check that fields of type enum have valid values
    if (isSetState() && !DatanodeState.VALID_VALUES.contains(state)){
      throw new TProtocolException("The field 'state' has been assigned the invalid value " + state);
    }
  }

}

