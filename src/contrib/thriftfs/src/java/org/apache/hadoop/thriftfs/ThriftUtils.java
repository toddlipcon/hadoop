/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.thriftfs;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.thriftfs.api.Block;
import org.apache.hadoop.thriftfs.api.Constants;
import org.apache.hadoop.thriftfs.api.DatanodeInfo;
import org.apache.hadoop.thriftfs.api.IOException;
import org.apache.hadoop.thriftfs.api.Namenode;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

public class ThriftUtils {
  
  static final Log LOG = LogFactory.getLog(ThriftUtils.class);

  public static LocatedBlock fromThrift(Block block) {
    if (block == null) {
      return null;
    }

    org.apache.hadoop.hdfs.protocol.Block b = new org.apache.hadoop.hdfs.protocol.Block(
        block.blockId, block.numBytes, block.genStamp);

    int n = block.nodes.size();
    org.apache.hadoop.hdfs.protocol.DatanodeInfo[] nodes =
        new org.apache.hadoop.hdfs.protocol.DatanodeInfo[n];
    for (int i = 0; i < n; ++i) {
      nodes[i] = fromThrift(block.nodes.get(0));
    }

    return new LocatedBlock(b, nodes);
  }

  public static Block toThrift(LocatedBlock block, String path,
      Map<String, Integer> thriftPorts) {
    if (block == null) {
      return new Block();
    }

    List<DatanodeInfo> nodes = new ArrayList<DatanodeInfo>();
    for (org.apache.hadoop.hdfs.protocol.DatanodeInfo n: block.getLocations()) {
      DatanodeInfo node = toThrift(n, thriftPorts); 
      if (node.getThriftPort() != Constants.UNKNOWN_THRIFT_PORT) {
        nodes.add(node);
      }
    }

    org.apache.hadoop.hdfs.protocol.Block b = block.getBlock();
    return new Block(b.getBlockId(), path, b.getNumBytes(),
        b.getGenerationStamp(), nodes);
  }

  public static org.apache.hadoop.hdfs.protocol.DatanodeInfo fromThrift(
      DatanodeInfo node) {
    if (node == null) {
      return null;
    }

    org.apache.hadoop.hdfs.protocol.DatanodeInfo ret =
        new org.apache.hadoop.hdfs.protocol.DatanodeInfo();
    ret.name = node.name;
    ret.storageID = node.storageID;
    ret.setCapacity(node.capacity);
    ret.setHostName(node.host);
    ret.setXceiverCount(node.xceiverCount);
    ret.setRemaining(node.remaining);
    if (node.state == Constants.DECOMMISSIONED) {
      ret.setDecommissioned();
    }
    return ret;
  }

  public static DatanodeInfo toThrift(
      org.apache.hadoop.hdfs.protocol.DatanodeInfo node,
      Map<String, Integer> thriftPorts) {
    if (node == null) {
      return new DatanodeInfo();
    }

    DatanodeInfo ret = new DatanodeInfo();
    ret.name = node.name;
    ret.storageID = node.storageID;
    ret.host = node.getHost();
    Integer p = thriftPorts.get(node.name);
    if (p == null) {
      LOG.warn("Unknown Thrift port for datanode " + node.name);
      ret.thriftPort = Constants.UNKNOWN_THRIFT_PORT;
    } else {
      ret.thriftPort = p.intValue();
    }
    ret.capacity = node.getCapacity();
    ret.dfsUsed = node.getDfsUsed();
    ret.remaining = node.getRemaining();
    ret.xceiverCount = node.getXceiverCount();
    ret.state = node.isDecommissioned() ? Constants.DECOMMISSIONED :
        node.isDecommissionInProgress() ? Constants.DECOMMISSION_INPROGRESS :
        Constants.NORMAL_STATE;
    return ret;
  }

  public static IOException toThrift(Throwable t) {
    if (t == null) {
      return new IOException();
    }

    IOException ret = new IOException();
    ret.msg = t.getMessage();
    ret.stack = "";
    for (StackTraceElement frame : t.getStackTrace()) {
      ret.stack += frame.toString() + "\n";
    }
    return ret;
  }

  /**
   * Creates a Thrift name node client.
   * 
   * @param conf the HDFS instance
   * @return a Thrift name node client.
   */
  public static Namenode.Client createNamenodeClient(Configuration conf)
      throws Exception {
    String s = conf.get(NamenodePlugin.THRFIT_ADDRESS_PROPERTY);
    if (s == null) {
      throw new RuntimeException("Missing property '" +
          NamenodePlugin.THRFIT_ADDRESS_PROPERTY + "'");
    }
    InetSocketAddress addr = NetUtils.createSocketAddr(s);
    TTransport t = new TSocket(addr.getHostName(), addr.getPort());
    t.open();
    TProtocol p = new TBinaryProtocol(t);
    return new Namenode.Client(p);
  }
}
