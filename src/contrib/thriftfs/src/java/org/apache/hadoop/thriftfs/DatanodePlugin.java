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

import java.io.EOFException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.zip.CRC32;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.thriftfs.api.Block;
import org.apache.hadoop.thriftfs.api.BlockData;
import org.apache.hadoop.thriftfs.api.Datanode;
import org.apache.hadoop.thriftfs.api.IOException;
import org.apache.hadoop.thriftfs.api.Namenode;
import org.apache.hadoop.util.ServicePlugin;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.transport.TTransport;

public class DatanodePlugin extends PluginBase implements ServicePlugin {

  /** Name of the configuration property of the Thrift server address */
  public static final String THRFIT_ADDRESS_PROPERTY =
      "dfs.thrift.datanode.address";
  /**
   * Default address and port this server will bind to, in case nothing is found
   * in the configuration object.
   */
  public static final String DEFAULT_THRIFT_ADDRESS = "0.0.0.0:0";

  private DataNode datanode;
  private Thread registerThread;
  private boolean register;

  static final Log LOG = LogFactory.getLog(DatanodePlugin.class);

  class ThriftHandler implements Datanode.Iface {

    private String clientName;
    private int bufferSize;
    private CRC32 summer;

    public ThriftHandler(String clientName) {
      this.clientName = clientName;
      this.bufferSize = conf.getInt("io.file.buffer.size", 4096);
      this.summer = new CRC32();
    }

    public BlockData readBlock(Block block, long offset, int length)
        throws IOException, TException {
      LOG.debug("readBlock(" + block.blockId + "," + offset + "," + length
          + "): Entering");

      BlockData ret = new BlockData();
      DFSClient.BlockReader reader = null;
      try {
        reader = DFSClient.BlockReader.newBlockReader(getSocket(), block.path,
            block.blockId, block.genStamp, offset, length, bufferSize, true,
            clientName);
        byte[] buf = new byte[length];
        int n = reader.read(buf, 0, length);
        if (n == -1) {
          throw new EOFException("EOF reading " + length + " bytes at offset "
              + offset + " from " + block);
        }
        LOG.debug("readBlock(" + block.blockId + ", " + offset + ", " + length
            + "): Read " + n + " bytes");
        ret.data = new byte[n];
        System.arraycopy(buf, 0, ret.data, 0, n);
        ret.length = n;

        summer.update(ret.data);
        ret.crc = (int) summer.getValue();
        summer.reset();
        LOG.debug("readBlock(" + block.blockId + ", " + offset + ", " + length
            + "): CRC32: " + ret.crc);
      } catch (Throwable t) {
        LOG.info("readBlock(" + block.blockId + ", " + offset + ", " + length
            + "): Failed", t);
        throw ThriftUtils.toThrift(t);
      } finally {
        if (reader != null) {
          try {
            reader.close();
          } catch (Throwable t) {
            LOG.warn("readBlock(" + block.blockId + ", " + offset + ", "
                + length + "): Cannot close block reader", t);
          }
        }
      }
      return ret;
    }

    private Socket getSocket() throws java.io.IOException {
      InetSocketAddress addr = datanode.getSelfAddr();
      return new Socket(addr.getAddress(), addr.getPort());
    }
  }

  @Override
  public void start(Object service) {
    this.datanode = (DataNode)service;
    try {
      super.start();

      register = true;
      registerThread = new Thread(new Runnable() {
        public void run() {
          Namenode.Client namenode = null;
          String name = datanode.dnRegistration.getName();
          while (register) {
            try {
              if (namenode == null) {
                namenode = ThriftUtils.createNamenodeClient(conf);
              }
              namenode.datanodeUp(name, port);
              register = false;
              LOG.info("Datanode " + name + " registered Thrift port " + port);
            } catch (Throwable t) {
              LOG.info("Datanode registration failed", t);
              try {
                Thread.sleep(1000);
              } catch (InterruptedException e) {}
            }
          }
        }
      });
      registerThread.start();
    } catch (Throwable t) {
      LOG.warn("Cannot start Thrift datanode plug-in", t);
    }
  }

  @Override
  public void stop() {
    register = false;
    try {
      registerThread.join();
    } catch (Throwable t) {}

    try {
      Namenode.Client namenode = ThriftUtils.createNamenodeClient(conf);
      namenode.datanodeDown(datanode.dnRegistration.getName(), port);
    } catch (Throwable t) {}

    super.stop();
  }

  class ProcessorFactory extends TProcessorFactory {

    ProcessorFactory() {
      super(null);
    }

    @Override
    public TProcessor getProcessor(TTransport t) {
      ThriftHandler impl = new ThriftHandler(PluginBase.getClientName(t));
      UserGroupInformation ugi = PluginBase.getUserGroupInformation(t);
      UserGroupInformation.setCurrentUser(ugi);
      LOG.info("Connection from user " + ugi);
      return new Datanode.Processor(impl);
    }
  }

  @Override
  protected TProcessorFactory getProcessorFactory() {
    return new ProcessorFactory();
  }

  @Override
  protected InetSocketAddress getAddress() {
    return NetUtils.createSocketAddr(conf.get(THRFIT_ADDRESS_PROPERTY,
        DEFAULT_THRIFT_ADDRESS));
  }
}
