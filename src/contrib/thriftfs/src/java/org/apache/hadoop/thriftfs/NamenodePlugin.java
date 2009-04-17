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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.FSConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.FSConstants.SafeModeAction;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.thriftfs.api.Block;
import org.apache.hadoop.thriftfs.api.Constants;
import org.apache.hadoop.thriftfs.api.DatanodeInfo;
import org.apache.hadoop.thriftfs.api.IOException;
import org.apache.hadoop.thriftfs.api.Namenode;
import org.apache.hadoop.thriftfs.api.Stat;
import org.apache.hadoop.util.ServicePlugin;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.transport.TTransport;

public class NamenodePlugin extends PluginBase implements ServicePlugin {

  /** Name of the configuration property of the Thrift server address */
  public static final String THRFIT_ADDRESS_PROPERTY = "dfs.thrift.address";

  /**
   * Default address and port this server will bind to, in case nothing is found
   * in the configuration object.
   */
  public static final String DEFAULT_THRIFT_ADDRESS = "0.0.0.0:9090";

  private NameNode namenode;

  private static Map<String, Integer> thriftPorts =
      new HashMap<String, Integer>();

  static final Log LOG = LogFactory.getLog(NamenodePlugin.class);

  /** Java server-side implementation of the 'Namenode' Thrift interface. */
  class ThriftHandler implements Namenode.Iface {

    public ThriftHandler() {}

    public void chmod(String path, short mode) throws IOException, TException {
      LOG.debug("chmod(" + path + ", " + mode + "): Entering");
      try {
        namenode.setPermission(path, new FsPermission(mode));
      } catch (Throwable t) {
        LOG.info("chmod(" + path + ", " + mode + "): Failed", t);
        throw ThriftUtils.toThrift(t);
      }
    }

    public void chown(String path, String owner, String group)
        throws IOException, TException {
      LOG.debug("chown(" + path + "," + owner + "," + group + "): Entering");
      try {
        // XXX Looks like namenode.setOwner() does not complain about this...
        if (owner == null && group == null) {
          throw new IllegalArgumentException(
              "Both 'owner' and 'group' are null");
        }
        namenode.setOwner(path, owner, group);
      } catch (Throwable t) {
        LOG.info("chown(" + path + "," + owner + "," + group + "): Failed", t);
        throw ThriftUtils.toThrift(t);
      }
    }

    public List<Long> df() throws IOException, TException {
      LOG.debug("Entering df()");
      try {
        long[] stats = namenode.getStats();
        List<Long> ret = new ArrayList<Long>();
        // capacityTotal
        ret.add(stats[0]);
        // capacityUsed
        ret.add(stats[1]);
        // capacityRemaining
        ret.add(stats[2]);
        LOG.debug("df(): Returning " + ret);
        return ret;
      } catch (Throwable t) {
        LOG.info("df(): Failed", t);
        throw ThriftUtils.toThrift(t);
      }
    }

    public void enterSafeMode() throws IOException, TException {
      LOG.debug("enterSafeMode(): Entering");
      try {
        namenode.setSafeMode(SafeModeAction.SAFEMODE_ENTER);
      } catch (Throwable t) {
        LOG.info("enterSafeMode(): Failed", t);
        throw ThriftUtils.toThrift(t);
      }
    }

    public List<Block> getBlocks(String path, long offset, long length)
        throws IOException, TException {
      LOG.debug("getBlocks(" + path + "," + offset + "," + length
          + "): Entering");
      List<Block> ret = new ArrayList<Block>();
      try {
        LocatedBlocks blocks = namenode.getBlockLocations(path, offset, length);
        for (LocatedBlock b : blocks.getLocatedBlocks()) {
          ret.add(ThriftUtils.toThrift(b, path, thriftPorts));
        }
        LOG.debug("getBlocks(" + path + "," + offset + "," + length
            + "): Returning " + ret);
        return ret;
      } catch (Throwable t) {
        LOG.info("getBlocks(" + path + "," + offset + "," + length
            + "): Failed", t);
        throw ThriftUtils.toThrift(t);
      }
    }

    public List<DatanodeInfo> getDatanodeReport(int type) throws IOException,
        TException {
      LOG.debug("getDatanodeReport(" + type + "): Entering");
      List<DatanodeInfo> ret = new ArrayList<DatanodeInfo>();
      try {
        DatanodeReportType rt;
        switch (type) {
        case Constants.ALL_DATANODES:
          rt = DatanodeReportType.ALL;
          break;
        case Constants.DEAD_DATANODES:
          rt = DatanodeReportType.DEAD;
          break;
        case Constants.LIVE_DATANODES:
          rt = DatanodeReportType.LIVE;
          break;
        default:
          throw new IllegalArgumentException("Invalid report type " + type);
        }
        for (org.apache.hadoop.hdfs.protocol.DatanodeInfo node : namenode
            .getDatanodeReport(rt)) {
          ret.add(ThriftUtils.toThrift(node, thriftPorts));
        }
        LOG.debug("getDatanodeReport(" + type + "): Returning " + ret);
        return ret;
      } catch (Throwable t) {
        LOG.info("getDatanodeReport(" + type + "): Failed", t);
        throw ThriftUtils.toThrift(t);
      }
    }

    public long getPreferredBlockSize(String path) throws IOException,
        TException {
      LOG.debug("getPreferredBlockSize(" + path + "): Entering");
      try {
        long ret = namenode.getPreferredBlockSize(path);
        LOG.debug("getPreferredBlockSize(" + path + "): Returning " + ret);
        return ret;
      } catch (Throwable t) {
        LOG.info("getPreferredBlockSize(" + path + "): Failed", t);
        throw ThriftUtils.toThrift(t);
      }
    }

    public boolean isInSafeMode() throws IOException, TException {
      LOG.debug("isInSafeMode(): Entering");
      try {
        boolean ret = namenode.setSafeMode(SafeModeAction.SAFEMODE_GET);
        LOG.debug("isInSafeMode(): Returning " + ret);
        return ret;
      } catch (Throwable t) {
        LOG.info("isInSafeMode(): Failed", t);
        throw ThriftUtils.toThrift(t);
      }
    }

    public void leaveSafeMode() throws IOException, TException {
      LOG.debug("leaveSafeMode(): Entering");
      try {
        namenode.setSafeMode(SafeModeAction.SAFEMODE_LEAVE);
      } catch (Throwable t) {
        LOG.info("leaveSafeMode(): Failed", t);
        throw ThriftUtils.toThrift(t);
      }
    }

    public List<Stat> ls(String path) throws IOException, TException {
      LOG.debug("ls(" + path + "):Entering");
      List<Stat> ret = new ArrayList<Stat>();
      try {
        for (FileStatus f : namenode.getListing(path)) {
          ret.add(toThrift(f));
        }
        LOG.debug("ls(" + path + "): Returning " + ret);
        return ret;
      } catch (Throwable t) {
        LOG.info("ls(" + path + "): Failed", t);
        throw ThriftUtils.toThrift(t);
      }
    }

    public boolean mkdirhier(String path, short perms) throws IOException,
        TException {
      LOG.debug("mkdirhier(" + path + ", " + perms + "): Entering");
      try {
        boolean ret = namenode.mkdirs(path, new FsPermission(perms));
        LOG.debug("mkdirhier(" + path + ", " + perms + "): Returning " + ret);
        return ret;
      } catch (Throwable t) {
        LOG.info("mkdirhier(" + path + ", " + perms + "): Failed", t);
        throw ThriftUtils.toThrift(t);
      }
    }

    public void refreshNodes() throws IOException, TException {
      LOG.debug("refreshNodes(): Entering");
      try {
        namenode.refreshNodes();
      } catch (Throwable t) {
        LOG.info("refreshNodes(): Failed", t);
        throw ThriftUtils.toThrift(t);
      }
    }

    public boolean rename(String path, String newPath) throws IOException,
        TException {
      LOG.debug("rename(" + path + ", " + newPath + "): Entering");
      try {
        boolean ret = namenode.rename(path, newPath);
        LOG.debug("rename(" + path + ", " + newPath + "): Returning " + ret);
        return ret;
      } catch (Throwable t) {
        LOG.info("rename(" + path + ", " + newPath + "): Failed", t);
        throw ThriftUtils.toThrift(t);
      }
    }

    public void reportBadBlocks(List<Block> blocks) throws IOException,
        TException {
      LOG.debug("reportBadBlocks(" + blocks + "): Entering");
      int n = blocks.size();
      LocatedBlock[] lb = new LocatedBlock[n];
      for (int i = 0; i < n; ++i) {
        lb[i] = ThriftUtils.fromThrift(blocks.get(i));
      }
      try {
        namenode.reportBadBlocks(lb);
      } catch (Throwable t) {
        LOG.info("reportBadBlocks(" + blocks + "): Failed", t);
        throw ThriftUtils.toThrift(t);
      }
    }

    public void setQuota(String path, long namespaceQuota, long diskspaceQuota)
        throws IOException, TException {
      LOG.debug("setQuota(" + path + "," + namespaceQuota + ","
          + diskspaceQuota + "): Entering");
      if (namespaceQuota == Constants.QUOTA_DONT_SET) {
        namespaceQuota = FSConstants.QUOTA_DONT_SET;
      }
      if (namespaceQuota == Constants.QUOTA_RESET) {
        namespaceQuota = FSConstants.QUOTA_RESET;
      }
      if (diskspaceQuota == Constants.QUOTA_DONT_SET) {
        diskspaceQuota = FSConstants.QUOTA_DONT_SET;
      }
      if (diskspaceQuota == Constants.QUOTA_RESET) {
        diskspaceQuota = FSConstants.QUOTA_RESET;
      }
      LOG.debug("setQuota(" + path + "," + namespaceQuota + ","
          + diskspaceQuota + "): Quota values translated");
      try {
        namenode.setQuota(path, namespaceQuota, diskspaceQuota);
      } catch (Throwable t) {
        LOG.info("setQuota(" + path + "," + namespaceQuota + ","
            + diskspaceQuota + "): Failed", t);
        throw ThriftUtils.toThrift(t);
      }
    }

    public boolean setReplication(String path, short repl) throws IOException,
        TException {
      LOG.debug("setReplication(" + path + "," + repl + "): Entering");
      try {
        return namenode.setReplication(path, repl);
      } catch (Throwable t) {
        LOG.info("setReplication(" + path + "," + repl + "): Failed", t);
        throw ThriftUtils.toThrift(t);
      }
    }

    public Stat stat(String path) throws IOException, TException {
      LOG.debug("stat(" + path + "): Entering");
      try {
        Stat ret = toThrift(namenode.getFileInfo(path));
        LOG.debug("stat(" + path + "): Returning " + ret);
        return ret;
      } catch (Throwable t) {
        LOG.info("stat(" + path + "): Failed");
        throw ThriftUtils.toThrift(t);
      }
    }

    public boolean unlink(String path, boolean recursive) throws IOException,
        TException {
      LOG.debug("unlink(" + path + "," + recursive + "): Entering");
      try {
        boolean ret = namenode.delete(path, recursive);
        LOG.debug("unlink(" + path + "," + recursive + "): Returning " + ret);
        return ret;
      } catch (Throwable t) {
        LOG.info("unlink(" + path + "," + recursive + "): Failed", t);
        throw ThriftUtils.toThrift(t);
      }
    }

    public void utime(String path, long atime, long mtime) throws IOException,
        TException {
      LOG.debug("utime(" + path + "," + atime + "," + mtime + "): Entering");
      if (mtime == -1 && atime == -1) {
        LOG.debug("utime(" + path + "," + atime + "," + mtime
            + "): Setting mtime and atime to now");
        mtime = atime = System.currentTimeMillis();
      }
      try {
        namenode.setTimes(path, mtime, atime);
      } catch (Throwable t) {
        LOG.info("utime(" + path + "," + atime + "," + mtime + "): Failed", t);
        throw ThriftUtils.toThrift(t);
      }
    }

    private Stat toThrift(FileStatus f) throws java.io.IOException {
      Stat st = new Stat();
      if (f == null) {
        return st;
      }

      st.path = f.getPath().toString();
      st.isDir = f.isDir();
      st.atime = f.getAccessTime();
      st.mtime = f.getModificationTime();
      st.perms = f.getPermission().toShort();
      st.owner = f.getOwner();
      st.group = f.getGroup();
      if (st.isDir) {
        ContentSummary s = namenode.getContentSummary(st.path);
        st.fileCount = s.getFileCount();
        st.directoryCount = s.getDirectoryCount();
        st.quota = s.getQuota();
        st.spaceConsumed = s.getSpaceConsumed();
        st.spaceQuota = s.getSpaceQuota();
      } else {
        st.length = f.getLen();
        st.blockSize = f.getBlockSize();
        st.replication = f.getReplication();
      }
      return st;
    }

    public void datanodeDown(String name, int thriftPort) throws TException {
      String addr = normalizeAddress(name);
      LOG.info("Datanode " + addr + ": Thrift port " + thriftPort + " closed");
      thriftPorts.remove(addr);
    }

    public void datanodeUp(String name, int thriftPort) throws TException {
      String addr = normalizeAddress(name);
      LOG.info("Datanode " + addr + ": Thrift port " + thriftPort + " open");
      thriftPorts.put(addr, thriftPort);
    }

    private String normalizeAddress(String name) {
      String resolved;
      int i = name.indexOf(':');
      if (i == -1) {
        resolved = NetUtils.normalizeHostName(name);
      } else {
        resolved = NetUtils.normalizeHostName(name.substring(0, i));
        resolved += ":" + name.substring(i + 1);
      }
      return resolved;
    }
  }

  @Override
  public void start(Object service) {
    this.namenode = (NameNode)service;
    try {
      super.start();
    } catch (Throwable t) {
      LOG.warn("Cannot start Thrift namenode plug-in", t);
    }
  }

  class ProcessorFactory extends TProcessorFactory {

    ProcessorFactory() {
      super(null);
    }

    @Override
    public TProcessor getProcessor(TTransport t) {
      ThriftHandler impl = new ThriftHandler();
      UserGroupInformation ugi = PluginBase.getUserGroupInformation(t);
      UserGroupInformation.setCurrentUser(ugi);
      LOG.info("Connection from user " + ugi);
      return new Namenode.Processor(impl);
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
