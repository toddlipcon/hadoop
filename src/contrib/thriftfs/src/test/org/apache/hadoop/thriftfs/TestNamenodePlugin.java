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

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.security.UnixUserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.thriftfs.api.Block;
import org.apache.hadoop.thriftfs.api.Constants;
import org.apache.hadoop.thriftfs.api.DatanodeInfo;
import org.apache.hadoop.thriftfs.api.DatanodeReportType;
import org.apache.hadoop.thriftfs.api.IOException;
import org.apache.hadoop.thriftfs.api.Namenode;
import org.apache.hadoop.thriftfs.api.Stat;
import org.apache.log4j.Level;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Unit tests for the name node Thrift interface
 */
public class TestNamenodePlugin {

  private static MiniDFSCluster cluster;
  private static Namenode.Client namenode;
  private static FileSystem fs;

  private static final short REPLICATION = 3;

  private static final String testFile = "/test-file";
  private static Path testFilePath = new Path(testFile);
  private final short PERMS = (short) 0644;

  // Raise verbosity level of Thrift classes.
  static {
    ((Log4JLogger) NamenodePlugin.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger) ThriftPluginServer.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger) ThriftUtils.LOG).getLogger().setLevel(Level.ALL);
  }

  @BeforeClass
  public static void setUp() throws Exception {
    cluster = Helper.createCluster(REPLICATION);
    fs = cluster.getFileSystem();
    Configuration conf = Helper.createConf();
    namenode = ThriftUtils.createNamenodeClient(conf);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    cluster.shutdown();
  }
  
  @After
  public void cleanup() throws Exception {
    fs.delete(testFilePath, false);
  }

  @Test
  public void testChmod() throws Exception {
    assertFalse(fs.exists(testFilePath));
    Helper.createFile(fs, testFile, REPLICATION, PERMS, 1024, 0);
    assertTrue(fs.exists(testFilePath));
    assertEquals(PERMS,
        fs.getFileStatus(testFilePath).getPermission().toShort());

    namenode.chmod(testFile, (short) 0600);
    assertEquals((short) 0600,
        fs.getFileStatus(testFilePath).getPermission().toShort());
  }

  @Test
  public void testChown() throws Exception {
    UserGroupInformation foo = new UnixUserGroupInformation("foo",
        new String[] { "foo-group" });
    UserGroupInformation me = UnixUserGroupInformation.getCurrentUGI();
    List<String> myGroups = new ArrayList<String>();
    for (String g : me.getGroupNames()) {
      myGroups.add(g);
    }

    Helper.createFile(fs, testFile, REPLICATION, PERMS, 1024, 0);
    assertTrue(fs.exists(testFilePath));
    FileStatus st = fs.getFileStatus(testFilePath);
    assertEquals(me.getUserName(), st.getOwner());
    assertEquals("supergroup", st.getGroup());

    namenode.chown(testFile, foo.getUserName(), null);
    st = fs.getFileStatus(testFilePath);
    assertEquals(foo.getUserName(), st.getOwner());
    assertEquals("supergroup", st.getGroup());

    namenode.chown(testFile, null, "foo-group");
    st = fs.getFileStatus(testFilePath);
    assertEquals(foo.getUserName(), st.getOwner());
    assertEquals("foo-group", st.getGroup());

    try {
      namenode.chown(testFile, null, null);
      fail("chmod() needs non-null owner or group");
    } catch (IOException e) {
    }
  }

  @Test
  public void testDf() throws Exception {
    List<Long> st = namenode.df();
    assertNotNull(st);
    assertEquals(3, st.size());
    for (long val : st) {
      assertTrue(val > 0);
    }
  }

  @Test
  public void testGetDatanodeReport() throws Exception {
    int numNodes = cluster.getDataNodes().size();
    List<DatanodeInfo> nodes = namenode
        .getDatanodeReport(DatanodeReportType.ALL_DATANODES);
    assertEquals(numNodes, nodes.size());

    nodes = namenode.getDatanodeReport(DatanodeReportType.DEAD_DATANODES);
    assertEquals(0, nodes.size());

    nodes = namenode.getDatanodeReport(DatanodeReportType.LIVE_DATANODES);
    assertEquals(numNodes, nodes.size());
  }

  @Test
  public void testGetPreferredBlockSize() throws Exception {
    long bs = 1024;
    Helper.createFile(fs, testFile, REPLICATION, PERMS, bs, 0);
    assertTrue(fs.exists(testFilePath));
    assertEquals(bs, namenode.getPreferredBlockSize(testFile));

    bs /= 2;
    assertTrue(namenode.unlink(testFile, false));
    Helper.createFile(fs, testFile, REPLICATION, PERMS, bs, 0);
    assertTrue(fs.exists(testFilePath));
    assertEquals(bs, namenode.getPreferredBlockSize(testFile));
  }

  @Test
  public void testLs() throws Exception {
    List<Stat> dir = namenode.ls("/");
    assertEquals(0, dir.size());

    assertTrue(namenode.mkdirhier("/foo", (short) 0755));
    dir = namenode.ls("/");
    assertEquals(1, dir.size());
    assertEquals(true, dir.get(0).isDir);

    Helper.createFile(fs, testFile, REPLICATION, PERMS, 1024, 0);
    assertTrue(fs.exists(testFilePath));
    dir = namenode.ls("/");
    assertEquals(2, dir.size());
    assertTrue(dir.get(0).isDir != dir.get(1).isDir);
    assertTrue(namenode.unlink("/foo", true));
  }

  @Test
  public void testMkdirhier() throws Exception {
    String foo = "/foo";
    short perms = (short) 0755;
    Path fooPath = new Path(foo);
    assertFalse(fs.exists(fooPath));

    assertTrue(namenode.mkdirhier(foo, perms));
    assertTrue(fs.exists(fooPath));
    assertTrue(namenode.mkdirhier(foo, perms));
    assertTrue(namenode.unlink(foo, true));

    String bar = "/bar/baz";
    Path barPath = new Path(bar);
    assertFalse(fs.exists(barPath));
    assertTrue(namenode.mkdirhier(bar, perms));
    assertTrue(fs.exists(barPath));
    assertTrue(namenode.mkdirhier(bar, perms));
    assertTrue(namenode.unlink(bar, true));
  }

  @Test
  public void testRefreshNodes() throws Exception {
    // XXX This does not test much...
    namenode.refreshNodes();
  }

  @Test
  public void testRename() throws Exception {
    String foo = "/foo";
    short perms = (short) 0755;
    Path fooPath = new Path(foo);
    assertTrue(namenode.mkdirhier(foo, perms));
    assertTrue(fs.exists(fooPath));

    assertFalse(namenode.rename(foo, foo));
    String bar = "/bar";
    Path barPath = new Path(bar);
    assertTrue(namenode.rename(foo, bar));
    assertTrue(fs.exists(barPath));
    assertFalse(fs.exists(fooPath));

    assertFalse(fs.exists(fooPath));
    assertFalse(namenode.rename(bar, "/foo/baz"));
    assertTrue(namenode.unlink(bar, true));

    assertFalse(fs.exists(testFilePath));
    Helper.createFile(fs, testFile, REPLICATION, PERMS, 1024, 0);
    assertTrue(fs.exists(testFilePath));

    assertTrue(namenode.mkdirhier("/foo/baz", PERMS));
    String newTestFile = "/foo/baz" + testFile;
    Path newTestFilePath = new Path(newTestFile);
    assertFalse(fs.exists(newTestFilePath));
    assertTrue(namenode.rename(testFile, newTestFile));
    assertTrue(fs.exists(newTestFilePath));
    assertFalse(fs.exists(testFilePath));

    assertTrue(namenode.mkdirhier(foo, perms));
    assertTrue(fs.exists(fooPath));
    assertTrue(fs.getFileStatus(fooPath).isDir());
    assertTrue(namenode.rename(newTestFile, foo));
    // XXX Bug or feature?
    assertTrue(fs.getFileStatus(fooPath).isDir());

    assertTrue(namenode.unlink(foo, true));
  }

  @Test
  public void testReportBadBlocks() throws Exception {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < 100; ++i) {
      sb.append("Blah blah blah");
    }
    String data = sb.toString();
    FSDataOutputStream out = fs.create(testFilePath, true, 512, REPLICATION,
        512);
    out.writeBytes(data);
    out.close();

    // Block here are Thrift blocks
    List<Block> blocks = namenode.getBlocks(testFile, 0, data.length());
    assertTrue(blocks.size() > 0);
    assertEquals(REPLICATION, blocks.get(0).nodes.size());

    List<Block> badBlocks = new ArrayList<Block>();
    Block b = blocks.get(0);
    assertEquals(REPLICATION, b.nodes.size());
    b.nodes.remove(0);
    badBlocks.add(b);
    namenode.reportBadBlocks(badBlocks);

    blocks = namenode.getBlocks(testFile, 0, data.length());
    assertTrue(blocks.size() > 0);
    assertTrue(blocks.get(0).nodes.size() < REPLICATION);
  }

  @Test
  public void testSafeMode() throws Exception {
    assertFalse(namenode.isInSafeMode());
    Helper.createFile(fs, testFile, REPLICATION, PERMS, 1024, 0);
    assertTrue(fs.exists(testFilePath));
    assertTrue(namenode.unlink(testFile, false));

    namenode.enterSafeMode();
    assertTrue(namenode.isInSafeMode());
    try {
      Helper.createFile(fs, testFile, REPLICATION, PERMS, 1024, 0);
      fail("create() must fail when cluster is in safe mode");
    } catch (Throwable t) {
    }

    namenode.leaveSafeMode();
    assertFalse(namenode.isInSafeMode());
  }

  @Test
  public void testSetQuota() throws Exception {
    try {
      namenode.setQuota("/not-there", 1, Constants.QUOTA_DONT_SET);
      fail("cannot setQuota() on non-existing directories");
    } catch (IOException e) {}

    Helper.createFile(fs, testFile, REPLICATION, PERMS, 1024, 0);
    assertTrue(fs.exists(testFilePath));
    try {
      namenode.setQuota(testFile, 1, Constants.QUOTA_DONT_SET);
      fail("cannot setQuota() on files");
    } catch (IOException e) {}

    short perms = (short) 0755;
    namenode.mkdirhier("/foo", perms);
    namenode.setQuota("/foo", 2, Constants.QUOTA_DONT_SET);

    namenode.mkdirhier("/foo/one", perms);
    try {
      namenode.mkdirhier("/foo/two", perms);
      fail("namespaceQuota not set");
    } catch (IOException e) {}

    namenode.setQuota("/foo", 3, Constants.QUOTA_DONT_SET);
    assertTrue(namenode.mkdirhier("/foo/two", perms));

    try {
      namenode.setQuota("/foo", 2, Constants.QUOTA_DONT_SET);
      fail("Cannot set invalid quota");
    } catch (IOException e) {}
    
    assertTrue(namenode.unlink("/foo", true));
  }

  @Test
  public void testSetReplication() throws Exception {
    short repl = (short) (REPLICATION - 1);
    Helper.createFile(fs, testFile, repl, PERMS, 1024, 0);
    assertTrue(fs.exists(testFilePath));
    FileStatus st = fs.getFileStatus(testFilePath);
    assertEquals(repl, st.getReplication());

    assertTrue(namenode.setReplication(testFile, REPLICATION));
    st = fs.getFileStatus(testFilePath);
    assertEquals(REPLICATION, st.getReplication());
  }

  @Test
  public void testStat() throws Exception {
    Stat st = namenode.stat("/");
    assertEquals("/", st.path);
    assertTrue(st.isDir);
    long now = System.currentTimeMillis();
    Thread.sleep(10);
    assertTrue(st.mtime < now);
    assertTrue(st.atime < now);
    assertEquals(1, st.directoryCount);
    assertEquals(0, st.fileCount);

    long then = now;
    assertFalse(fs.exists(testFilePath));
    Helper.createFile(fs, testFile, REPLICATION, PERMS, 1024, 0);
    assertTrue(fs.exists(testFilePath));

    st = namenode.stat(testFile);
    assertEquals(testFile, st.path);
    assertFalse(st.isDir);
    now = System.currentTimeMillis();
    Thread.sleep(10);
    assertTrue(st.atime > then);
    assertTrue(st.mtime > then);
    assertTrue(now > st.atime);
    assertTrue(now > st.mtime);
    assertEquals(1024, st.blockSize);
    assertEquals(REPLICATION, st.replication);
    assertEquals(0, st.length);

    st = namenode.stat("/not-there");
    // XXX I would expect st == null.
    assertNull(st.path);
  }

  @Test
  public void testUnlink() throws Exception {
    assertFalse(namenode.unlink("/", true));

    assertFalse(fs.exists(testFilePath));
    assertFalse(namenode.unlink(testFile, false));

    Helper.createFile(fs, testFile, REPLICATION, PERMS, 1024, 0);
    assertTrue(fs.exists(testFilePath));
    assertTrue(namenode.unlink(testFile, false));

    assertTrue(namenode.mkdirhier("/foo", (short) 0755));
    assertTrue(namenode.unlink("/foo", false));

    assertTrue(namenode.mkdirhier("/foo", (short) 0755));
    Path newTestFile = new Path("/foo/test-file");
    Helper.createFile(fs, "/foo/test-file", REPLICATION, PERMS, 1024, 0);
    assertTrue(fs.exists(newTestFile));
    try {
      namenode.unlink("/foo", false);
      fail("unlink(path, recursive=false) must fail for non-empty paths");
    } catch (IOException e) {
    }
    assertTrue(namenode.unlink("/foo", true));
  }

  @Test
  public void testUtime() throws Exception {
    long tstamp = System.currentTimeMillis();
    Helper.createFile(fs, testFile, REPLICATION, PERMS, 1024, 0);
    assertTrue(fs.exists(testFilePath));

    FileStatus st = fs.getFileStatus(testFilePath);
    assertTrue(st.getAccessTime() >= tstamp);
    assertTrue(st.getModificationTime() >= tstamp);

    Thread.sleep(10);
    tstamp = System.currentTimeMillis();
    namenode.utime(testFile, -1, tstamp);
    st = fs.getFileStatus(testFilePath);
    assertTrue(st.getAccessTime() < tstamp);
    assertTrue(st.getModificationTime() == tstamp);

    Thread.sleep(10);
    tstamp = System.currentTimeMillis();
    namenode.utime(testFile, tstamp, -1);
    st = fs.getFileStatus(testFilePath);
    assertTrue(st.getAccessTime() == tstamp);
    assertTrue(st.getModificationTime() < tstamp);

    long prev = tstamp;
    namenode.utime(testFile, -1, -1);
    Thread.sleep(10);
    tstamp = System.currentTimeMillis();
    st = fs.getFileStatus(testFilePath);
    assertTrue(st.getModificationTime() < tstamp);
    assertTrue(st.getModificationTime() >= prev);
    assertTrue(st.getAccessTime() < tstamp);
    assertTrue(st.getAccessTime() >= prev);
    assertTrue(st.getAccessTime() == st.getModificationTime());
  }
}
