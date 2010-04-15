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
package org.apache.hadoop.hdfs;

import junit.framework.TestCase;
import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileUtil.HardLink;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.FSDataset;
import org.apache.hadoop.hdfs.server.datanode.SimulatedFSDataset;

import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.log4j.Level;
/**
 * This class tests the building blocks that are needed to
 * support HDFS appends.
 */
public class TestMultiThreadedSync extends TestCase {
  static final int blockSize = 1024*1024;
  static final int numBlocks = 10;
  static final int fileSize = numBlocks * blockSize + 1;

  private long seed;
  private byte[] toWrite = null;


  /*
   * creates a file but does not close it
   */ 
  private FSDataOutputStream createFile(FileSystem fileSys, Path name, int repl)
    throws IOException {
    FSDataOutputStream stm = fileSys.create(name, true,
                                            fileSys.getConf().getInt("io.file.buffer.size", 4096),
                                            (short)repl, (long)blockSize);
    return stm;
  }
  
  private void initBuffer(int size) {
    long seed = AppendTestUtil.nextLong();
    toWrite = AppendTestUtil.randomBytes(seed, size);
  }

  private class WriterThread extends Thread {
    private final FSDataOutputStream stm;
    private final AtomicReference<Throwable> thrown;
    private final int numWrites;

    public WriterThread(FSDataOutputStream stm,
      AtomicReference<Throwable> thrown, int numWrites) {
      this.stm = stm;
      this.thrown = thrown;
      this.numWrites = numWrites;
    }

    public void run() {
      try {
        for (int i = 0; i < numWrites && thrown.get() == null; i++) {
          doAWrite();
        }
      } catch (Throwable t) {
        thrown.compareAndSet(null, t);
      }
    }

    private void doAWrite() throws IOException {
      stm.write(toWrite);
      stm.sync();
    }
  }

  public void testMutipleSyncers() throws Exception {
    Configuration conf = new Configuration();
    MiniDFSCluster cluster = new MiniDFSCluster(conf, 1, true, null);
    FileSystem fs = cluster.getFileSystem();
    Path p = new Path("/multiple-syncers.dat");
    try {
      doMultithreadedWrites(conf, p, 10, 7, 10000);
    } finally {
      fs.close();
      cluster.shutdown();
    }
  }

  public void doMultithreadedWrites(
    Configuration conf, Path p, int numThreads, int bufferSize, int numWrites)
    throws Exception {
    initBuffer(bufferSize);

    // create a new file.
    FileSystem fs = p.getFileSystem(conf);
    FSDataOutputStream stm = createFile(fs, p, 1);
    System.out.println("Created file simpleFlush.dat");

    stm.sync();
    stm.sync();
    stm.sync();
    stm.write(1);
    stm.sync();
    stm.sync();

    ArrayList<Thread> threads = new ArrayList<Thread>();
    AtomicReference<Throwable> thrown = new AtomicReference<Throwable>();
    for (int i = 0; i < numThreads; i++) {
      Thread t = new WriterThread(stm, thrown, numWrites);
      threads.add(t);
      t.start();
    }

    for (Thread t : threads) {
      t.join();
    }
    if (thrown.get() != null) {
      throw new RuntimeException("Deferred", thrown.get());
    }
    stm.close();
    System.out.println("Closed file.");
  }

  public static void main(String args[]) throws Exception {
    TestMultiThreadedSync test = new TestMultiThreadedSync();
    Configuration conf = new Configuration();
    Path p = new Path("/user/todd/test.dat");
    long st = System.nanoTime();
    test.doMultithreadedWrites(conf, p, 10, 511, 50000);
    long et = System.nanoTime();

    System.out.println("Finished in " + ((et - st) / 1000000) + "ms");
  }

}
