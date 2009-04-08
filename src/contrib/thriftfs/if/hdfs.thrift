/*
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
 * hdfs.thrift
 *
 * Thrift interface for HDFS.
 */

/*
 * Namespaces for generated code. The idea is to keep code generated by
 * Thrift under a 'hadoop.api' namespace, so that a higher-level set of
 * functions and classes may be defined under 'hadoop'.
 */
namespace cpp     hadoop.api
namespace csharp  Hadoop.API
namespace java    org.apache.hadoop.thriftfs.api
namespace perl    Hadoop.API
namespace php     hadoop_api
namespace py      hadoop.api
namespace rb      Hadoop.API

/* Values for 'type' argument to getDatanodeReport(). */
enum DatanodeReportType {
  ALL_DATANODES = 1;
  LIVE_DATANODES = 2;
  DEAD_DATANODES = 3;
}

/* Values for DatanodeInfo.state */
enum DatanodeState {
  NORMAL_STATE = 1;
  DECOMMISSION_INPROGRESS = 2;
  DECOMMISSIONED = 3;
}

/* Value for unknown Thrift port in DatanodeInfo */
const i32 UNKNOWN_THRIFT_PORT = -1;

/* Values for setQuota() parameters. */
const i64 QUOTA_DONT_SET = -2

/* Clear quota on this path. */
const i64 QUOTA_RESET = -1


/** 
 * Information and state of a data node.
 *
 * Modelled after org.apache.hadoop.hdfs.protocol.DatanodeInfo
 */
struct DatanodeInfo {
  /** HDFS name of the datanode (equals to <host>:<datanode port>) */
  1: string name,
  
  /** Unique ID within a HDFS cluster */ 
  2: string storageID,
  
  /** Host name of the Thrift server socket. */
  3: string host,
  
  /** Port number of the Thrift server socket, or UNKNOWN_THRIFT_PORT
      if the Thrift port for this datanode is not known. */
  4: i32 thriftPort,
  
  /** Raw capacity of the data node (in bytes). */
  5: i64 capacity,
  
  /** Space used by the data node (in bytes). */
  6: i64 dfsUsed,
  
  /** Raw free space in the data node (in bytes). */
  7: i64 remaining,
  
  /** Number of active connections to the data node. */
  8: i32 xceiverCount,
  
  /** State of this data node. */
  9: DatanodeState state
}

/**
 * Representation of a file block in HDFS
 *
 * Modelled after org.apache.hadoop.hdfs.protocol.LocatedBlock
 */
struct Block {
  /** Block ID (unique among all blocks in a filesystem). */
  1: i64 blockId,
  
  /** Path of the file which this block belongs to. */
  2: string path,

  /** Length of this block. */
  3: i64 numBytes,
  
  /** Generational stamp of this block. */
  4: i64 genStamp,
  
  /** List of data nodes with copies  of this block. */
  5: list<DatanodeInfo> nodes
}

/**
 * Information about a path in HDFS.
 *
 * Modelled after org.apache.hadoop.fs.ContentSummary and
 *                org.apache.hadoop.fs.FileStatus
 */
struct Stat {
  /** The path. */
  1: string path,

  /**
   * True:  The path represents a file.
   * False: The path represents a directory.
   */
  2: bool isDir,

  /* Fields common to file and directory paths. */
  
  /** Access time (milliseconds since 1970-01-01 00:00 UTC). */
  3: i64 atime,          

  /** Modification time (milliseconds since 1970-01-01 00:00 UTC). */
  4: i64 mtime,
  
  /** Access permissions */                     
  5: i16 perms,
  
  /** Owner */
  6: string owner,
  
  /** Group */
  7: string group,
  
  /* Fields for directory paths (will be zero for file entries). */
  
  /** Number of files in this directory */
  8: i64 fileCount,
  
  /** Number of directories in this directory */
  9: i64 directoryCount,
  
  /** Quota for this directory (in bytes). */
  10: i64 quota,
  
  /** Space consumed in disk (in bytes). */
  11: i64 spaceConsumed,
  
  /** Quota consumed in disk (in bytes). */
  12: i64 spaceQuota,
  
  /* Fields for file paths (will be zero for directory entries). */
  
  /** Length (in bytes). */
  13: i64 length,
  
  /** Block size (in bytes). */
  14: i64 blockSize,
  
  /** Replication factor. */
  15: i16 replication
}

/** Generic I/O error */
exception IOException {
  /** Error message. */
  1: string msg,
  
  /** Textual representation of the call stack. */
  2: string stack
}

/** Quota-related error */
exception QuotaException {
  /** Error message. */
  1: string msg,
  
  /** Textual representation of the call stack. */
  2: string stack
}

/**
 * Provides an interface to a Hadoop Namenode. It is basically a Thrift
 * translation of org.apache.hadoop.hdfs.protocol.ClientProtocol.
 */
service Namenode {

  /** Set permissions of an existing file or directory. */
  void chmod(/** Path of the file or directory. */
             1:  string path,
             
             /** New permissions for the file or directory. */
             2:  i16 perms) throws (1: IOException err),

  /**
   * Set owner of a file or directory.
   *
   * If either parameter 'owner' or 'group' is set to null, that
   * parameter is left unchanged.
   *
   * Parameters 'owner' and 'group' cannot be both null.
   */
  void chown(/** Path to the file or directory */
             1:  string path,
             
             /** New owner. */
             2:  string owner,
             
             /** New group. */
             3:  string group) throws (1: IOException err),

  /**
   * Return a list containing:
   *   (index 0) The total storage capacity of the file system (in bytes).
   *   (index 1) The total used space of the file system (in bytes).
   *   (index 2) The available storage of the file system (in bytes).
   */
  list<i64> df() throws (1: IOException err),

  /**
   * Enter safe mode.
   */
  void enterSafeMode() throws (1: IOException err),

  /** Get a list of all blocks containing a region of a file */
  list<Block> getBlocks(/** Path to the file. */
                        1:  string path,
                        
                        /** Offset of the region. */
                        2:  i64 offset,
                        
                        /** Length of the region */
                        3:  i64 length) throws (1: IOException err),
  
  /** Get a report on the system's current data nodes. */
  list<DatanodeInfo> getDatanodeReport(/**
                                        * Type of data nodes to return
                                        * information about.
                                        */
                                       1: DatanodeReportType type)
                                          throws (1: IOException err),

  /**
   * Get the preferred block size for the given file.
   *
   * The path must exist, or IOException is thrown.
   */
  i64 getPreferredBlockSize(/** Path to the file. */
                            1:  string path) throws (1: IOException err),
                            
  /**
   * Returns whether HDFS is in safe mode or not.
   */
  bool isInSafeMode() throws (1: IOException err),

  /**
   * Leave safe mode.
   */
  void leaveSafeMode() throws (1: IOException err),

  /** Get a listing of the indicated directory. */
  list<Stat> ls(/** Path to the directory. */
                1:  string path) throws (1: IOException err),

  /**
   * Create a directory (or hierarchy of directories).
   *
   * Returns false if directory did not exist and could not be created,
   * true otherwise.
   */
  bool mkdirhier(/** Path to the directory. */
                 1:  string path,
                 
                 /** Access permissions of the directory. */
                 2:  i16    perms) throws (1: IOException err),

  /** Tells the name node to reread the hosts and exclude files. */
  void refreshNodes() throws (1: IOException err),

  /**
   * Rename an item in the file system namespace.
   *
   * Returns true  if successful, or
   *         false if the old name does not exist or if the new name already
   *               belongs to the namespace.
   */
  bool rename(/** Path to existing file or directory. */
              1:  string path,
              
              /** New path. */
              2:  string newPath) throws (1: IOException err),
  
  /**  Report corrupted blocks. */
  void reportBadBlocks(/** List of corrupted blocks. */
                       1:  list<Block> blocks) throws (1: IOException err),

  /**
   * Get information about a path in HDFS.
   *
   * Return value will be nul if path does not exist.
   */
  Stat stat(/** Path of the file or directory. */
            1:  string path)  throws (1: IOException err),
  
  /**
   * Set the quota for a directory.
   *
   * Quota parameters may have three types of values:
   *
   *    (1) 0 or more:      Quota will be set to that value.
   *    (2) QUOTA_DONT_SET: Quota will not be changed,
   *    (3) QUOTA_RESET:    Quota will be reset.
   *
   * Any other value is a runtime error.
   */
  void setQuota(/** Path of the directory. */
                1:  string path,
                
                /** Limit on the number of names in the directory. */
                2:  i64  namespaceQuota,
                
                /**
                 * Limit on disk space occupied by all the files in the
                 * directory.
                 */
                3: i64 diskspaceQuota) throws (1: IOException err),
  
  /**
   * Set replication factor for an existing file.
   * 
   * This call just updates the value of the replication factor. The actual
   * block replication is not expected to be performed during this method call.
   * The blocks will be populated or removed in the background as the result of
   * the routine block maintenance procedures.
   * 
   * Returns true if successful, false if file does not exist or is a
   * directory.
   */
  bool setReplication(/** Path of the file. */
                      1:  string path,
                      
                      /** New replication factor. */
                      2:  i16 replication) throws (1: IOException err),
                      
  /**
   * Delete a file or directory from the file system.
   *
   * Any blocks belonging to the deleted files will be garbage-collected.
   */
  bool unlink(/** Path of the file or directory. */
              1:  string path,
              
              /** Delete a non-empty directory recursively. */
              2:  bool recursive) throws (1: IOException err),

  /**
   * Sets the modification and access time of a file or directory.
   *
   * Setting *one single time paramater* to -1 means that time parameter
   * must not be set by this call.
   * 
   * Setting *both time parameters* to -1 means both of them must be set to
   * the current time.
   */
  void utime(/** Path of the file or directory. */
             1:  string path,
             
             /** Access time in milliseconds since 1970-01-01 00:00 UTC */
             2:  i64 atime,
             
             /** Modification time in milliseconds since 1970-01-01 00:00 UTC */
             3:  i64 mtime) throws (1: IOException err),

  
  /*
   * The following methods are meant to be called by datanodes to advertise
   * themselves to the namenode.
   */
  
  /**
   * Inform the namenode that a datanode process has started.
   */
  void datanodeUp(/** <host name>:<port number> of the datanode */
                  1:  string name,
                  /** the storage id of the datanode */
                  2:  string storage,
                  /** Thrift port of the datanode */
                  3:  i32 thriftPort),
                  
  /**
   * Inform the namenode that a datanode process has stopped.
   */
  void datanodeDown(/** <host name>:<port number> of the datanode */
                    1:  string name,
                    /** the storage id of the datanode */
                    2:  string storage,
                    /** Thrift port of the datanode */
                    3:  i32 thriftPort),
}

/** Encapsulates a block data transfer with its CRC */
struct BlockData {
  /** CRC32 of the data being transfered */
  1:  i32 crc,
  /** Length of the data being transfered */
  2:  i32 length,
  /** The data itsef */
  3:  binary data
}

/**
 * Provides an interface to data nodes, so that clients may read and write
 * data blocks.
 */
service Datanode {

  /**
   * Read bytes from a block.
   *
   * Only 2^31 - 1 bytes may be read on a single call to this method.
   */
  BlockData readBlock(/** Block to be read from. */
                      1:  Block block,
                   
                      /** Offset within the block where read must start from. */
                      2:  i64 offset,
                   
                      /** Number of bytes to read. */
                      3:  i32 length) throws (1:IOException err)
}
