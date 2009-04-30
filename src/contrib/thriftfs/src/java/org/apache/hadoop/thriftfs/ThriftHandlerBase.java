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

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.io.IOException;
import java.util.StringTokenizer;
import javax.security.auth.login.LoginException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UnixUserGroupInformation;


/**
 * Base class to provide some utility functions for thrift plugin handlers
 */
public abstract class ThriftHandlerBase {
  protected final ThriftServerContext serverContext;
  static final Log LOG = LogFactory.getLog(ThriftHandlerBase.class);

  public ThriftHandlerBase(ThriftServerContext serverContext) {
    this.serverContext = serverContext;
  }

  /**
   * Return information about the user at the other end of the transport.
   * 
   * @param t an open Thrift transport.
   * @return the user at the other end, or a sensible default (the current Unix
   *         login user if available, {user:"unknown", groups:["unknown"]}
   *         otherwise.
   */
  UserGroupInformation getUserGroupInformation() {
    try {
      LOG.info("Trying to get remote user");
      String uid = serverContext.getRemoteUserFromIdent();
      LOG.info("Got remote user '" + uid + "'");
      return new UnixUserGroupInformation(uid, new String[] {});
    } catch (IOException ioe) {
        LOG.info("Cannot get remote user: " + ioe.getMessage());
    }

    LOG.info("Retrieving current UNIX user");
    UserGroupInformation ugi = null;
    try {
      ugi = UnixUserGroupInformation.login();
    } catch (LoginException e) {
      LOG.info("Cannot get current UNIX user: " + e.getMessage());
      ugi = new UnixUserGroupInformation(new String[] { "unknown", "unknown" });
    }

    UserGroupInformation.setCurrentUser(ugi);
    LOG.info("Connection from user " + ugi);
    return ugi;
  }
}