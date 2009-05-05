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
import java.util.Map;
import javax.security.auth.login.LoginException;

import org.apache.hadoop.conf.Configuration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UnixUserGroupInformation;
import org.apache.hadoop.util.VersionInfo;

import org.apache.hadoop.thriftfs.api.RequestContext;
import org.apache.hadoop.thriftfs.api.HadoopServiceBase;

/**
 * Base class to provide some utility functions for thrift plugin handlers
 */
public abstract class ThriftHandlerBase implements HadoopServiceBase.Iface {
  protected final ThriftServerContext serverContext;
  static final Log LOG = LogFactory.getLog(ThriftHandlerBase.class);

  public ThriftHandlerBase(ThriftServerContext serverContext) {
    this.serverContext = serverContext;
  }

  /**
   * Return the version info of this server
   */
  public org.apache.hadoop.thriftfs.api.VersionInfo getVersionInfo(
    RequestContext ctx) {
    org.apache.hadoop.thriftfs.api.VersionInfo vi =
      new org.apache.hadoop.thriftfs.api.VersionInfo();
    vi.version = VersionInfo.getVersion();
    vi.revision = VersionInfo.getRevision();
    vi.branch = VersionInfo.getBranch();
    vi.compileDate = VersionInfo.getDate();
    vi.compilingUser = VersionInfo.getUser();
    vi.url = VersionInfo.getUrl();
    vi.buildVersion = VersionInfo.getBuildVersion();
    return vi;
  }

  /**
   * Should be called by all RPCs on the request context passed in.
   * This assumes the authentication role of the requester.
   */
  protected void assumeUserContext(RequestContext ctx) {
    UserGroupInformation ugi = null;
    if (ctx != null && ctx.confOptions != null) {
      Configuration conf = new Configuration(false);
      
      for (Map.Entry<String, String> entry : ctx.confOptions.entrySet()) {
        conf.set(entry.getKey(), entry.getValue());
      }
      try {
        ugi = UnixUserGroupInformation.readFromConf(
          conf, UnixUserGroupInformation.UGI_PROPERTY_NAME);
      } catch (Throwable e) {}
    }
    if (ugi == null) {
      ugi = inferUserGroupInformation();
    }
    UserGroupInformation.setCurrentUser(ugi);
  }

  /**
   * Infer information about the user at the other end of the transport.
   * 
   * @param t an open Thrift transport.
   * @return the user at the other end, or a sensible default (the current Unix
   *         login user if available, {user:"unknown", groups:["unknown"]}
   *         otherwise.
   */
  private UserGroupInformation inferUserGroupInformation() {
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