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

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Random;
import java.util.StringTokenizer;
import java.util.concurrent.TimeUnit;

import javax.security.auth.login.LoginException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UnixUserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.LineReader;
import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportFactory;

/**
 * Thrift HDFS plug-in base class.
 */
public abstract class PluginBase implements Configurable, Runnable {

  protected Configuration conf;
  private TThreadPoolServer server;
  //private TThreadPoolServer.Options options;

  protected int port;

  private static final Random random = new Random();

  static final Log LOG = LogFactory.getLog(PluginBase.class);

  static {
    Configuration.addDefaultResource("thriftfs-default.xml");
    Configuration.addDefaultResource("thriftfs-site.xml");
  }

  public PluginBase() {
    //options = new TThreadPoolServer.Options();
    port = -1;
  }

  /**
   * Start processing requests.
   * 
   * @throws IllegalStateException if the server has already been started.
   * @throws IOException on network errors.
   */
  public void start() throws IOException {
    InetSocketAddress address = getAddress();
    String hostname = address.getHostName();
    port = address.getPort();

    synchronized (this) {
      if (server != null) {
        throw new IllegalStateException("Thrift server already started");
      }
      LOG.info("Starting Thrift server");
      ServerSocket sock = new ServerSocket();
      sock.setReuseAddress(true);
      if (port == 0) {
        sock.bind(null);
        address = new InetSocketAddress(hostname, sock.getLocalPort());
        port = address.getPort();
      } else {
        sock.bind(address);
      }
      TServerTransport transport = new TServerSocket(sock);
      TThreadPoolServer.Options options = new TThreadPoolServer.Options();
      options.minWorkerThreads = conf.getInt("dfs.thrift.threads.min", 5);
      options.maxWorkerThreads = conf.getInt("dfs.thrift.threads.max", 20);
      options.stopTimeoutVal = conf.getInt("dfs.thrift.timeout", 60);
      options.stopTimeoutUnit = TimeUnit.SECONDS;
      server = new TThreadPoolServer(getProcessorFactory(), transport,
          new TTransportFactory(), new TTransportFactory(),
          new TBinaryProtocol.Factory(), new TBinaryProtocol.Factory(), options);
    }

    Thread t = new Thread(this);
    t.start();
    LOG.info("Thrift server listening on " + hostname + ":" + port);
  }

  /** Stop processing requests. */
  public void stop() {
    synchronized (this) {
      if (server != null) {
        LOG.info("Stopping Thrift server");
        server.stop();
        LOG.info("Thrift stopped");
        server = null;
        port = -1;
      }
    }
  }

  public void close() throws IOException {
    stop();
  }

  public void run() {
    server.serve();
  }

  public Configuration getConf() {
    return conf;
  }

  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  /** Extract this server's address from the configuration object */
  protected abstract InetSocketAddress getAddress();

  /** Return a processor factory for this server */
  protected abstract TProcessorFactory getProcessorFactory();

  /**
   * Tries to get the client name for a given Thrift transport.
   * 
   * @param t the Thrift transport
   * @return The client name, if 'transport' was a socket, or
   *         "unknown-client:<random number>" otherwise.
   */
  protected static String getClientName(TTransport t) {
    if (TSocket.class.isAssignableFrom(t.getClass())) {
      Socket s = ((TSocket)t).getSocket();
      return s.getInetAddress().getHostAddress() + ":" + s.getPort();
    }
    return "unknown-client:" + random.nextLong();
  }

  /**
   * Return information about the user at the other end of the transport.
   * 
   * @param t an open Thrift transport.
   * @return the user at the other end, or a sensible default (the current Unix
   *         login user if available, {user:"unknown", groups:["unknown"]}
   *         otherwise.
   */
  protected static UserGroupInformation getUserGroupInformation(TTransport t) {
    if (TSocket.class.isAssignableFrom(t.getClass())) {
      LOG.info("Trying to get remote user");
      try {
        String uid = getRemoteUser(((TSocket)t).getSocket());
        LOG.info("Got remote user '" + uid + "'");
        return new UnixUserGroupInformation(uid, new String[] {});
      } catch (Exception e) {
        LOG.info("Cannot get remote user: " + e.getMessage());
      }
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

  /**
   * Get the user identity at the other end of a socket using the IDENT
   * protocol.
   * 
   * @param socket the socket
   * @return the user id
   * @throws IOException on network or protocol error
   * @see <a href="http://www.faqs.org/rfcs/rfc1413.html">RFC 1413</a>
   */
  static String getRemoteUser(Socket socket) throws IOException {
    return getRemoteUser(socket, 113, 1000);
  }

  /**
   * Get the user identity at the other end of a socket using the IDENT
   * protocol.
   * 
   * Visibility set to package for testing purposes.
   * 
   * @param socket the socket
   * @param identPort the IDENT port number on the other end of the socket
   * @param timeout timeout (in milliseconds) for the IDENT connection.
   * @return the user id
   */
  static String getRemoteUser(Socket socket, int identPort, int timeout)
      throws IOException {
    InetAddress remote = socket.getInetAddress();
    InetSocketAddress identAddress = new InetSocketAddress(remote, identPort);
    LOG.debug("Connecting to IDENT server on " + identAddress);
    Socket identSock = new Socket();
    identSock.connect(identAddress, timeout);
    try {
      String req = socket.getPort() + "," + socket.getLocalPort() + "\r\n";
      LOG.debug("Sending IDENT request: " + req);
      OutputStream out = identSock.getOutputStream();
      out.write(req.getBytes());
      out.flush();

      LOG.debug("Reading IDENT response");
      Text res = new Text();
      LineReader in = new LineReader(identSock.getInputStream());
      in.readLine(res);
      String s = res.toString();
      LOG.debug("Got IDENT response: " + s);
      StringTokenizer tk = new StringTokenizer(s, ":");
      // Skip <port-on-server>,<port-on-client>
      tk.nextToken();
      String result = tk.nextToken().trim();
      if ("USERID".equals(result)) {
        // Skip <opsys-field>
        tk.nextToken();
        return tk.nextToken().trim();
      }
      if ("ERROR".equals(result)) {
        throw new IOException("IDENT protocol error: " + tk.nextToken().trim());
      }
      throw new IOException("Malformed IDENT response '" + s + "'");
    } finally {
      try {
        LOG.debug("Closing IDENT socket");
        identSock.close();
      } catch (Exception e) {
        LOG.error("Cannot close IDENT socket to " + identAddress, e);
      }
    }
  }
}
