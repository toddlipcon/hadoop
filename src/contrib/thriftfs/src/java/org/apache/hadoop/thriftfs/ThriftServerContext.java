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

import java.io.OutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Random;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TSocket;

/**
 * Represents the context of a Thrift service
 */
class ThriftServerContext {
  private final TTransport transport;
  private final String clientName;

  /** The port that identd runs on according to RFC 1413 */
  private final static int IDENT_PORT = 113;

  /** How many milliseconds to wait for an identd reply */
  private final static int IDENT_TIMEOUT_MS = 1000;

  static final Log LOG = LogFactory.getLog(ThriftHandlerBase.class);

  private static Random random = new Random();

  public ThriftServerContext(TTransport transport) {
    this.transport = transport;
    this.clientName = assignClientName();
  }

  /**
   * Tries to get the client name for a given Thrift transport.
   * 
   * @param t the Thrift transport
   * @return The client name, if 'transport' was a socket, or
   *         "unknown-client:<random number>" otherwise.
   */
  private String assignClientName() {
    Socket sock = getTransportSocket();
    if (sock != null) {
      return sock.getInetAddress().getHostAddress() + ":" + sock.getPort();
    }
    return "unknown-client:" + random.nextLong();
  }

  public String getClientName() {
    return this.clientName;
  }

  /**
   * Tries to get the Socket out of a Thrift transport.
   *
   * @param t the Thrift transport
   * @return the socket, or null if the transport was non-socket type.
   */
  private Socket getTransportSocket() {
    if (TSocket.class.isAssignableFrom(transport.getClass())) {
      return ((TSocket)transport).getSocket();
    }
    return null;
  }

  /**
   * Get the user identity at the other end of a socket using the IDENT
   * protocol (RFC 1413)
   * 
   * @param socket the socket
   * @return the user id on the remote system
   * @throws IOException on network or protocol error, or if the transport is not TCP
   * @see <a href="http://www.faqs.org/rfcs/rfc1413.html">RFC 1413</a>
   */
  String getRemoteUserFromIdent() throws IOException {
    Socket socket = getTransportSocket();
    if (socket == null) {
      throw new IOException("Cannot use identd since the request was not over TCP");
    }

    return getRemoteUserFromIdent(socket, IDENT_PORT, IDENT_TIMEOUT_MS);
  }

  static String getRemoteUserFromIdent(Socket socket, int port, int timeout)
    throws IOException {
    InetAddress remote = socket.getInetAddress();
    InetSocketAddress identAddress = new InetSocketAddress(remote, port);

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
