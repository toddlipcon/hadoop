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
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UnixUserGroupInformation;
import org.apache.hadoop.util.LineReader;
import org.apache.log4j.Level;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestRemoteUser {

  private MockIdentServer identServer;
  private ServerSocket targetSocket;
  private int targetPort = 0;
  private String uid;
  
  private static final Log LOG = LogFactory.getLog(TestRemoteUser.class);
  
  static {
    ((Log4JLogger) ThriftPluginServer.LOG).getLogger().setLevel(Level.ALL);
  }

  /**
   * Dummy IDENT server.
   */
  class MockIdentServer implements Runnable {

    ServerSocket socket;
    int port;
    String uid;

    MockIdentServer(String uid) throws IOException {
      this.uid = uid;
      socket = new ServerSocket();
      socket.bind(new InetSocketAddress(0));
      port = socket.getLocalPort();
      LOG.info("Mock IDENT server bound to port " + port);
    }
    
    @Override
    public void run() {
      Socket sock = null;
      try {
        while (true) {
          sock = socket.accept();
          LOG.info("Mock IDENT server: Got connection");
          Text txt = new Text();
          LineReader in = new LineReader(sock.getInputStream());
          in.readLine(txt);
          String req = txt.toString();
          LOG.info("Mock IDENT server: Got request '" + req + "'");

          String res = "42, 42, : USERID : UNIX : " + uid + "\r\n";
          LOG.info("Mock IDENT server: Sending response '" + res + "'");
          sock.getOutputStream().write(res.getBytes());
        }
      } catch (Throwable t) {
      } finally {
        try {
          sock.close();
        } catch (IOException e) {}
      }
    }
  }
  
  @Before
  public void setUp() throws Exception {
    uid = UnixUserGroupInformation.login().getUserName();
    
    identServer = new MockIdentServer(uid);
    Thread t = new Thread(identServer);
    t.start();
    
    targetSocket = new ServerSocket();
    targetSocket.bind(new InetSocketAddress(targetPort));
    targetPort = targetSocket.getLocalPort();
    LOG.info("Target socket bound to port " + targetPort);
    t = new Thread(new Runnable(){
      @Override
      public void run() {
        Socket sock = null;
        try {
          sock = targetSocket.accept();
          LOG.info("Connection received on target socket");
          while (true) {
            try {
              Thread.sleep(1000);
            } catch (InterruptedException e) {
              LOG.info("Target socket thread interrupted");
              break;
            }
          }
        } catch (Throwable t) {
        } finally {
          try {
            sock.close();
          } catch (IOException e) {}
        }
      }
    });
    t.start();
  }
  @After
  public void tearDown() throws Exception {
    LOG.info("Closing target socket");
    targetSocket.close();
    
    LOG.info("Closing IDENT server");
    identServer.socket.close();
  }
  
  @Test
  public void testRemoteUser() throws Exception {
    Socket sock = new Socket();
    sock.connect(new InetSocketAddress(targetPort));
    
    String uid = ThriftPluginServer.getRemoteUser(sock, identServer.port, 0);
    assertEquals(this.uid, uid);
  }
}
