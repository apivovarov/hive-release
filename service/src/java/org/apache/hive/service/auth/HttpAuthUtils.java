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


package org.apache.hive.service.auth;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hive.service.cli.thrift.TCLIService;
import org.apache.hive.service.cli.thrift.TCLIService.Iface;
import org.apache.hive.service.cli.thrift.ThriftCLIService;
import org.apache.http.protocol.BasicHttpContext;
import org.apache.http.protocol.HttpContext;
import org.apache.thrift.TProcessor;
import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.transport.TTransport;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.Oid;

/**
 *
 * Utility functions for http mode authentication
 *
 */
public class HttpAuthUtils {

  public static final String WWW_AUTHENTICATE = "WWW-Authenticate";
  public static final String AUTHORIZATION = "Authorization";
  public static final String BASIC = "Basic";
  public static final String NEGOTIATE = "Negotiate";

  private static class HttpCLIServiceProcessorFactory extends TProcessorFactory {
    private final ThriftCLIService service;
    private final HiveConf hiveConf;
    private final boolean isDoAsEnabled;

    public HttpCLIServiceProcessorFactory(ThriftCLIService service) {
      super(null);
      this.service = service;
      this.hiveConf = service.getHiveConf();
      this.isDoAsEnabled = hiveConf.getBoolVar(HiveConf.ConfVars.HIVE_SERVER2_ENABLE_DOAS);
    }

    @Override
    public TProcessor getProcessor(TTransport trans) {
      TProcessor baseProcessor = new TCLIService.Processor<Iface>(service);
      return isDoAsEnabled ? new HttpCLIServiceUGIProcessor(baseProcessor) :
        baseProcessor;
    }
  }

  public static TProcessorFactory getAuthProcFactory(ThriftCLIService service) {
    return new HttpCLIServiceProcessorFactory(service);
  }

  /**
   * 
   * @return Stringified Base64 encoded kerberosAuthHeader on success
   * @throws GSSException
   * @throws IOException
   * @throws InterruptedException
   */
  public static String doKerberosAuth(String principal, String host, String serverHttpUrl)
      throws GSSException, IOException, InterruptedException {
    UserGroupInformation clientUGI = getClientUGI("kerberos");
    String serverPrincipal = getServerPrincipal(principal, host);
    // Uses the Ticket Granting Ticket in the UserGroupInformation
    return clientUGI.doAs(new HttpKerberosClientAction(serverPrincipal,
        clientUGI.getUserName(), serverHttpUrl));
  }

  /**
   * Get server pricipal and verify that hostname is present
   * @return
   * @throws IOException
   */
  private static String getServerPrincipal(String principal, String host)
      throws IOException {
    return ShimLoader.getHadoopThriftAuthBridge().getServerPrincipal(principal, host);
  }

  /**
   * JAAS login to setup the client UserGroupInformation.
   * Sets up the kerberos Ticket Granting Ticket,
   * in the client UserGroupInformation object
   * @return Client's UserGroupInformation
   * @throws IOException
   */
  public static UserGroupInformation getClientUGI(String authType)
      throws IOException {
    return ShimLoader.getHadoopThriftAuthBridge().getCurrentUGIWithConf(authType);
  }

  /**
   * 
   * HttpKerberosClientAction
   *
   */
  public static class HttpKerberosClientAction implements PrivilegedExceptionAction<String> {
    String serverPrincipal;
    String clientUserName;
    String serverHttpUrl;
    private final Base64 base64codec;
    public static final String HTTP_RESPONSE = "HTTP_RESPONSE";
    public static final String SERVER_HTTP_URL = "SERVER_HTTP_URL";
    private HttpContext httpContext;

    public HttpKerberosClientAction(String serverPrincipal,
        String clientUserName, String serverHttpUrl) {
      this.serverPrincipal = serverPrincipal;
      this.clientUserName = clientUserName;
      this.serverHttpUrl = serverHttpUrl;
      this.base64codec = new Base64(0);
      this.httpContext = new BasicHttpContext();
      httpContext.setAttribute(SERVER_HTTP_URL, serverHttpUrl);
    }

    @Override
    public String run() throws Exception {
      // This Oid for Kerberos GSS-API mechanism.
      Oid krb5Oid = new Oid("1.2.840.113554.1.2.2");

      GSSManager manager = GSSManager.getInstance();

      // Create a GSSName out of the server's name.
      GSSName serverName = manager.createName(serverPrincipal,
          GSSName.NT_HOSTBASED_SERVICE, krb5Oid);

      /*
       * Create a GSSContext for mutual authentication with the
       * server.
       *    - serverName is the GSSName that represents the server.
       *    - krb5Oid is the Oid that represents the mechanism to
       *      use. The client chooses the mechanism to use.
       *    - null is passed in for client credentials
       *    - DEFAULT_LIFETIME lets the mechanism decide how long the
       *      context can remain valid.
       * Note: Passing in null for the credentials asks GSS-API to
       * use the default credentials. This means that the mechanism
       * will look among the credentials stored in the current Subject (UserGroupInformation)
       * to find the right kind of credentials that it needs.
       */
      GSSContext gssContext = manager.createContext(serverName,
          krb5Oid,
          null,
          GSSContext.DEFAULT_LIFETIME);

      // Mutual authentication not required
      gssContext.requestMutualAuth(false);

      // Estabilish context
      byte[] inToken = new byte[0];
      byte[] outToken;
      byte[] userNameWithSeparator = (clientUserName + ":").getBytes();
      // ArrayUtils.addAll(one,two)
      outToken = gssContext.initSecContext(inToken, 0, inToken.length);
      gssContext.dispose();
      return new String(base64codec.encode(ArrayUtils.addAll(
          userNameWithSeparator, outToken)), "UTF-8");
    }
  }
}
