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

package org.apache.hive.jdbc;

import java.io.IOException;

import org.apache.hive.service.auth.HttpAuthUtils;
import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.message.BufferedHeader;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.CharArrayBuffer;

/**
 *
 * Authentication interceptor which adds Base64 encoded payload,
 * containing the username and kerberos service ticket,
 * to the outgoing http request header.
 *
 */
public class HttpKerberosRequestInterceptor implements HttpRequestInterceptor {

  String kerberosAuthHeader;

  public HttpKerberosRequestInterceptor(String kerberosAuthHeader) {
    this.kerberosAuthHeader = kerberosAuthHeader;
  }

  @Override
  public void process(HttpRequest httpRequest, HttpContext httpContext) throws HttpException, IOException {
    // Set the session key token (Base64 encoded) in the header
    CharArrayBuffer buffer = new CharArrayBuffer(32);
    buffer.append(HttpAuthUtils.AUTHORIZATION);
    buffer.append(": " + HttpAuthUtils.NEGOTIATE);
    buffer.append(kerberosAuthHeader);
    httpRequest.addHeader(new BufferedHeader(buffer));
  }

}
