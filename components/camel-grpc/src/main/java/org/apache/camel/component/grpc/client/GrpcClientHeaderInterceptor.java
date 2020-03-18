/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.camel.component.grpc.client;

import java.util.HashMap;
import java.util.Map;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.ForwardingClientCallListener;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import org.apache.camel.Exchange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GrpcClientHeaderInterceptor implements ClientInterceptor {

    private static final Logger LOG = LoggerFactory.getLogger(GrpcClientHeaderInterceptor.class);

    private ThreadLocal threadLocalExchange = new ThreadLocal<Exchange>() {
        @Override
        protected Exchange initialValue() {
            return null;
        }
    };

    public void setThreadLocalExchange(ThreadLocal threadLocalExchange) {
        this.threadLocalExchange = threadLocalExchange;
    }

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(MethodDescriptor<ReqT, RespT> methodDescriptor, CallOptions callOptions, Channel channel) {
        return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(channel.newCall(methodDescriptor, callOptions)) {

            @Override
            public void start(Listener<RespT> responseListener, Metadata headers) {
                /* put custom header */
                Map<String, Object> headersMap = ((Exchange) threadLocalExchange.get()).getIn().getHeaders();
                for (String key : headersMap.keySet()) {
                    String header = (String) headersMap.get(key);
                    Metadata.Key<String> headerKey = Metadata.Key.of(key, Metadata.ASCII_STRING_MARSHALLER);
                    headers.put(headerKey, header);
                }
                LOG.debug("Request Headers :: " + headers);
                super.start(new ForwardingClientCallListener.SimpleForwardingClientCallListener<RespT>(responseListener) {
                    @Override
                    public void onHeaders(Metadata headers) {
                        LOG.debug("Response Headers :: " + headers);
                        Exchange exchange = (Exchange) threadLocalExchange.get();
                        if(exchange != null) {
                            for (String key : headers.keys()) {
                                Metadata.Key<String> headerKey = Metadata.Key.of(key, Metadata.ASCII_STRING_MARSHALLER);
                                String header = headers.get(headerKey);
                                exchange.getOut().setHeader(key,headerKey);
                            }
                        }
                        super.onHeaders(headers);
                    }
                }, headers);
            }
        };
    }
}
