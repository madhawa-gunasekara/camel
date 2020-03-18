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
package org.apache.camel.component.grpc;


import io.grpc.ForwardingServerCall;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.stub.StreamObserver;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.test.AvailablePortFinder;
import org.apache.camel.test.junit4.CamelTestSupport;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class GrpcCustomHeaderTest extends CamelTestSupport {

    private static final Logger LOG = LoggerFactory.getLogger(GrpcCustomHeaderTest.class);

    private static final int GRPC_TEST_PORT = AvailablePortFinder.getNextAvailable();
    private static final int GRPC_TEST_PING_ID = 1;
    private static final int GRPC_TEST_PONG_ID01 = 1;
    private static final int GRPC_TEST_PONG_ID02 = 2;
    private static final String GRPC_TEST_PING_VALUE = "PING";
    private static final String GRPC_TEST_PONG_VALUE = "PONG";
    private static final String CLIENT_HEADER_NAME = "clientHeader";
    private static final String EXPECTED_CLIENT_HEADER_VALUE = "clientHeaderValue";
    private static Server grpcServer;

    private String actualClientHeaderValue;
    @Test
    public void testGrpcHeadersInProducer() throws Exception {
        LOG.info("gRPC PingSyncAsync method test start");
        // Testing simple method with sync request and asyc response in synchronous invocation style
        PingRequest pingRequest = PingRequest.newBuilder().setPingName(GRPC_TEST_PING_VALUE).setPingId(GRPC_TEST_PING_ID).build();
        Object pongResponse = template.requestBody("direct:grpc-sync-sync", pingRequest);
        assertNotNull(pongResponse);
        assertTrue(pongResponse instanceof PongResponse);
        assertEquals(actualClientHeaderValue, EXPECTED_CLIENT_HEADER_VALUE);

    }

    @Override
    protected RouteBuilder createRouteBuilder() {
        return new RouteBuilder() {
            @Override
            public void configure() {
                from("direct:grpc-sync-sync").setHeader(CLIENT_HEADER_NAME, constant(EXPECTED_CLIENT_HEADER_VALUE)).to("grpc://localhost:" + GRPC_TEST_PORT + "/org.apache.camel.component.grpc.PingPong?method=pingSyncSync&synchronous=true");
                from("direct:grpc-sync-proto-method-name")
                        .to("grpc://localhost:" + GRPC_TEST_PORT + "/org.apache.camel.component.grpc.PingPong?method=PingSyncSync&synchronous=true");
                from("direct:grpc-sync-async").to("grpc://localhost:" + GRPC_TEST_PORT + "/org.apache.camel.component.grpc.PingPong?method=pingSyncAsync&synchronous=true");
            }
        };
    }

    @Before
    public void startHttpServer() throws Exception {
        grpcServer = ServerBuilder.forPort(GRPC_TEST_PORT).addService(ServerInterceptors.intercept(new GrpcCustomHeaderTest.PingPongImpl(), new HeaderServerInterceptor())).build().start();
        LOG.info("gRPC server started on port {}", GRPC_TEST_PORT);
    }

    @After
    public void stopHttpServer() {
        if (grpcServer != null) {
            grpcServer.shutdown();
            LOG.info("gRPC server stoped");
        }
    }


    /**
     * Test gRPC PingPong server implementation
     */
    static class PingPongImpl extends PingPongGrpc.PingPongImplBase {
        @Override
        public void pingSyncSync(PingRequest request, StreamObserver<PongResponse> responseObserver) {
            LOG.info("gRPC server received data from PingPong service PingId={} PingName={}", request.getPingId(), request.getPingName());
            PongResponse response = PongResponse.newBuilder().setPongName(request.getPingName() + GRPC_TEST_PONG_VALUE).setPongId(request.getPingId()).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void pingSyncAsync(PingRequest request, StreamObserver<PongResponse> responseObserver) {
            LOG.info("gRPC server received data from PingAsyncResponse service PingId={} PingName={}", request.getPingId(), request.getPingName());
            PongResponse response01 = PongResponse.newBuilder().setPongName(request.getPingName() + GRPC_TEST_PONG_VALUE).setPongId(GRPC_TEST_PONG_ID01).build();
            PongResponse response02 = PongResponse.newBuilder().setPongName(request.getPingName() + GRPC_TEST_PONG_VALUE).setPongId(GRPC_TEST_PONG_ID02).build();
            responseObserver.onNext(response01);
            responseObserver.onNext(response02);
            responseObserver.onCompleted();
        }
    }

    class HeaderServerInterceptor implements ServerInterceptor {


        private final Metadata.Key<String> customHeaderKey =
                Metadata.Key.of("custom_server_header_key", Metadata.ASCII_STRING_MARSHALLER);

        @Override
        public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call,
                                                                     final Metadata requestHeaders, ServerCallHandler<ReqT, RespT> next) {
            LOG.info("header received from client:" + requestHeaders);
            Metadata.Key<String> headerKey = Metadata.Key.of(CLIENT_HEADER_NAME, Metadata.ASCII_STRING_MARSHALLER);
            actualClientHeaderValue = requestHeaders.get(headerKey);
            return next.startCall(new ForwardingServerCall.SimpleForwardingServerCall<ReqT, RespT>(call) {
                @Override
                public void sendHeaders(Metadata responseHeaders) {
                    responseHeaders.put(customHeaderKey, "customRespondValue");
                    super.sendHeaders(responseHeaders);
                }
            }, requestHeaders);
        }
    }

}
