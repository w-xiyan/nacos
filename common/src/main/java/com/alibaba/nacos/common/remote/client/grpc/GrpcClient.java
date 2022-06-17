/*
 * Copyright 1999-2020 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.nacos.common.remote.client.grpc;

import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.grpc.auto.BiRequestStreamGrpc;
import com.alibaba.nacos.api.grpc.auto.Payload;
import com.alibaba.nacos.api.grpc.auto.RequestGrpc;
import com.alibaba.nacos.api.remote.request.ConnectionSetupRequest;
import com.alibaba.nacos.api.remote.request.Request;
import com.alibaba.nacos.api.remote.request.ServerCheckRequest;
import com.alibaba.nacos.api.remote.response.ErrorResponse;
import com.alibaba.nacos.api.remote.response.Response;
import com.alibaba.nacos.api.remote.response.ServerCheckResponse;
import com.alibaba.nacos.common.remote.ConnectionType;
import com.alibaba.nacos.common.remote.client.Connection;
import com.alibaba.nacos.common.remote.client.RpcClient;
import com.alibaba.nacos.common.remote.client.RpcClientStatus;
import com.alibaba.nacos.common.utils.LoggerUtils;
import com.alibaba.nacos.common.utils.ThreadFactoryBuilder;
import com.alibaba.nacos.common.utils.ThreadUtils;
import com.alibaba.nacos.common.utils.VersionUtils;
import com.google.common.util.concurrent.ListenableFuture;
import io.grpc.CompressorRegistry;
import io.grpc.DecompressorRegistry;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * gRPC Client.
 *
 * @author liuzunfei
 * @version $Id: GrpcClient.java, v 0.1 2020年07月13日 9:16 PM liuzunfei Exp $
 */
@SuppressWarnings("PMD.AbstractClassShouldStartWithAbstractNamingRule")
public abstract class GrpcClient extends RpcClient {
    
    static final Logger LOGGER = LoggerFactory.getLogger(GrpcClient.class);
    
    protected static final String NACOS_SERVER_GRPC_PORT_OFFSET_KEY = "nacos.server.grpc.port.offset";
    
    private ThreadPoolExecutor grpcExecutor = null;
    
    private Integer threadPoolCoreSize;
    
    private Integer threadPoolMaxSize;
    
    private static final long DEFAULT_MAX_INBOUND_MESSAGE_SIZE = 10 * 1024 * 1024L;
    
    private static final long DEFAULT_KEEP_ALIVE_TIME = 6 * 60 * 1000;
    
    @Override
    public ConnectionType getConnectionType() {
        return ConnectionType.GRPC;
    }
    
    /**
     * Empty constructor.
     */
    public GrpcClient(String name) {
        super(name);
    }
    
    /**
     * Set core size of thread pool.
     *
     * @param threadPoolCoreSize core size of thread pool for grpc.
     */
    public void setThreadPoolCoreSize(Integer threadPoolCoreSize) {
        this.threadPoolCoreSize = threadPoolCoreSize;
    }
    
    /**
     * Set max size of thread pool.
     *
     * @param threadPoolMaxSize max size of thread pool for grpc.
     */
    public void setThreadPoolMaxSize(Integer threadPoolMaxSize) {
        this.threadPoolMaxSize = threadPoolMaxSize;
    }
    
    protected Integer getThreadPoolCoreSize() {
        return threadPoolCoreSize != null ? threadPoolCoreSize : ThreadUtils.getSuitableThreadCount(2);
    }
    
    protected Integer getThreadPoolMaxSize() {
        return threadPoolMaxSize != null ? threadPoolMaxSize : ThreadUtils.getSuitableThreadCount(8);
    }
    
    protected ThreadPoolExecutor createGrpcExecutor(String serverIp) {
        ThreadPoolExecutor grpcExecutor = new ThreadPoolExecutor(
                getThreadPoolCoreSize(),
                getThreadPoolMaxSize(),
                10L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(10000),
                new ThreadFactoryBuilder()
                        .daemon(true)
                        .nameFormat("nacos-grpc-client-executor-" + serverIp + "-%d")
                        .build());
        grpcExecutor.allowCoreThreadTimeOut(true);
        return grpcExecutor;
    }
    
    @Override
    public void shutdown() throws NacosException {
        super.shutdown();
        if (grpcExecutor != null) {
            LOGGER.info("Shutdown grpc executor " + grpcExecutor);
            grpcExecutor.shutdown();
        }
    }

    /**
     * Create a stub using a channel.
     *
     * @param managedChannelTemp channel.
     * @return if server check success,return a non-null stub.
     */
    private RequestGrpc.RequestFutureStub createNewChannelStub(ManagedChannel managedChannelTemp) {
        return RequestGrpc.newFutureStub(managedChannelTemp);
    }

    /**
     * create a new channel with specific server address.
     *
     * @param serverIp   serverIp.
     * @param serverPort serverPort.
     * @return if server check success,return a non-null channel.
     */
    private ManagedChannel createNewManagedChannel(String serverIp, int serverPort) {
        ManagedChannelBuilder<?> managedChannelBuilder = ManagedChannelBuilder.forAddress(serverIp, serverPort).executor(grpcExecutor)
                .compressorRegistry(CompressorRegistry.getDefaultInstance()).decompressorRegistry(DecompressorRegistry.getDefaultInstance())
                .maxInboundMessageSize(getInboundMessageSize()).keepAliveTime(keepAliveTimeMillis(), TimeUnit.MILLISECONDS).usePlaintext();
        return managedChannelBuilder.build();
    }
    
    private int getInboundMessageSize() {
        String messageSize = System.getProperty("nacos.remote.client.grpc.maxinbound.message.size",
                String.valueOf(DEFAULT_MAX_INBOUND_MESSAGE_SIZE));
        return Integer.parseInt(messageSize);
    }
    
    private int keepAliveTimeMillis() {
        String keepAliveTimeMillis = System
                .getProperty("nacos.remote.grpc.keep.alive.millis", String.valueOf(DEFAULT_KEEP_ALIVE_TIME));
        return Integer.parseInt(keepAliveTimeMillis);
    }
    
    /**
     * shutdown a  channel.
     *
     * @param managedChannel channel to be shutdown.
     */
    private void shuntDownChannel(ManagedChannel managedChannel) {
        if (managedChannel != null && !managedChannel.isShutdown()) {
            managedChannel.shutdownNow();
        }
    }
    
    /**
     * check server if success.
     *
     * @param requestBlockingStub requestBlockingStub used to check server.
     * @return success or not
     */
    private Response serverCheck(String ip, int port, RequestGrpc.RequestFutureStub requestBlockingStub) {
        try {
            if (requestBlockingStub == null) {
                return null;
            }
            ServerCheckRequest serverCheckRequest = new ServerCheckRequest();
            Payload grpcRequest = GrpcUtils.convert(serverCheckRequest);
            ListenableFuture<Payload> responseFuture = requestBlockingStub.request(grpcRequest);
            Payload response = responseFuture.get(3000L, TimeUnit.MILLISECONDS);
            //receive connection unregister response here,not check response is success.
            return (Response) GrpcUtils.parse(response);
        } catch (Exception e) {
            LoggerUtils.printIfErrorEnabled(LOGGER,
                    "Server check fail, please check server {} ,port {} is available , error ={}", ip, port, e);
            return null;
        }
    }
    
    private StreamObserver<Payload> bindRequestStream(final BiRequestStreamGrpc.BiRequestStreamStub streamStub,
            final GrpcConnection grpcConn) {
        
        return streamStub.requestBiStream(new StreamObserver<Payload>() {
            
            @Override
            public void onNext(Payload payload) {
                
                LoggerUtils.printIfDebugEnabled(LOGGER, "[{}]Stream server request receive, original info: {}",
                        grpcConn.getConnectionId(), payload.toString());
                try {
                    Object parseBody = GrpcUtils.parse(payload);
                    final Request request = (Request) parseBody;
                    if (request != null) {
                        
                        try {
                            Response response = handleServerRequest(request);
                            if (response != null) {
                                response.setRequestId(request.getRequestId());
                                sendResponse(response);
                            } else {
                                LOGGER.warn("[{}]Fail to process server request, ackId->{}", grpcConn.getConnectionId(),
                                        request.getRequestId());
                            }
                            
                        } catch (Exception e) {
                            LoggerUtils.printIfErrorEnabled(LOGGER, "[{}]Handle server request exception: {}",
                                    grpcConn.getConnectionId(), payload.toString(), e.getMessage());
                            Response errResponse = ErrorResponse
                                    .build(NacosException.CLIENT_ERROR, "Handle server request error");
                            errResponse.setRequestId(request.getRequestId());
                            sendResponse(errResponse);
                        }
                        
                    }
                    
                } catch (Exception e) {
                    
                    LoggerUtils.printIfErrorEnabled(LOGGER, "[{}]Error to process server push response: {}",
                            grpcConn.getConnectionId(), payload.getBody().getValue().toStringUtf8());
                }
            }
            
            @Override
            public void onError(Throwable throwable) {
                boolean isRunning = isRunning();
                boolean isAbandon = grpcConn.isAbandon();
                if (isRunning && !isAbandon) {
                    LoggerUtils.printIfErrorEnabled(LOGGER, "[{}]Request stream error, switch server,error={}",
                            grpcConn.getConnectionId(), throwable);
                    if (rpcClientStatus.compareAndSet(RpcClientStatus.RUNNING, RpcClientStatus.UNHEALTHY)) {
                        switchServerAsync();
                    }
                    
                } else {
                    LoggerUtils.printIfWarnEnabled(LOGGER, "[{}]Ignore error event,isRunning:{},isAbandon={}",
                            grpcConn.getConnectionId(), isRunning, isAbandon);
                }
                
            }
            
            @Override
            public void onCompleted() {
                boolean isRunning = isRunning();
                boolean isAbandon = grpcConn.isAbandon();
                if (isRunning && !isAbandon) {
                    LoggerUtils.printIfErrorEnabled(LOGGER, "[{}]Request stream onCompleted, switch server",
                            grpcConn.getConnectionId());
                    if (rpcClientStatus.compareAndSet(RpcClientStatus.RUNNING, RpcClientStatus.UNHEALTHY)) {
                        switchServerAsync();
                    }
                    
                } else {
                    LoggerUtils.printIfInfoEnabled(LOGGER, "[{}]Ignore complete event,isRunning:{},isAbandon={}",
                            grpcConn.getConnectionId(), isRunning, isAbandon);
                }
                
            }
        });
    }
    
    private void sendResponse(Response response) {
        try {
            ((GrpcConnection) this.currentConnection).sendResponse(response);
        } catch (Exception e) {
            LOGGER.error("[{}]Error to send ack response, ackId->{}", this.currentConnection.getConnectionId(),
                    response.getRequestId());
        }
    }

    // 引入了基于 gRPC 的长连接模型来提升配置监听的性能, 客户端和服务端会建立长连接来监听配置的变更, 一旦服务端有配置变更, 就会将配置信息推送到客户端中
    @Override
    public Connection connectToServer(ServerInfo serverInfo) {
        try {
            if (grpcExecutor == null) {
                this.grpcExecutor = createGrpcExecutor(serverInfo.getServerIp());
            }
            int port = serverInfo.getServerPort() + rpcPortOffset();
            //创建一个新的通道
            ManagedChannel managedChannel = createNewManagedChannel(serverInfo.getServerIp(), port);
            // 创建一个 Grpc 的 Stub
            RequestGrpc.RequestFutureStub newChannelStubTemp = createNewChannelStub(managedChannel);
            if (newChannelStubTemp != null) {
                // 检查服务端是否可用
                Response response = serverCheck(serverInfo.getServerIp(), port, newChannelStubTemp);
                if (response == null || !(response instanceof ServerCheckResponse)) {
                    shuntDownChannel(managedChannel);
                    return null;
                }
                // 创建一个 Grpc 的 Stream
                BiRequestStreamGrpc.BiRequestStreamStub biRequestStreamStub = BiRequestStreamGrpc
                        .newStub(newChannelStubTemp.getChannel());
                // 创建连接信息, 保存 Grpc 的连接信息, 也就是长连接的一个 holder
                GrpcConnection grpcConn = new GrpcConnection(serverInfo, grpcExecutor);
                grpcConn.setConnectionId(((ServerCheckResponse) response).getConnectionId());
                
                //create stream request and bind connection event to this connection.
                // 创建 stream 请求同时绑定到当前连接中
                StreamObserver<Payload> payloadStreamObserver = bindRequestStream(biRequestStreamStub, grpcConn);
                
                // stream observer to send response to server
                // 绑定 Grpc 相关连接信息
                grpcConn.setPayloadStreamObserver(payloadStreamObserver);
                grpcConn.setGrpcFutureServiceStub(newChannelStubTemp);
                grpcConn.setChannel(managedChannel);
                //send a  setup request.
                // 发送一个初始化连接请求, 用于上报客户端的一些信息, 例如标签, 客户端版本等
                ConnectionSetupRequest conSetupRequest = new ConnectionSetupRequest();
                conSetupRequest.setClientVersion(VersionUtils.getFullClientVersion());
                conSetupRequest.setLabels(super.getLabels());
                conSetupRequest.setAbilities(super.clientAbilities);
                conSetupRequest.setTenant(super.getTenant());
                grpcConn.sendRequest(conSetupRequest);
                //wait to register connection setup
                // 等待连接建立成功
                Thread.sleep(100L);
                return grpcConn;
            }
            return null;
        } catch (Exception e) {
            LOGGER.error("[{}]Fail to connect to server!,error={}", GrpcClient.this.getName(), e);
        }
        return null;
    }
    
}



