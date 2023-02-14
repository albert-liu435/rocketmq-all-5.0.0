/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.remoting.netty;

/**
 * netty服务配置信息
 */
public class NettyServerConfig implements Cloneable {

    /**
     * 绑定的IP地址
     * Bind address may be hostname, IPv4 or IPv6.
     * By default, it's wildcard address, listening all network interfaces.
     */
    private String bindAddress = "0.0.0.0";
    //监听端口，默认初始化为9876
    private int listenPort = 0;
    //含义：业务线程池的线程个数，RocketMQ 按任务类型，每个任务类型会拥有一个专门的线程池，比如发送消息，消费消息，另外再加一个其他线程池（默认的业务线程池）。默认业务线程池，采用 fixed 类型，其线程名称：RemotingExecutorThread_。
    //
    //作用范围：该参数目前主要用于 NameServer 的默认业务线程池，处理诸如 broker、producer,consume 与 NameServer 的所有交互命令。
    //————————————————
    //版权声明：本文为CSDN博主「中间件兴趣圈」的原创文章，遵循CC 4.0 BY-SA版权协议，转载请附上原文出处链接及本声明。
    //原文链接：https://blog.csdn.net/prestigeding/article/details/75269290
    private int serverWorkerThreads = 8;
    //含义：Netty public 任务线程池个数。线程名称：NettyServerPublicExecutor_。
    //Netty网络会根据业务类型创建不同的线程池，比如处理消息发送、消息消费、心跳检测等。如果该业务类型未注册线程池，则由public线程池执行。
    private int serverCallbackExecutorThreads = 0;
    //含义：Netty IO 线程个数，Selector 所在的线程个数，也就主从 Reactor 模型中的从 Reactor 线程数量 。
    //
    //线程名称：NettyServerNIOSelector_。
    //
    //作用范围：broker,product,consume 服务端的IO线程数量。
    //主要是NameServer、Broker端解析请求、返回相应的线程个数。这类线程主要用于处理网络请求，先解析请求包，然后转发到各个业务线程池完成具体的业务操作，最后将结果返回给调用方
    private int serverSelectorThreads = 3;
    //含义：服务端 oneWay(单向执行)、异步调用的信号量（并发度）。
    //备注：单向（Oneway）发送特点为只负责发送消息，不等待服务器回应且没有回调函数触发，即只发送请求不等待应答。
    //
    //应用场景：适用于某些耗时非常短，但对可靠性要求并不高的场景，例如日志收集。
    //send oneway消息请求的并发度(Broker端参数)
    private int serverOnewaySemaphoreValue = 256;
    //异步消息发送的最大并发度(Broker端参数)
    private int serverAsyncSemaphoreValue = 64;
    // 通道空闲时间，默认120S, 通过Netty的IdleStateHandler实现
    //网络连接最大空闲时间，默认为120s,如果连接空闲时间超过该参数设置的值，连接将被关闭
    private int serverChannelMaxIdleTimeSeconds = 120;
    // 网络socket发送缓存区大小，默认为64k
    private int serverSocketSndBufSize = NettySystemConfig.socketSndbufSize;
    // 网络socket接收缓存区大小，默认为64k
    private int serverSocketRcvBufSize = NettySystemConfig.socketRcvbufSize;
    private int writeBufferHighWaterMark = NettySystemConfig.writeBufferHighWaterMark;
    private int writeBufferLowWaterMark = NettySystemConfig.writeBufferLowWaterMark;
    private int serverSocketBacklog = NettySystemConfig.socketBacklog;
    // 是否使用PooledByteBuf(可重用，缓存ByteBuf)
    private boolean serverPooledByteBufAllocatorEnable = true;

    /**
     * make install
     * <p>
     * <p>
     * ../glibc-2.10.1/configure \ --prefix=/usr \ --with-headers=/usr/include \
     * --host=x86_64-linux-gnu \ --build=x86_64-pc-linux-gnu \ --without-gd
     */
    private boolean useEpollNativeSelector = false;

    public String getBindAddress() {
        return bindAddress;
    }

    public void setBindAddress(String bindAddress) {
        this.bindAddress = bindAddress;
    }

    public int getListenPort() {
        return listenPort;
    }

    public void setListenPort(int listenPort) {
        this.listenPort = listenPort;
    }

    public int getServerWorkerThreads() {
        return serverWorkerThreads;
    }

    public void setServerWorkerThreads(int serverWorkerThreads) {
        this.serverWorkerThreads = serverWorkerThreads;
    }

    public int getServerSelectorThreads() {
        return serverSelectorThreads;
    }

    public void setServerSelectorThreads(int serverSelectorThreads) {
        this.serverSelectorThreads = serverSelectorThreads;
    }

    public int getServerOnewaySemaphoreValue() {
        return serverOnewaySemaphoreValue;
    }

    public void setServerOnewaySemaphoreValue(int serverOnewaySemaphoreValue) {
        this.serverOnewaySemaphoreValue = serverOnewaySemaphoreValue;
    }

    public int getServerCallbackExecutorThreads() {
        return serverCallbackExecutorThreads;
    }

    public void setServerCallbackExecutorThreads(int serverCallbackExecutorThreads) {
        this.serverCallbackExecutorThreads = serverCallbackExecutorThreads;
    }

    public int getServerAsyncSemaphoreValue() {
        return serverAsyncSemaphoreValue;
    }

    public void setServerAsyncSemaphoreValue(int serverAsyncSemaphoreValue) {
        this.serverAsyncSemaphoreValue = serverAsyncSemaphoreValue;
    }

    public int getServerChannelMaxIdleTimeSeconds() {
        return serverChannelMaxIdleTimeSeconds;
    }

    public void setServerChannelMaxIdleTimeSeconds(int serverChannelMaxIdleTimeSeconds) {
        this.serverChannelMaxIdleTimeSeconds = serverChannelMaxIdleTimeSeconds;
    }

    public int getServerSocketSndBufSize() {
        return serverSocketSndBufSize;
    }

    public void setServerSocketSndBufSize(int serverSocketSndBufSize) {
        this.serverSocketSndBufSize = serverSocketSndBufSize;
    }

    public int getServerSocketRcvBufSize() {
        return serverSocketRcvBufSize;
    }

    public void setServerSocketRcvBufSize(int serverSocketRcvBufSize) {
        this.serverSocketRcvBufSize = serverSocketRcvBufSize;
    }

    public int getServerSocketBacklog() {
        return serverSocketBacklog;
    }

    public void setServerSocketBacklog(int serverSocketBacklog) {
        this.serverSocketBacklog = serverSocketBacklog;
    }

    public boolean isServerPooledByteBufAllocatorEnable() {
        return serverPooledByteBufAllocatorEnable;
    }

    public void setServerPooledByteBufAllocatorEnable(boolean serverPooledByteBufAllocatorEnable) {
        this.serverPooledByteBufAllocatorEnable = serverPooledByteBufAllocatorEnable;
    }

    public boolean isUseEpollNativeSelector() {
        return useEpollNativeSelector;
    }

    public void setUseEpollNativeSelector(boolean useEpollNativeSelector) {
        this.useEpollNativeSelector = useEpollNativeSelector;
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        return (NettyServerConfig) super.clone();
    }

    public int getWriteBufferLowWaterMark() {
        return writeBufferLowWaterMark;
    }

    public void setWriteBufferLowWaterMark(int writeBufferLowWaterMark) {
        this.writeBufferLowWaterMark = writeBufferLowWaterMark;
    }

    public int getWriteBufferHighWaterMark() {
        return writeBufferHighWaterMark;
    }

    public void setWriteBufferHighWaterMark(int writeBufferHighWaterMark) {
        this.writeBufferHighWaterMark = writeBufferHighWaterMark;
    }
}
