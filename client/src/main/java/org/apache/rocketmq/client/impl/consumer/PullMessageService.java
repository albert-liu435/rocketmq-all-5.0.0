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
package org.apache.rocketmq.client.impl.consumer;

import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.message.MessageRequestMode;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.logging.InternalLogger;

/**
 * 拉取消息服务
 * <p>
 * 一个MQ客户端（MQClientInstance）只会创建一个消息拉取线程
 * 向Broker拉取消息，并且同一时间只会拉取一个topic中的一个队列，
 * 拉取线程一次向Broker拉取一批消息后，会提交到消费组的线程池，
 * 然后“不知疲倦”地向Broker发起下一个拉取请求。
 * RocketMQ客户端为每一个消费组创建独立的消费线程池，即在并
 * 发消费模式下，单个消费组内的并发度为线程池线程个数。线程池处
 * 理一批消息后会向Broker汇报消息消费进度。
 */
public class PullMessageService extends ServiceThread {
    private final InternalLogger log = ClientLogger.getLog();
    //消息请求阻塞队列
    //原来，PullMessageService提供了延迟添加与立即添加两种方式
    //将PullRequest放入pullRequestQueue。那么PullRequest是在什么时
    //候创建的呢？executePullRequestImmediately方法调用
    private final LinkedBlockingQueue<MessageRequest> messageRequestQueue = new LinkedBlockingQueue<MessageRequest>();

    private final MQClientInstance mQClientFactory;
    private final ScheduledExecutorService scheduledExecutorService = Executors
            .newSingleThreadScheduledExecutor(new ThreadFactory() {
                @Override
                public Thread newThread(Runnable r) {
                    return new Thread(r, "PullMessageServiceScheduledThread");
                }
            });

    public PullMessageService(MQClientInstance mQClientFactory) {
        this.mQClientFactory = mQClientFactory;
    }

    /**
     * 延迟将PullRequest添加到pullRequestQueue
     * @param pullRequest
     * @param timeDelay
     */
    public void executePullRequestLater(final PullRequest pullRequest, final long timeDelay) {
        if (!isStopped()) {
            this.scheduledExecutorService.schedule(new Runnable() {
                @Override
                public void run() {
                    PullMessageService.this.executePullRequestImmediately(pullRequest);
                }
            }, timeDelay, TimeUnit.MILLISECONDS);
        } else {
            log.warn("PullMessageServiceScheduledThread has shutdown");
        }
    }

    /**
     * 通过跟踪发现，主要有两个地方会调用
     * executePullRequestImmediately：一个是在RocketMQ根据
     * PullRequest拉取任务执行完一次消息拉取任务后，又将PullRequest
     * 对象放入pullRequestQueue；另一个是在RebalanceImpl中创建的。
     * RebalanceImpl是5.5节要重点介绍的消息队列负载机制，也就是
     * PullRequest对象真正创建的地方。
     *
     * @param pullRequest
     */
    public void executePullRequestImmediately(final PullRequest pullRequest) {
        try {
            this.messageRequestQueue.put(pullRequest);
        } catch (InterruptedException e) {
            log.error("executePullRequestImmediately pullRequestQueue.put", e);
        }
    }

    public void executePopPullRequestLater(final PopRequest pullRequest, final long timeDelay) {
        if (!isStopped()) {
            this.scheduledExecutorService.schedule(new Runnable() {
                @Override
                public void run() {
                    PullMessageService.this.executePopPullRequestImmediately(pullRequest);
                }
            }, timeDelay, TimeUnit.MILLISECONDS);
        } else {
            log.warn("PullMessageServiceScheduledThread has shutdown");
        }
    }

    public void executePopPullRequestImmediately(final PopRequest pullRequest) {
        try {
            this.messageRequestQueue.put(pullRequest);
        } catch (InterruptedException e) {
            log.error("executePullRequestImmediately pullRequestQueue.put", e);
        }
    }

    public void executeTaskLater(final Runnable r, final long timeDelay) {
        if (!isStopped()) {
            this.scheduledExecutorService.schedule(r, timeDelay, TimeUnit.MILLISECONDS);
        } else {
            log.warn("PullMessageServiceScheduledThread has shutdown");
        }
    }

    public ScheduledExecutorService getScheduledExecutorService() {
        return scheduledExecutorService;
    }

    /**
     * 消息拉取
     * 根据消费组名从MQClientInstance中获取消费者的内部实现类
     * MQConsumerInner，令人意外的是，这里将consumer强制转换为
     * DefaultMQPushConsumerImpl，也就是PullMessageService，该线程只
     * 为推模式服务，那拉模式如何拉取消息呢？其实细想也不难理解，对
     * 于拉模式，RocketMQ只需要提供拉取消息API，再由应用程序调用
     * API。
     *
     * ProcessQueue是MessageQueue在消费端的重现、快照。
     * PullMessageService从消息服务器默认每次拉取32条消息，按消息队
     * 列偏移量的顺序存放在ProcessQueue中，PullMessageService将消息
     * 提交到消费者消费线程池，消息成功消费后，再从ProcessQueue中移
     * 除。
     *
     * @param pullRequest
     */
    private void pullMessage(final PullRequest pullRequest) {
        //根据消费组名从MQClientInstance中获取消费者的内部实现类
        //MQConsumerInner，令人意外的是，这里将consumer强制转换为
        //DefaultMQPushConsumerImpl，也就是PullMessageService，该线程只
        //为推模式服务，那拉模式如何拉取消息呢？其实细想也不难理解，对
        //于拉模式，RocketMQ只需要提供拉取消息API，再由应用程序调用
        //API。
        final MQConsumerInner consumer = this.mQClientFactory.selectConsumer(pullRequest.getConsumerGroup());
        if (consumer != null) {
            DefaultMQPushConsumerImpl impl = (DefaultMQPushConsumerImpl) consumer;
            impl.pullMessage(pullRequest);
        } else {
            log.warn("No matched consumer for the PullRequest {}, drop it", pullRequest);
        }
    }

    private void popMessage(final PopRequest popRequest) {
        final MQConsumerInner consumer = this.mQClientFactory.selectConsumer(popRequest.getConsumerGroup());
        if (consumer != null) {
            DefaultMQPushConsumerImpl impl = (DefaultMQPushConsumerImpl) consumer;
            impl.popMessage(popRequest);
        } else {
            log.warn("No matched consumer for the PopRequest {}, drop it", popRequest);
        }
    }

    /**
     * PullMessageService消息拉取服务线程，run()方法是其核心逻辑
     */
    @Override
    public void run() {
        log.info(this.getServiceName() + " service started");
        //1）while(!this.isStopped())是一种通用的设计技巧，Stopped
        //声明为volatile，每执行一次业务逻辑，检测一下其运行状态，可以
        //通过其他线程将Stopped设置为true，从而停止该线程。
        while (!this.isStopped()) {
            try {
                //从阻塞队列中获取一个任务
                //从pullRequestQueue中获取一个PullRequest消息拉取任务，
                //如果pullRequestQueue为空，则线程将阻塞，直到有拉取任务被放
                //入。
                MessageRequest messageRequest = this.messageRequestQueue.take();
                if (messageRequest.getMessageRequestMode() == MessageRequestMode.POP) {
                    this.popMessage((PopRequest) messageRequest);
                } else {

                    this.pullMessage((PullRequest) messageRequest);
                }
            } catch (InterruptedException ignored) {
            } catch (Exception e) {
                log.error("Pull Message Service Run Method exception", e);
            }
        }

        log.info(this.getServiceName() + " service end");
    }

    @Override
    public void shutdown(boolean interrupt) {
        super.shutdown(interrupt);
        ThreadUtils.shutdownGracefully(this.scheduledExecutorService, 1000, TimeUnit.MILLISECONDS);
    }

    @Override
    public String getServiceName() {
        return PullMessageService.class.getSimpleName();
    }

}
