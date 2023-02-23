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
package org.apache.rocketmq.client.consumer;

import java.util.List;

import org.apache.rocketmq.common.message.MessageQueue;

/**
 * 集群模式下消息队列的负载
 * 策略。
 * 即分配算法
 * RocketMQ默认提供5种分配算法。
 * <p>
 * 消息负载算法如果没有特殊的要求，尽量使用
 * AllocateMessageQueueAveragely、Al
 * locateMessageQueueAveragelyByCircle，这是因为分配算法比较直
 * 观。消息队列分配原则为一个消费者可以分配多个消息队列，但同一
 * 个消息队列只会分配给一个消费者，故如果消费者个数大于消息队列
 * 数量，则有些消费者无法消费消息。
 * 对比消息队列是否发生变化，主要思路是遍历当前负载队列集
 * 合，如果队列不在新分配队列的集合中，需要将该队列停止消费并保
 * 存消费进度；遍历已分配的队列，如果队列不在队列负载表中
 * （processQueueTable），则需要创建该队列拉取任务PullRequest，
 * 然后添加到PullMessageService线程的pullRequestQueue中，
 * PullMessageService才会继续拉取任务，
 * <p>
 * <p>
 * Strategy Algorithm for message allocating between consumers
 */
public interface AllocateMessageQueueStrategy {

    /**
     * Allocating by consumer id
     *
     * @param consumerGroup current consumer group
     * @param currentCID    current consumer id
     * @param mqAll         message queue set in current topic
     * @param cidAll        consumer set in current consumer group
     * @return The allocate result of given strategy
     */
    List<MessageQueue> allocate(
            final String consumerGroup,
            final String currentCID,
            final List<MessageQueue> mqAll,
            final List<String> cidAll
    );

    /**
     * Algorithm name
     *
     * @return The strategy name
     */
    String getName();
}
