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
package org.apache.rocketmq.client.consumer.store;

import java.util.Map;
import java.util.Set;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;

/**
 * 消息消费者在消费一批消息后，需要记录该批消息已经消费完
 * 毕，否则当消费者重新启动时，又要从消息消费队列最开始消费。从
 * 5.6.1节也可以看到，一次消息消费后会从ProcessQueue处理队列中移
 * 除该批消息，返回ProcessQueue的最小偏移量，并存入消息进度表。
 * 那么消息进度文件存储在哪里合适呢？
 * 1）广播模式：同一个消费组的所有消息消费者都需要消费主题下
 * 的所有消息，也就是同组内消费者的消息消费行为是对立的，互相不
 * 影响，故消息进度需要独立存储，最理想的存储地方应该是与消费者
 * 绑定。
 * 2）集群模式：同一个消费组内的所有消息消费者共享消息主题下
 * 的所有消息，同一条消息（同一个消息消费队列）在同一时间只会被
 * 消费组内的一个消费者消费，并且随着消费队列的动态变化而重新负
 * 载，因此消费进度需要保存在每个消费者都能访问到的地方。
 *
 *
 * Offset store interface
 */
public interface OffsetStore {

    //1）void load()：从消息进度存储文件加载消息进度到内存。
    //2）void updateOffset(MessageQueue mq, long offset,
    //boolean increaseOnly)：更新内存中的消息消费进度。
    //MessageQueue mq：消息消费队列。
    //long offset：消息消费偏移量。
    //increaseOnly：true表示offset必须大于内存中当前的消费偏移
    //量才更新。
    //3）long readOffset(final MessageQueue mq, final
    //ReadOffsetType type)：读取消息消费进度。
    //mq：消息消费队列。
    //ReadOffsetType type：读取方式，可选值包括
    //READ_FROM_MEMORY，即从内存中读取，READ_FROM_STORE，即从磁
    //盘中读取，MEMORY_FIRST_THEN_STORE，即先从内存中读取，再从
    //磁盘中读取。
    //4）void persistAll(final Set messageQueue)持久化指定消息
    //队列进度到磁盘。
    //Set messageQueue：消息队列集合。
    //5）void removeOffset(messageQueue mq)：将消息队列的消息消
    //费进度从内存中移除。
    //6）Map cloneOffsetTable(String topic)：复制该主题下所有消
    //息队列的消息消费进度。
    //7）void updateConsumeOffsetToBroker(MessageQueue mq,long
    //offset,boolean isOneway)：使用集群模式更新存储在Broker端的消
    //息消费进度

    /**
     * Load
     */
    void load() throws MQClientException;

    /**
     * Update the offset,store it in memory
     */
    void updateOffset(final MessageQueue mq, final long offset, final boolean increaseOnly);

    /**
     * 从本地读取偏移量
     * Get offset from local storage
     *
     * @return The fetched offset
     */
    long readOffset(final MessageQueue mq, final ReadOffsetType type);

    /**
     * Persist all offsets,may be in local storage or remote name server
     */
    void persistAll(final Set<MessageQueue> mqs);

    /**
     * Persist the offset,may be in local storage or remote name server
     */
    void persist(final MessageQueue mq);

    /**
     * Remove offset
     */
    void removeOffset(MessageQueue mq);

    /**
     * @return The cloned offset table of given topic
     */
    Map<MessageQueue, Long> cloneOffsetTable(String topic);

    /**
     * @param mq
     * @param offset
     * @param isOneway
     */
    void updateConsumeOffsetToBroker(MessageQueue mq, long offset, boolean isOneway) throws RemotingException,
            MQBrokerException, InterruptedException, MQClientException;
}
