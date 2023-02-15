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

package org.apache.rocketmq.client.latency;

import org.apache.rocketmq.client.impl.producer.TopicPublishInfo;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.logging.InternalLogger;

/**
 * 消息失败策略，延迟实现的门面类。
 */
public class MQFaultStrategy {
    private final static InternalLogger log = ClientLogger.getLog();
    private final LatencyFaultTolerance<String> latencyFaultTolerance = new LatencyFaultToleranceImpl();
    //启用Broker故障延迟机制,默认为不启用
    private boolean sendLatencyFaultEnable = false;
    //latencyMax：最大延迟时间数值，在消息发送之前，先记录当前时间（start），然后消息发送成功或失败时记录当前时间（end），(end-start)代表一次消息延迟时间，发送错误时，updateFaultItem 中 isolation 为 true，与 latencyMax 中值进行比较时得值为 30s,也就时该 broke r在接下来得 600000L，也就时5分钟内不提供服务，等待该 Broker 的恢复。
    //————————————————
    //版权声明：本文为CSDN博主「中间件兴趣圈」的原创文章，遵循CC 4.0 BY-SA版权协议，转载请附上原文出处链接及本声明。
    //原文链接：https://blog.csdn.net/prestigeding/article/details/75799003
    private long[] latencyMax = {50L, 100L, 550L, 1000L, 2000L, 3000L, 15000L};
    private long[] notAvailableDuration = {0L, 0L, 30000L, 60000L, 120000L, 180000L, 600000L};

    public long[] getNotAvailableDuration() {
        return notAvailableDuration;
    }

    public void setNotAvailableDuration(final long[] notAvailableDuration) {
        this.notAvailableDuration = notAvailableDuration;
    }

    public long[] getLatencyMax() {
        return latencyMax;
    }

    public void setLatencyMax(final long[] latencyMax) {
        this.latencyMax = latencyMax;
    }

    public boolean isSendLatencyFaultEnable() {
        return sendLatencyFaultEnable;
    }

    public void setSendLatencyFaultEnable(final boolean sendLatencyFaultEnable) {
        this.sendLatencyFaultEnable = sendLatencyFaultEnable;
    }

    /**
     * 选择消息队列
     * 在消息发送过程中，可能会多次执行选择消息队列这个方法，
     * lastBrokerName就是上一次选择的执行发送消息失败的Broker。第一
     * 次执行消息队列选择时，lastBrokerName为null，此时直接用
     * sendWhichQueue自增再获取值，与当前路由表中消息队列的个数取
     * 模，返回该位置的MessageQueue(selectOneMessageQueue()方法，如
     * 果消息发送失败，下次进行消息队列选择时规避上次MesageQueue所在
     * 的Broker，否则有可能再次失败。
     *
     * @param tpInfo
     * @param lastBrokerName
     * @return
     */
    public MessageQueue selectOneMessageQueue(final TopicPublishInfo tpInfo, final String lastBrokerName) {
        //要理解代码@2，@3 处的逻辑，我们就需要理解 RocketMQ 发送消息延迟机制，具体实现类：MQFaultStrategy。


        //sendLatencyFaultEnable，是否开启消息失败延迟规避机制，该值在消息发送者那里可以设置，如果该值为false,直接从 topic 的所有队列中选择下一个，而不考虑该消息队列是否可用（比如Broker挂掉）
        if (this.sendLatencyFaultEnable) {
            try {
                //代码@2-start--end,这里使用了本地线程变量 ThreadLocal 保存上一次发送的消息队列下标，消息发送使用轮询机制获取下一个发送消息队列。
                //代码@2对 topic 所有的消息队列进行一次验证，为什么要循环呢？因为加入了发送异常延迟，要确保选中的消息队列(MessageQueue)所在的Broker是正常的。
                int index = tpInfo.getSendWhichQueue().incrementAndGet();//@2 start
                //遍历消息队列的数量
                for (int i = 0; i < tpInfo.getMessageQueueList().size(); i++) {
                    //进行选择算法
                    int pos = Math.abs(index++) % tpInfo.getMessageQueueList().size();
                    if (pos < 0)
                        pos = 0;
                    //选择消息队列
                    MessageQueue mq = tpInfo.getMessageQueueList().get(pos);//@2 end
                    //判断当前的消息队列是否可用。
                    //从@2--@3，一旦一个 MessageQueue 符合条件，即刻返回，但该 Topic 所在的所 有Broker全部标记不可用时，进入到下一步逻辑处理。（在此处，我们要知道，标记为不可用，并不代表真的不可用，Broker 是可以在故障期间被运营管理人员进行恢复的，比如重启）
                    //判断消息队列是否可用
                    if (latencyFaultTolerance.isAvailable(mq.getBrokerName()))//@3
                        return mq;
                }
                //代码@4，5：根据 Broker 的 startTimestart 进行一个排序，值越小，排前面，然后再选择一个，返回（此时不能保证一定可用，会抛出异常，如果消息发送方式是同步调用，则有重试机制）。
                final String notBestBroker = latencyFaultTolerance.pickOneAtLeast();//@4
                int writeQueueNums = tpInfo.getQueueIdByBroker(notBestBroker);//@5 start
                if (writeQueueNums > 0) {
                    final MessageQueue mq = tpInfo.selectOneMessageQueue();
                    if (notBestBroker != null) {
                        mq.setBrokerName(notBestBroker);
                        mq.setQueueId(tpInfo.getSendWhichQueue().incrementAndGet() % writeQueueNums);
                    }
                    return mq;
                } else {
                    latencyFaultTolerance.remove(notBestBroker); //@5 end
                }
            } catch (Exception e) {
                log.error("Error occurred when selecting message queue", e);
            }

            return tpInfo.selectOneMessageQueue();
        }

        return tpInfo.selectOneMessageQueue(lastBrokerName); //@6
    }

    /**
     * 如果isolation为true，则使用30s作为
     * computeNotAvailableDuration方法的参数。如果isolation为false，
     * 则使用本次消息发送时延作为computeNotAvailableDuration方法的参
     * 数。
     * computeNotAvailableDuration的作用是计算因本次消息发送故障
     * 需要规避Broker的时长，也就是接下来多长的时间内，该Broker将不
     * 参与消息发送队列负载。具体算法是，从latencyMax数组尾部开始寻
     * 找，找到第一个比currentLatency小的下标，然后从
     * notAvailableDuration数组中获取需要规避的时长，该方法最终调用
     * LatencyFaultTolerance的updateFaultItem()方法
     *
     * @param brokerName
     * @param currentLatency
     * @param isolation
     */
    public void updateFaultItem(final String brokerName, final long currentLatency, boolean isolation) {
        if (this.sendLatencyFaultEnable) {
            long duration = computeNotAvailableDuration(isolation ? 30000 : currentLatency);
            this.latencyFaultTolerance.updateFaultItem(brokerName, currentLatency, duration);
        }
    }

    private long computeNotAvailableDuration(final long currentLatency) {
        for (int i = latencyMax.length - 1; i >= 0; i--) {
            if (currentLatency >= latencyMax[i])
                return this.notAvailableDuration[i];
        }

        return 0;
    }
}
