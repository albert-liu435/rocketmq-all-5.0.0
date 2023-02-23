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

package org.apache.rocketmq.common.subscription;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.rocketmq.common.MixAll;

public class SubscriptionGroupConfig {

    //1）String groupName：消费组名。
    //2）consumeEnable：是否可以消费，默认为true，如果
    //consumeEnable=false，该消费组无法拉取消息，因而无法消费消息。
    //3）consumeFromMinEnable：是否允许从队列最小偏移量开始消
    //费，默认为true，目前未使用该参数。
    //4）consumeBroadcastEnable：设置该消费组是否能以广播模式消
    //费，默认为true，如果设置为false，表示只能以集群模式消费。
    //5）retryQueueNums：重试队列个数，默认为1，每一个Broker上
    //有一个重试队列。
    //6）retryMaxTimes：消息最大重试次数，默认16次。
    //7）brokerId：主节点ID。
    //8）whichBrokerWhenConsumeSlowly：如果消息堵塞（主节点），
    //将转向该brokerId的服务器上拉取消息，默认为1。
    //9）notifyConsumerIdsChangedEnable：当消费发生变化时，是否
    //立即进行消息队列重新负载。消费组订阅信息配置信息存储在Broker
    //的 ${ROCKET_HOME}/store/config/subscriptionGroup.json中。
    //BrokerConfig.autoCreateSubscriptionGroup默认为true，表示在第
    //一次使用消费组配置信息时如果不存在消费组，则使用上述默认值自
    //动创建一个，如果为false，则只能通过客户端命令mqadmin
    //updateSubGroup创建消费组后再修改相关参数

    private String groupName;

    private boolean consumeEnable = true;
    private boolean consumeFromMinEnable = true;
    private boolean consumeBroadcastEnable = true;
    private boolean consumeMessageOrderly = false;

    private int retryQueueNums = 1;

    private int retryMaxTimes = 16;
    private GroupRetryPolicy groupRetryPolicy = new GroupRetryPolicy();

    private long brokerId = MixAll.MASTER_ID;

    private long whichBrokerWhenConsumeSlowly = 1;

    private boolean notifyConsumerIdsChangedEnable = true;

    private int groupSysFlag = 0;

    // Only valid for push consumer
    private int consumeTimeoutMinute = 15;

    public String getGroupName() {
        return groupName;
    }

    public void setGroupName(String groupName) {
        this.groupName = groupName;
    }

    public boolean isConsumeEnable() {
        return consumeEnable;
    }

    public void setConsumeEnable(boolean consumeEnable) {
        this.consumeEnable = consumeEnable;
    }

    public boolean isConsumeFromMinEnable() {
        return consumeFromMinEnable;
    }

    public void setConsumeFromMinEnable(boolean consumeFromMinEnable) {
        this.consumeFromMinEnable = consumeFromMinEnable;
    }

    public boolean isConsumeBroadcastEnable() {
        return consumeBroadcastEnable;
    }

    public void setConsumeBroadcastEnable(boolean consumeBroadcastEnable) {
        this.consumeBroadcastEnable = consumeBroadcastEnable;
    }

    public boolean isConsumeMessageOrderly() {
        return consumeMessageOrderly;
    }

    public void setConsumeMessageOrderly(boolean consumeMessageOrderly) {
        this.consumeMessageOrderly = consumeMessageOrderly;
    }

    public int getRetryQueueNums() {
        return retryQueueNums;
    }

    public void setRetryQueueNums(int retryQueueNums) {
        this.retryQueueNums = retryQueueNums;
    }

    public int getRetryMaxTimes() {
        return retryMaxTimes;
    }

    public void setRetryMaxTimes(int retryMaxTimes) {
        this.retryMaxTimes = retryMaxTimes;
    }

    public GroupRetryPolicy getGroupRetryPolicy() {
        return groupRetryPolicy;
    }

    public void setGroupRetryPolicy(GroupRetryPolicy groupRetryPolicy) {
        this.groupRetryPolicy = groupRetryPolicy;
    }

    public long getBrokerId() {
        return brokerId;
    }

    public void setBrokerId(long brokerId) {
        this.brokerId = brokerId;
    }

    public long getWhichBrokerWhenConsumeSlowly() {
        return whichBrokerWhenConsumeSlowly;
    }

    public void setWhichBrokerWhenConsumeSlowly(long whichBrokerWhenConsumeSlowly) {
        this.whichBrokerWhenConsumeSlowly = whichBrokerWhenConsumeSlowly;
    }

    public boolean isNotifyConsumerIdsChangedEnable() {
        return notifyConsumerIdsChangedEnable;
    }

    public void setNotifyConsumerIdsChangedEnable(final boolean notifyConsumerIdsChangedEnable) {
        this.notifyConsumerIdsChangedEnable = notifyConsumerIdsChangedEnable;
    }

    public int getGroupSysFlag() {
        return groupSysFlag;
    }

    public void setGroupSysFlag(int groupSysFlag) {
        this.groupSysFlag = groupSysFlag;
    }

    public int getConsumeTimeoutMinute() {
        return consumeTimeoutMinute;
    }

    public void setConsumeTimeoutMinute(int consumeTimeoutMinute) {
        this.consumeTimeoutMinute = consumeTimeoutMinute;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) (brokerId ^ (brokerId >>> 32));
        result = prime * result + (consumeBroadcastEnable ? 1231 : 1237);
        result = prime * result + (consumeEnable ? 1231 : 1237);
        result = prime * result + (consumeFromMinEnable ? 1231 : 1237);
        result = prime * result + (notifyConsumerIdsChangedEnable ? 1231 : 1237);
        result = prime * result + ((groupName == null) ? 0 : groupName.hashCode());
        result = prime * result + retryMaxTimes;
        result = prime * result + retryQueueNums;
        result =
            prime * result + (int) (whichBrokerWhenConsumeSlowly ^ (whichBrokerWhenConsumeSlowly >>> 32));
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        SubscriptionGroupConfig other = (SubscriptionGroupConfig) obj;
        return new EqualsBuilder()
            .append(groupName, other.groupName)
            .append(consumeEnable, other.consumeEnable)
            .append(consumeFromMinEnable, other.consumeFromMinEnable)
            .append(consumeBroadcastEnable, other.consumeBroadcastEnable)
            .append(retryQueueNums, other.retryQueueNums)
            .append(retryMaxTimes, other.retryMaxTimes)
            .append(brokerId, other.brokerId)
            .append(whichBrokerWhenConsumeSlowly, other.whichBrokerWhenConsumeSlowly)
            .append(notifyConsumerIdsChangedEnable, other.notifyConsumerIdsChangedEnable)
            .append(groupSysFlag, other.groupSysFlag)
            .isEquals();
    }

    @Override
    public String toString() {
        return "SubscriptionGroupConfig{" +
            "groupName='" + groupName + '\'' +
            ", consumeEnable=" + consumeEnable +
            ", consumeFromMinEnable=" + consumeFromMinEnable +
            ", consumeBroadcastEnable=" + consumeBroadcastEnable +
            ", consumeMessageOrderly=" + consumeMessageOrderly +
            ", retryQueueNums=" + retryQueueNums +
            ", retryMaxTimes=" + retryMaxTimes +
            ", groupRetryPolicy=" + groupRetryPolicy +
            ", brokerId=" + brokerId +
            ", whichBrokerWhenConsumeSlowly=" + whichBrokerWhenConsumeSlowly +
            ", notifyConsumerIdsChangedEnable=" + notifyConsumerIdsChangedEnable +
            ", groupSysFlag=" + groupSysFlag +
            '}';
    }
}
