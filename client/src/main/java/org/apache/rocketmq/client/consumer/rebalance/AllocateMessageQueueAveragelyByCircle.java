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
package org.apache.rocketmq.client.consumer.rebalance;

import java.util.ArrayList;
import java.util.List;

import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.common.message.MessageQueue;

/**
 * 2）AllocateMessageQueueAveragelyByCircle：平均轮询分配，
 * 推荐使用。
 * 举例来说，如果现在有8个消息消费队列q1、q2、q3、q4、q5、
 * q6、q7、q8，有3个消费者c1、c2、c3，那么根据该负载算法，消息队
 * 列分配如下。
 * c1：q1、q4、q7。
 * c2：q2、q5、q8。
 * c3：q3、q6。
 * Cycle average Hashing queue algorithm
 */
public class AllocateMessageQueueAveragelyByCircle extends AbstractAllocateMessageQueueStrategy {

    public AllocateMessageQueueAveragelyByCircle() {
        log = ClientLogger.getLog();
    }

    public AllocateMessageQueueAveragelyByCircle(InternalLogger log) {
        super(log);
    }

    @Override
    public List<MessageQueue> allocate(String consumerGroup, String currentCID, List<MessageQueue> mqAll,
                                       List<String> cidAll) {

        List<MessageQueue> result = new ArrayList<MessageQueue>();
        if (!check(consumerGroup, currentCID, mqAll, cidAll)) {
            return result;
        }

        int index = cidAll.indexOf(currentCID);
        for (int i = index; i < mqAll.size(); i++) {
            if (i % cidAll.size() == index) {
                result.add(mqAll.get(i));
            }
        }
        return result;
    }

    @Override
    public String getName() {
        return "AVG_BY_CIRCLE";
    }
}
