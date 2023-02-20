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
package org.apache.rocketmq.common.message;

/**
 * 消息请求模式
 * Message Request Mode
 */
public enum MessageRequestMode {

    /**
     * 拉取模式
     * pull
     */
    PULL("PULL"),

    /**
     * 在pop模式下工作的消费者可以共享MessageQueue
     * pop, consumer working in pop mode could share MessageQueue
     */
    POP("POP");

    private String name;

    MessageRequestMode(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }
}
