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
package org.apache.rocketmq.store;

import java.util.concurrent.atomic.AtomicLong;

public abstract class ReferenceResource {
    protected final AtomicLong refCount = new AtomicLong(1);
    protected volatile boolean available = true;
    protected volatile boolean cleanupOver = false;
    private volatile long firstShutdownTimestamp = 0;

//    文件的引用hold和释放release
// 在数据提交commit和刷新flush的时候都会跟这两个方法有关系。首先会获取文件的引用，在处理完之后释放。hold和release方法在MappedFile的父类ReferenceResource类中定义的
//
//    作者：szhlcy
//    链接：https://www.jianshu.com/p/2aad2044980d
//    来源：简书
//    著作权归作者所有。商业转载请联系作者获得授权，非商业转载请注明出处。


    public synchronized boolean hold() {
        //是否可用
        if (this.isAvailable()) {
            //获取引用的数量，如果大于0说明存在引用，然后增加引用
            if (this.refCount.getAndIncrement() > 0) {
                return true;
            } else {
                //否则减少
                this.refCount.getAndDecrement();
            }
        }

        return false;
    }

    public boolean isAvailable() {
        return this.available;
    }

    /**
     * 关闭MappedFile。初次调用时this.available为true，
     * 设置available为false，并设置初次关闭的时间戳
     * （firstShutdownTimestamp）为当前时间戳。调用release()方法尝试
     * 释放资源，release只有在引用次数小于1的情况下才会释放资源。如
     * 果引用次数大于0，对比当前时间与firstShutdownTimestamp，如果已
     * 经超过了其最大拒绝存活期，则每执行一次引用操作，引用数减少
     * 1000，直到引用数小于0时通过执行realse()方法释放资源
     *
     * @param intervalForcibly
     */
    public void shutdown(final long intervalForcibly) {
        if (this.available) {
            this.available = false;
            this.firstShutdownTimestamp = System.currentTimeMillis();
            this.release();
        } else if (this.getRefCount() > 0) {
            if ((System.currentTimeMillis() - this.firstShutdownTimestamp) >= intervalForcibly) {
                this.refCount.set(-1000 - this.getRefCount());
                this.release();
            }
        }
    }

    /**
     * 将引用次数减1，如果引用数小于、等于0，则执行cleanup()方
     * 法，下面重点分析cleanup()方法的实现
     */
    public void release() {
        //减少文件的引用
        long value = this.refCount.decrementAndGet();
        if (value > 0)
            return;
        //如果没有引用了，就可以释放对应的缓冲和内存映射
        synchronized (this) {

            this.cleanupOver = this.cleanup(value);
        }
    }

    public long getRefCount() {
        return this.refCount.get();
    }

    public abstract boolean cleanup(final long currentRef);

    /**
     * 判断是否清理完成，判断标准是引用次数小于、等于0并
     * 且cleanupOver为true，cleanupOver为true的触发条件是release成功
     * 将MappedByteBuffer资源释放了，如代码清单4-26所示。稍后详细分
     * 析release()方法。
     *
     * @return
     */
    public boolean isCleanupOver() {
        return this.refCount.get() <= 0 && this.cleanupOver;
    }
}
