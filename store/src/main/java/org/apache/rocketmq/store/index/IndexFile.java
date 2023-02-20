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
package org.apache.rocketmq.store.index;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.List;

import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.logfile.DefaultMappedFile;
import org.apache.rocketmq.store.logfile.MappedFile;

/**
 * Index文件基于物理磁盘文件实现哈希索引。Index文件由40字节
 * 的文件头、500万个哈希槽、2000万个Index条目组成，每个哈希槽4字
 * 节、每个Index条目含有20个字节，分别为4字节索引key的哈希码、8
 * 字节消息物理偏移量、4字节时间戳、4字节的前一个Index条目（哈希
 * 冲突的链表结构）。
 * <p>
 * <p>
 * 消息索引，主要存储消息key与offset的对应关系。
 * <p>
 * <p>
 * IndexFile（索引文件）提供了一种可以通过key或时间区间来查询消息的方法。Index文件的存储位置是：HOME\store\index{fileName}，文件名fileName是以创建时的时间戳命名的，
 * 固定的单个IndexFile文件大小约为400M，一个IndexFile可以保存 2000W个索引，IndexFile的底层存储设计为在文件系统中实现HashMap结构，故rocketmq的索引文件其底层实现为hash索引。
 * <p>
 * 作者：szhlcy
 * 链接：https://www.jianshu.com/p/82521a464424
 * 来源：简书
 * 著作权归作者所有。商业转载请联系作者获得授权，非商业转载请注明出处。
 */
public class IndexFile {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private static int hashSlotSize = 4;
    private static int indexSize = 20;
    private static int invalidIndex = 0;
    private final int hashSlotNum;
    private final int indexNum;
    private final int fileTotalSize;
    private final MappedFile mappedFile;
    private final MappedByteBuffer mappedByteBuffer;
    private final IndexHeader indexHeader;

    public IndexFile(final String fileName, final int hashSlotNum, final int indexNum,
                     final long endPhyOffset, final long endTimestamp) throws IOException {
        this.fileTotalSize =
                IndexHeader.INDEX_HEADER_SIZE + (hashSlotNum * hashSlotSize) + (indexNum * indexSize);
        this.mappedFile = new DefaultMappedFile(fileName, fileTotalSize);
        this.mappedByteBuffer = this.mappedFile.getMappedByteBuffer();
        this.hashSlotNum = hashSlotNum;
        this.indexNum = indexNum;

        ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
        this.indexHeader = new IndexHeader(byteBuffer);

        if (endPhyOffset > 0) {
            this.indexHeader.setBeginPhyOffset(endPhyOffset);
            this.indexHeader.setEndPhyOffset(endPhyOffset);
        }

        if (endTimestamp > 0) {
            this.indexHeader.setBeginTimestamp(endTimestamp);
            this.indexHeader.setEndTimestamp(endTimestamp);
        }
    }

    public String getFileName() {
        return this.mappedFile.getFileName();
    }

    public int getFileSize() {
        return this.fileTotalSize;
    }

    public void load() {
        this.indexHeader.load();
    }

    public void shutdown() {
        this.flush();
        UtilAll.cleanBuffer(this.mappedByteBuffer);
    }

    public void flush() {
        long beginTime = System.currentTimeMillis();
        if (this.mappedFile.hold()) {
            this.indexHeader.updateByteBuffer();
            this.mappedByteBuffer.force();
            this.mappedFile.release();
            log.info("flush index file elapsed time(ms) " + (System.currentTimeMillis() - beginTime));
        }
    }

    public boolean isWriteFull() {
        return this.indexHeader.getIndexCount() >= this.indexNum;
    }

    public boolean destroy(final long intervalForcibly) {
        return this.mappedFile.destroy(intervalForcibly);
    }

    /**
     * RocketMQ将消息索引键与消息偏移量的映射关系写入Index的实现
     * 方法为public boolean putKey（final String key, final long
     * phyOffset, final long storeTimestamp），参数含义分别为消息索
     * 引、消息物理偏移量、消息存储时间
     * <p>
     * <p>
     * 1）计算新添加条目的起始物理偏移量：头部字节长度+哈希槽数
     * 量×单个哈希槽大小（4个字节）+当前Index条目个数×单个Index条
     * 目大小（20个字节）。
     * 2）依次将哈希码、消息物理偏移量、消息存储时间戳与Index文
     * 件时间戳、当前哈希槽的值存入MappedByteBuffer。
     * 3）将当前Index文件中包含的条目数量存入哈希槽中，覆盖原先
     * 哈希槽的值。
     * 以上是哈希冲突链式解决方案的关键实现，哈希槽中存储的是该
     * 哈希码对应的最新Index条目的下标，新的Index条目最后4个字节存储
     * 该哈希码上一个条目的Index下标。如果哈希槽中存储的值为0或大于
     * 当前Index文件最大条目数或小于-1，表示该哈希槽当前并没有与之对
     * 应的Index条目。值得注意的是，Index文件条目中存储的不是消息索
     * 引key，而是消息属性key的哈希，在根据key查找时需要根据消息物理
     * 偏移量找到消息，进而验证消息key的值。之所以只存储哈希，而不存
     * 储具体的key，是为了将Index条目设计为定长结构，才能方便地检索
     * 与定位条目，
     *
     * @param key
     * @param phyOffset
     * @param storeTimestamp
     * @return
     */
    public boolean putKey(final String key, final long phyOffset, final long storeTimestamp) {
        //第一步：当前已使用条目大于、等于允许最大条目数时，返回
        //fasle，表示当前Index文件已写满。如果当前index文件未写满，则根
        //据key算出哈希码。根据keyHash对哈希槽数量取余定位到哈希码对应
        //的哈希槽下标，哈希码对应的哈希槽的物理地址为IndexHeader（40字
        //节）加上下标乘以每个哈希槽的大小（4字节）
        if (this.indexHeader.getIndexCount() < this.indexNum) {
            int keyHash = indexKeyHashMethod(key);
            int slotPos = keyHash % this.hashSlotNum;
            int absSlotPos = IndexHeader.INDEX_HEADER_SIZE + slotPos * hashSlotSize;

            try {
                //读取哈希槽中存储的数据，如果哈希槽存储的数据小于0
                //或大于当前Index文件中的索引条目，则将slotValue设置为0
                int slotValue = this.mappedByteBuffer.getInt(absSlotPos);
                if (slotValue <= invalidIndex || slotValue > this.indexHeader.getIndexCount()) {
                    slotValue = invalidIndex;
                }
                //：计算待存储消息的时间戳与第一条消息时间戳的差值，
                //并转换成秒
                long timeDiff = storeTimestamp - this.indexHeader.getBeginTimestamp();

                timeDiff = timeDiff / 1000;

                if (this.indexHeader.getBeginTimestamp() <= 0) {
                    timeDiff = 0;
                } else if (timeDiff > Integer.MAX_VALUE) {
                    timeDiff = Integer.MAX_VALUE;
                } else if (timeDiff < 0) {
                    timeDiff = 0;
                }
                //将条目信息存储在Index文件中。
                int absIndexPos =
                        IndexHeader.INDEX_HEADER_SIZE + this.hashSlotNum * hashSlotSize
                                + this.indexHeader.getIndexCount() * indexSize;

                this.mappedByteBuffer.putInt(absIndexPos, keyHash);
                this.mappedByteBuffer.putLong(absIndexPos + 4, phyOffset);
                this.mappedByteBuffer.putInt(absIndexPos + 4 + 8, (int) timeDiff);
                this.mappedByteBuffer.putInt(absIndexPos + 4 + 8 + 4, slotValue);

                this.mappedByteBuffer.putInt(absSlotPos, this.indexHeader.getIndexCount());

                if (this.indexHeader.getIndexCount() <= 1) {
                    this.indexHeader.setBeginPhyOffset(phyOffset);
                    this.indexHeader.setBeginTimestamp(storeTimestamp);
                }

                if (invalidIndex == slotValue) {
                    this.indexHeader.incHashSlotCount();
                }
                this.indexHeader.incIndexCount();
                this.indexHeader.setEndPhyOffset(phyOffset);
                this.indexHeader.setEndTimestamp(storeTimestamp);

                return true;
            } catch (Exception e) {
                log.error("putKey exception, Key: " + key + " KeyHashCode: " + key.hashCode(), e);
            }
        } else {
            log.warn("Over index file capacity: index count = " + this.indexHeader.getIndexCount()
                    + "; index max num = " + this.indexNum);
        }

        return false;
    }

    public int indexKeyHashMethod(final String key) {
        int keyHash = key.hashCode();
        int keyHashPositive = Math.abs(keyHash);
        if (keyHashPositive < 0) {
            keyHashPositive = 0;
        }
        return keyHashPositive;
    }

    public long getBeginTimestamp() {
        return this.indexHeader.getBeginTimestamp();
    }

    public long getEndTimestamp() {
        return this.indexHeader.getEndTimestamp();
    }

    public long getEndPhyOffset() {
        return this.indexHeader.getEndPhyOffset();
    }

    public boolean isTimeMatched(final long begin, final long end) {
        boolean result = begin < this.indexHeader.getBeginTimestamp() && end > this.indexHeader.getEndTimestamp();
        result = result || begin >= this.indexHeader.getBeginTimestamp() && begin <= this.indexHeader.getEndTimestamp();
        result = result || end >= this.indexHeader.getBeginTimestamp() && end <= this.indexHeader.getEndTimestamp();
        return result;
    }

    /**
     * 更新文件索引头信息。如果当前文件只包含一个条目，
     * 则更新beginPhyOffset、beginTimestamp、endPyhOffset、
     * endTimestamp以及当前文件使用索引条目等信息，
     *
     * @param phyOffsets 查找到的消息物理偏移量。
     * @param key        索引key
     * @param maxNum     本次查找最大消息条数
     * @param begin      开始时间戳。
     * @param end        结束时间戳
     */
    public void selectPhyOffset(final List<Long> phyOffsets, final String key, final int maxNum,
                                final long begin, final long end) {
        if (this.mappedFile.hold()) {
            //根据key算出key的哈希码，keyHash对哈希槽数量取余，
            //定位到哈希码对应的哈希槽下标，哈希槽的物理地址为
            //IndexHeader（40字节）加上下标乘以每个哈希槽的大小（4字节），
            int keyHash = indexKeyHashMethod(key);
            int slotPos = keyHash % this.hashSlotNum;
            int absSlotPos = IndexHeader.INDEX_HEADER_SIZE + slotPos * hashSlotSize;

            try {
                //如果对应的哈希槽中存储的数据小于1或大于当前索引条
                //目个数，表示该哈希码没有对应的条目，直接返回
                int slotValue = this.mappedByteBuffer.getInt(absSlotPos);
                if (slotValue <= invalidIndex || slotValue > this.indexHeader.getIndexCount()
                        || this.indexHeader.getIndexCount() <= 1) {
                } else {

                    //
                    for (int nextIndexToRead = slotValue; ; ) {
                        if (phyOffsets.size() >= maxNum) {
                            break;
                        }

                        //因为会存在哈希冲突，所以根据slotValue定位该哈希槽
                        //最新的一个Item条目，将存储的物理偏移量加入phyOffsets，然后继
                        //续验证Item条目中存储的上一个Index下标，如果大于、等于1并且小
                        //于当前文件的最大条目数，则继续查找，否则结束查找
                        int absIndexPos =
                                IndexHeader.INDEX_HEADER_SIZE + this.hashSlotNum * hashSlotSize
                                        + nextIndexToRead * indexSize;

                        int keyHashRead = this.mappedByteBuffer.getInt(absIndexPos);
                        long phyOffsetRead = this.mappedByteBuffer.getLong(absIndexPos + 4);

                        long timeDiff = this.mappedByteBuffer.getInt(absIndexPos + 4 + 8);
                        int prevIndexRead = this.mappedByteBuffer.getInt(absIndexPos + 4 + 8 + 4);
                        //根据Index下标定位到条目的起始物理偏移量，然后依次
                        //读取哈希码、物理偏移量、时间戳、上一个条目的Index下标

                        //如果存储的时间戳小于0，则直接结束查找。如果哈希匹
                        //配并且消息存储时间介于待查找时间start、end之间，则将消息物理
                        //偏移量加入phyOffsets，并验证条目的前一个Index索引，如果索引大
                        //于、等于1并且小于Index条目数，则继续查找，否则结束查找

                        if (timeDiff < 0) {
                            break;
                        }

                        timeDiff *= 1000L;

                        long timeRead = this.indexHeader.getBeginTimestamp() + timeDiff;
                        boolean timeMatched = timeRead >= begin && timeRead <= end;

                        if (keyHash == keyHashRead && timeMatched) {
                            phyOffsets.add(phyOffsetRead);
                        }

                        if (prevIndexRead <= invalidIndex
                                || prevIndexRead > this.indexHeader.getIndexCount()
                                || prevIndexRead == nextIndexToRead || timeRead < begin) {
                            break;
                        }

                        nextIndexToRead = prevIndexRead;
                    }
                }
            } catch (Exception e) {
                log.error("selectPhyOffset exception ", e);
            } finally {
                this.mappedFile.release();
            }
        }
    }
}
