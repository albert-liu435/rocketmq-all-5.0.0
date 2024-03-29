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
package org.apache.rocketmq.store.logfile;

import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageExtBatch;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.store.AppendMessageCallback;
import org.apache.rocketmq.store.AppendMessageResult;
import org.apache.rocketmq.store.AppendMessageStatus;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.store.PutMessageContext;
import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.apache.rocketmq.store.TransientStorePool;
import org.apache.rocketmq.store.config.FlushDiskType;
import org.apache.rocketmq.store.util.LibC;
import sun.nio.ch.DirectBuffer;

public class DefaultMappedFile extends AbstractMappedFile {


//    这里需要额外讲解的是，几个表示位置的参数。wrotePosition，committedPosition，flushedPosition。大概的关系如下wrotePosition<=committedPosition<=flushedPosition<=fileSize
//
//    作者：szhlcy
//    链接：https://www.jianshu.com/p/2aad2044980d
//    来源：简书
//    著作权归作者所有。商业转载请联系作者获得授权，非商业转载请注明出处。


    //操作系统每页大小，默认4k
    public static final int OS_PAGE_SIZE = 1024 * 4;
    protected static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    //文件已使用的映射虚拟内存
    //类变量，所有 MappedFile 实例已使用字节总数。
    //当前JVM实例中MappedFile虚拟内存
    protected static final AtomicLong TOTAL_MAPPED_VIRTUAL_MEMORY = new AtomicLong(0);
    //映射额文件个数
    //MappedFile 个数。当前JVM实例中MappedFile对象个数
    protected static final AtomicInteger TOTAL_MAPPED_FILES = new AtomicInteger(0);
    //当前文件的写指针，从0开始(内存映射文件中的写指针)
    protected static final AtomicIntegerFieldUpdater<DefaultMappedFile> WROTE_POSITION_UPDATER;
    //当前文件的提交指针，如果开启transientStorePoolEnable，则数据会存储在transientStorePoolEnable中，然后提交到内存映射ByteBuffer中再写入磁盘
    protected static final AtomicIntegerFieldUpdater<DefaultMappedFile> COMMITTED_POSITION_UPDATER;
    //刷写到磁盘指针，该指针之前的数据持久化到磁盘中
    protected static final AtomicIntegerFieldUpdater<DefaultMappedFile> FLUSHED_POSITION_UPDATER;
    //已经写入的位置
    //当前MappedFile对象当前写指针。
    protected volatile int wrotePosition;
    // 提交完成位置
    //当前提交的指针。
    protected volatile int committedPosition;
    //刷新完成位置
    //当前刷写到磁盘的指针。
    protected volatile int flushedPosition;
    //文件大小
    protected int fileSize;
    //创建MappedByteBuffer用的
    //文件通道。
    protected FileChannel fileChannel;
    /**
     * Message will put to here first, and then reput to FileChannel if writeBuffer is not null.
     * 消息将首先放在这里，如果writeBuffer不为空，则再放到FileChannel。
     * 如果开启了transientStorePoolEnable，消息会写入堆外内存，然后提交到 PageCache 并最终刷写到磁盘。
     * <p>
     * 堆内存ByteBuffer，如果不为空，数据首先将存储在该Buffer中，然后提交到MappedFile对应的内存映射文件Buffer。
     */
    protected ByteBuffer writeBuffer = null;
    //ByteBuffer的缓冲池，堆外内存，transientStorePoolEnable 为 true 时生效。
    protected TransientStorePool transientStorePool = null;
    //文件名
    protected String fileName;
    //文件开始offset,文件序号,代表该文件代表的文件偏移量。
    protected long fileFromOffset;
    //物理文件
    protected File file;
    //对应操作系统的 PageCache。
    //物理文件对应的内存映射Buffer
    protected MappedByteBuffer mappedByteBuffer;
    //最后一次存储时间戳。
    protected volatile long storeTimestamp = 0;
    //是否是MappedFileQueue队列中第一个文件。
    protected boolean firstCreateInQueue = false;
    private long lastFlushTime = -1L;

    protected MappedByteBuffer mappedByteBufferWaitToClean = null;
    protected long swapMapTime = 0L;
    protected long mappedByteBufferAccessCountSinceLastSwap = 0L;

    static {
        WROTE_POSITION_UPDATER = AtomicIntegerFieldUpdater.newUpdater(DefaultMappedFile.class, "wrotePosition");
        COMMITTED_POSITION_UPDATER = AtomicIntegerFieldUpdater.newUpdater(DefaultMappedFile.class, "committedPosition");
        FLUSHED_POSITION_UPDATER = AtomicIntegerFieldUpdater.newUpdater(DefaultMappedFile.class, "flushedPosition");
    }

    public DefaultMappedFile() {
    }

    public DefaultMappedFile(final String fileName, final int fileSize) throws IOException {
        init(fileName, fileSize);
    }

    public DefaultMappedFile(final String fileName, final int fileSize,
                             final TransientStorePool transientStorePool) throws IOException {
        init(fileName, fileSize, transientStorePool);
    }

    public static int getTotalMappedFiles() {
        return TOTAL_MAPPED_FILES.get();
    }

    public static long getTotalMappedVirtualMemory() {
        return TOTAL_MAPPED_VIRTUAL_MEMORY.get();
    }

    /**
     * 初始化fileFromOffset为文件名，也就是文件名代表该
     * 文件的起始偏移量，通过RandomAccessFile创建读写文件通道，并将
     * 文件内容使用NIO的内存映射Buffer将文件映射到内存中
     *
     * @param fileName           file name
     * @param fileSize           file size
     * @param transientStorePool transient store pool 如果transientStorePoolEnable为true，则初始化MappedFile的
     *                           writeBuffer，该buffer从transientStorePool中获取。
     * @throws IOException
     */
    @Override
    public void init(final String fileName, final int fileSize,
                     final TransientStorePool transientStorePool) throws IOException {
        init(fileName, fileSize);
        //开启了堆外内存
        this.writeBuffer = transientStorePool.borrowBuffer();
        this.transientStorePool = transientStorePool;
    }

    /**
     * 根据是否开启transientStorePoolEnable存在两种初始
     * 化情况。transientStorePool-Enable为true表示内容先存储在堆外内
     * 存，然后通过Commit线程将数据提交到FileChannel中，再通过Flush
     * 线程将数据持久化到磁盘中
     *
     * @param fileName
     * @param fileSize
     * @throws IOException
     */
    private void init(final String fileName, final int fileSize) throws IOException {
        //文件名
        this.fileName = fileName;
        //文件大小
        this.fileSize = fileSize;
        //创建文件
        this.file = new File(fileName);
        //文件名称是文件占用内存偏移量的起始位置，前面说过RocketMQ中消息存储的文件的命名是偏移量来进行命名的
        //根据文件的名称计算文件其实的偏移量
        this.fileFromOffset = Long.parseLong(this.file.getName());
        boolean ok = false;

        UtilAll.ensureDirOK(this.file.getParent());

        try {
            //创建读写类型的fileChannel
            this.fileChannel = new RandomAccessFile(this.file, "rw").getChannel();
            //获取写入类型的内存文件映射对象mappedByteBuffer
            this.mappedByteBuffer = this.fileChannel.map(MapMode.READ_WRITE, 0, fileSize);
            //增加已经映射的虚拟内存
            TOTAL_MAPPED_VIRTUAL_MEMORY.addAndGet(fileSize);
            //已经映射文件数量+1
            TOTAL_MAPPED_FILES.incrementAndGet();
            ok = true;
        } catch (FileNotFoundException e) {
            log.error("Failed to create file " + this.fileName, e);
            throw e;
        } catch (IOException e) {
            log.error("Failed to map file " + this.fileName, e);
            throw e;
        } finally {
            if (!ok && this.fileChannel != null) {
                this.fileChannel.close();
            }
        }
    }

    @Override
    public long getLastModifiedTimestamp() {
        return this.file.lastModified();
    }

    public boolean getData(int pos, int size, ByteBuffer byteBuffer) {
        if (byteBuffer.remaining() < size) {
            return false;
        }

        int readPosition = getReadPosition();
        if ((pos + size) <= readPosition) {

            if (this.hold()) {
                try {
                    int readNum = fileChannel.read(byteBuffer, pos);
                    return size == readNum;
                } catch (Throwable t) {
                    log.warn("Get data failed pos:{} size:{} fileFromOffset:{}", pos, size, this.fileFromOffset);
                    return false;
                } finally {
                    this.release();
                }
            } else {
                log.debug("matched, but hold failed, request pos: " + pos + ", fileFromOffset: "
                        + this.fileFromOffset);
            }
        } else {
            log.warn("selectMappedBuffer request pos invalid, request pos: " + pos + ", size: " + size
                    + ", fileFromOffset: " + this.fileFromOffset);
        }

        return false;
    }

    @Override
    public int getFileSize() {
        return fileSize;
    }

    @Override
    public FileChannel getFileChannel() {
        return fileChannel;
    }

    /**
     * 拼接消息的方法，供commitlog使用.即消息的写入
     *
     * @param msg
     * @param cb
     * @param putMessageContext
     * @return
     */
    @Override
    public AppendMessageResult appendMessage(final MessageExtBrokerInner msg, final AppendMessageCallback cb,
                                             PutMessageContext putMessageContext) {
        return appendMessagesInner(msg, cb, putMessageContext);
    }

    @Override
    public AppendMessageResult appendMessages(final MessageExtBatch messageExtBatch, final AppendMessageCallback cb,
                                              PutMessageContext putMessageContext) {
        return appendMessagesInner(messageExtBatch, cb, putMessageContext);
    }

    /**
     * 将消息追加到MappedFile中。首先获取MappedFile当前
     * 的写指针，如果currentPos大于或等于文件大小，表明文件已写满，
     * 抛出AppendMessageStatus.UNKNOWN_ERROR。如果currentPos小于文件
     * 大小，通过slice()方法创建一个与原ByteBuffer共享的内存区，且拥
     * 有独立的position、limit、capacity等指针，并设置position为当前
     * 指针，
     *
     * @param messageExt
     * @param cb
     * @param putMessageContext
     * @return
     */
    public AppendMessageResult appendMessagesInner(final MessageExt messageExt, final AppendMessageCallback cb,
                                                   PutMessageContext putMessageContext) {
        assert messageExt != null;
        assert cb != null;
        //        获取当前写的位置
        //page cache的写指针位置
        int currentPos = WROTE_POSITION_UPDATER.get(this);

        if (currentPos < this.fileSize) {
            //这里的writeBuffer，如果在启动的时候配置了启用暂存池，这里的writeBuffer是堆外内存方式。获取byteBuffer
            //开启了堆外内存就用堆外内存，否则用page cache
            ByteBuffer byteBuffer = appendMessageBuffer().slice();
            //定位到写指针位置
            byteBuffer.position(currentPos);
            AppendMessageResult result;
            //根据消息类型，是批量消息还是单个消息，进入相应的处理
            if (messageExt instanceof MessageExtBatch && !((MessageExtBatch) messageExt).isInnerBatch()) {
                // traditional batch message
                //                批量消息序列化后组装映射的buffer
                result = cb.doAppend(this.getFileFromOffset(), byteBuffer, this.fileSize - currentPos,
                        (MessageExtBatch) messageExt, putMessageContext);
            } else if (messageExt instanceof MessageExtBrokerInner) {
                // traditional single message or newly introduced inner-batch message
                //                消息序列化后组装映射的buffer=》
                result = cb.doAppend(this.getFileFromOffset(), byteBuffer, this.fileSize - currentPos,
                        (MessageExtBrokerInner) messageExt, putMessageContext);
            } else {
                return new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
            }
            //写指针后移bytes位
            WROTE_POSITION_UPDATER.addAndGet(this, result.getWroteBytes());
            this.storeTimestamp = result.getStoreTimestamp();
            return result;
        }
        //表明文件写满
        log.error("MappedFile.appendMessage return null, wrotePosition: {} fileSize: {}", currentPos, this.fileSize);
        return new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
    }

    protected ByteBuffer appendMessageBuffer() {
        this.mappedByteBufferAccessCountSinceLastSwap++;
        return writeBuffer != null ? writeBuffer : this.mappedByteBuffer;
    }

    @Override
    public long getFileFromOffset() {
        return this.fileFromOffset;
    }

    @Override
    public boolean appendMessage(final byte[] data) {
        return appendMessage(data, 0, data.length);
    }

    @Override
    public boolean appendMessage(ByteBuffer data) {
        int currentPos = WROTE_POSITION_UPDATER.get(this);
        int remaining = data.remaining();

        if ((currentPos + remaining) <= this.fileSize) {
            try {
                //设置写的起始位置
                this.fileChannel.position(currentPos);
                while (data.hasRemaining()) {
                    //写入
                    this.fileChannel.write(data);
                }
            } catch (Throwable e) {
                log.error("Error occurred when append message to mappedFile.", e);
            }
            WROTE_POSITION_UPDATER.addAndGet(this, remaining);
            return true;
        }
        return false;
    }

    /**
     * Content of data from offset to offset + length will be written to file.
     *
     * @param offset The offset of the subarray to be used.
     * @param length The length of the subarray to be used.
     */
    @Override
    public boolean appendMessage(final byte[] data, final int offset, final int length) {
        int currentPos = WROTE_POSITION_UPDATER.get(this);

        if ((currentPos + length) <= this.fileSize) {
            try {
                ByteBuffer buf = this.mappedByteBuffer.slice();
                buf.position(currentPos);
                buf.put(data, offset, length);
            } catch (Throwable e) {
                log.error("Error occurred when append message to mappedFile.", e);
            }
            WROTE_POSITION_UPDATER.addAndGet(this, length);
            return true;
        }

        return false;
    }

    /**
     *  flush方法比较简单，就是将fileChannel中的数据写入文件中。
     * <p>
     * 刷盘指的是将内存中的数据写入磁盘，永久存储在磁盘中，由
     * MappedFile的flush()方法实现
     *
     * <p>
     * 如果使用writeBuffer存储的话则调用fileChannel的force将内存中的数据持久化到磁盘，刷盘结束后，flushedPosition会等于committedPosition，否则调用mappedByteBuffer的force，最后flushedPosition会等于writePosition。
     *
     * @return The current flushed position
     */

    //直接调用mappedByteBuffer或fileChannel的force()方法将数据
    //写入磁盘，将内存中的数据持久化到磁盘中，那么flushedPosition应
    //该等于MappedByteBuffer中的写指针。如果writeBuffer不为空，则
    //flushedPosition应等于上一次commit指针。因为上一次提交的数据就
    //是进入MappedByteBuffer中的数据。如果writeBuffer为空，表示数据
    //是直接进入MappedByteBuffer的，wrotePosition代表的是
    //MappedByteBuffer中的指针，故设置flushedPosition为
    //wrotePosition。
    @Override
    public int flush(final int flushLeastPages) {
        if (this.isAbleToFlush(flushLeastPages)) {
            //检查文件是否有效，也就是有引用，并添加引用
            if (this.hold()) {
                //获取写入的位置
                //没开启对外内存使用写指针，否则使用提交指针
                int value = getReadPosition();

                try {
                    this.mappedByteBufferAccessCountSinceLastSwap++;

                    //We only append data to fileChannel or mappedByteBuffer, never both.
                    //如果writeBuffer不为null，说明用了临时存储池，说明前面已经把信息写入了writeBuffer了，直接刷新到磁盘就可以。
                    //fileChannel的位置不为0，说明已经设置了buffer进去了，直接刷新到磁盘
                    if (writeBuffer != null || this.fileChannel.position() != 0) {
                        //使用了堆外内存，则使用send file 刷盘
                        this.fileChannel.force(false);
                    } else {
                        //如果数据在mappedByteBuffer中，则刷新mappedByteBuffer数据到磁盘
                        //page cache 刷盘， mmap一般用于小文件映射，send file用于大文件，这里的mapper file都是使用mmap映射，对于commit log这种1G的大文件是否不合适呢？
                        this.mappedByteBuffer.force();
                    }
                    this.lastFlushTime = System.currentTimeMillis();
                } catch (Throwable e) {
                    log.error("Error occurred when force data to disk.", e);
                }
                //设置已经刷新的值
                FLUSHED_POSITION_UPDATER.set(this, value);
                //释放引用
                this.release();
            } else {
                log.warn("in flush, hold failed, flush offset = " + FLUSHED_POSITION_UPDATER.get(this));
                FLUSHED_POSITION_UPDATER.set(this, getReadPosition());
            }
        }
        return this.getFlushedPosition();
    }

    /**
     * 这里的commit方法的作用就是把前面写到缓冲中的数据提交到fileChannel中。这里存在两种情况，
     * 一种是使用堆外内存的缓冲，一种是使用内存映射的缓冲。两者的处理方式是不一样的。
     * <p>
     * <p>
     * 执行提交操作，commitLeastPages为本次提交的最小页数，如果
     * 待提交数据不满足commitLeastPages，则不执行本次提交操作，等待
     * 下次提交。writeBuffer如果为空，直接返回wrotePosition指针，无
     * 须执行commit操作，这表明commit操作的主体是writeBuffer
     *
     * @param commitLeastPages the least pages to commit
     * @return
     */
    @Override
    public int commit(final int commitLeastPages) {
        /**
         * writeBuffer 为  null的情况下，说明没有使用临时存储池，使用的是mappedByteBuffer也就是内存映射的方式，
         * 直接写到映射区域中的，那么这个时候就不需要写入的fileChannel了。直接返回写入的位置作为已经提交的位置。
         *
         * writeBuffer 不为  null，说明用的是临时存储池，使用的堆外内存，那么这个时候需要先把信息提交到fileChannel中
         *
         * 这是commit代码，如果writeBuffer为空，直接返回wrotePosition指针，无需commit
         */
        if (writeBuffer == null) {
            //no need to commit data to file channel, so just regard wrotePosition as committedPosition.
            return WROTE_POSITION_UPDATER.get(this);
        }
        //检查是否需要刷盘
        //isAbleToCommit（）用于判断是否可以commit。如果文件已满返回true，commitLeastPages为本次提交最小的页数，如果commitLeastPages > 0 ，则比较wrotePosition（当前writeBuffe的写指针）与上一次提交的指针（committedPosition）的差值，
        // 除以OS_PAGE_SIZE 获取当前脏页的数量，如果大于commitLeastPages则返回true，反之false。
        if (this.isAbleToCommit(commitLeastPages)) {
            //检查当前文件是不是有效，就是当前文件还存在引用
            if (this.hold()) {
                commit0();
                //引用次数减1
                this.release();
            } else {
                log.warn("in commit, hold failed, commit offset = " + COMMITTED_POSITION_UPDATER.get(this));
            }
        }

        // All dirty data has been committed to FileChannel.
        if (writeBuffer != null && this.transientStorePool != null && this.fileSize == COMMITTED_POSITION_UPDATER.get(this)) {
            this.transientStorePool.returnBuffer(writeBuffer);
            this.writeBuffer = null;
        }
        //获取已经刷新的位置
        return COMMITTED_POSITION_UPDATER.get(this);
    }

    /**
     * Commit0（）为具体的实现代码，首先创建writeBUffer的共享缓存区，然后将新创建的position回退到上一次提交的位置，设置limit为writePos（当前最大有效数据指针），然后把commitedPosition到wrotePosition的数据（未提交数据）
     * 写入FileChannel中，然后更新committedPosition指针为wrotePosition。
     * <p>
     * 下面介绍具体的MappedFile提交实现过程。首先创建writeBuffer
     * 的共享缓存区，然后将新创建的position回退到上一次提交的位置
     * （committedPosition），设置limit为wrotePosition（当前最大有效
     * 数据指针），接着把committedPosition到wrotePosition的数据复制
     * （写入）到FileChannel中，最后更新committedPosition指针为
     * wrotePosition。commit的作用是将MappedFile# writeBuffer中的数
     * 据提交到文件通道FileChannel中。
     * ByteBuffer使用技巧：调用slice()方法创建一个共享缓存区，与
     * 原先的ByteBuffer共享内存并维护一套独立的指针（position、
     * mark、limit）。
     */
    protected void commit0() {
        //获取已经写入的数据的位置
        int writePos = WROTE_POSITION_UPDATER.get(this);
        //获取上次提交的位置
        int lastCommittedPosition = COMMITTED_POSITION_UPDATER.get(this);
        //如果还有没有提交的数据，则进行写入
        if (writePos - lastCommittedPosition > 0) {
            try {
                //使用了堆外内存
                ByteBuffer byteBuffer = writeBuffer.slice();
                byteBuffer.position(lastCommittedPosition);
                byteBuffer.limit(writePos);
                //使用send file 写到堆外内存，并设置提交指针
                this.fileChannel.position(lastCommittedPosition);
                this.fileChannel.write(byteBuffer);
                COMMITTED_POSITION_UPDATER.set(this, writePos);
            } catch (Throwable e) {
                log.error("Error occurred when commit data to FileChannel.", e);
            }
        }
    }

    private boolean isAbleToFlush(final int flushLeastPages) {
        int flush = FLUSHED_POSITION_UPDATER.get(this);
        int write = getReadPosition();

        if (this.isFull()) {
            return true;
        }

        if (flushLeastPages > 0) {
            return ((write / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE)) >= flushLeastPages;
        }

        return write > flush;
    }

    /**
     * 判断是否执行commit操作。如果文件已满，返回true。如果
     * commitLeastPages大于0，则计算wrotePosition（当前writeBuffe的
     * 写指针）与上一次提交的指针（committedPosition）的差值，将其除
     * 以OS_PAGE_SIZE得到当前脏页的数量，如果大于commitLeastPages，
     * 则返回true。如果commitLeastPages小于0，表示只要存在脏页就提
     * 交
     *
     * @param commitLeastPages
     * @return
     */
    protected boolean isAbleToCommit(final int commitLeastPages) {
        int commit = COMMITTED_POSITION_UPDATER.get(this);
        int write = WROTE_POSITION_UPDATER.get(this);

        if (this.isFull()) {
            return true;
        }

        if (commitLeastPages > 0) {
            return ((write / OS_PAGE_SIZE) - (commit / OS_PAGE_SIZE)) >= commitLeastPages;
        }

        return write > commit;
    }

    @Override
    public int getFlushedPosition() {
        return FLUSHED_POSITION_UPDATER.get(this);
    }

    @Override
    public void setFlushedPosition(int pos) {
        FLUSHED_POSITION_UPDATER.set(this, pos);
    }

    @Override
    public boolean isFull() {
        return this.fileSize == WROTE_POSITION_UPDATER.get(this);
    }

    /**
     * 读取数据
     * 首先查找pos到当前最大可读指针之间的数据，因为在整个写入期
     * 间都未曾改变MappedByteBuffer的指针，所以
     * mappedByteBuffer.slice()方法返回的共享缓存区空间为整个
     * MappedFile。然后通过设置byteBuffer的position为待查找的值，读
     * 取字节为当前可读字节长度，最终返回的ByteBuffer的limit（可读最
     * 大长度）为size。整个共享缓存区的容量为
     * MappedFile#fileSizepos，故在操作SelectMappedBufferResult时不
     * 能对包含在里面的ByteBuffer调用flip()方法。
     * <p>
     * 操作ByteBuffer时如果使用了slice()方法，对其ByteBuffer进行读取
     * 时一般手动指定position和limit指针，而不是调用flip()方法切换读
     * 写状态。
     *
     * @param pos  the given position
     * @param size the size of the returned sub-region
     * @return
     */
    @Override
    public SelectMappedBufferResult selectMappedBuffer(int pos, int size) {
        //获取提交的位置
        int readPosition = getReadPosition();
        //如果要读取的信息在已经提交的信息中，就进行读取
        if ((pos + size) <= readPosition) {
            //检查文件是否有效
            if (this.hold()) {
                this.mappedByteBufferAccessCountSinceLastSwap++;
                //读取数据然后返回
                ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
                byteBuffer.position(pos);
                ByteBuffer byteBufferNew = byteBuffer.slice();
                byteBufferNew.limit(size);
                return new SelectMappedBufferResult(this.fileFromOffset + pos, byteBufferNew, size, this);
            } else {
                log.warn("matched, but hold failed, request pos: " + pos + ", fileFromOffset: "
                        + this.fileFromOffset);
            }
        } else {
            log.warn("selectMappedBuffer request pos invalid, request pos: " + pos + ", size: " + size
                    + ", fileFromOffset: " + this.fileFromOffset);
        }

        return null;
    }

    /**
     * 获取文件读取的位置
     *
     * @param pos the given position
     * @return
     */
    @Override
    public SelectMappedBufferResult selectMappedBuffer(int pos) {
        int readPosition = getReadPosition();
        if (pos < readPosition && pos >= 0) {
            if (this.hold()) {
                //创建新的缓冲区
                this.mappedByteBufferAccessCountSinceLastSwap++;
                ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
                byteBuffer.position(pos);
                //获取指定位置到最新提交的位置之间的数据
                int size = readPosition - pos;
                ByteBuffer byteBufferNew = byteBuffer.slice();
                byteBufferNew.limit(size);
                return new SelectMappedBufferResult(this.fileFromOffset + pos, byteBufferNew, size, this);
            }
        }

        return null;
    }

    /**
     * 清除内存映射
     * 如果available为true，表示MappedFile当前可用，无须清理，返
     * 回false，如果资源已经被清除，返回true。如果是堆外内存，调用堆
     * 外内存的cleanup()方法进行清除，维护MappedFile类变量
     * TOTAL_MAPPED_VIRTUAL_MEMORY、TOTAL_MAPPED_FILES并返回true，表
     * 示cleanupOver为true。
     *
     * @param currentRef
     * @return
     */
    @Override
    public boolean cleanup(final long currentRef) {
        if (this.isAvailable()) {
            log.error("this file[REF:" + currentRef + "] " + this.fileName
                    + " have not shutdown, stop unmapping.");
            return false;
        }

        if (this.isCleanupOver()) {
            log.error("this file[REF:" + currentRef + "] " + this.fileName
                    + " have cleanup, do not do it again.");
            return true;
        }
        //        清除映射缓冲区=》
        UtilAll.cleanBuffer(this.mappedByteBuffer);
        UtilAll.cleanBuffer(this.mappedByteBufferWaitToClean);
        this.mappedByteBufferWaitToClean = null;
        //        减少映射文件所占虚拟内存
        TOTAL_MAPPED_VIRTUAL_MEMORY.addAndGet(this.fileSize * (-1));
        //        改变映射文件数量
        TOTAL_MAPPED_FILES.decrementAndGet();
        log.info("unmap file[REF:" + currentRef + "] " + this.fileName + " OK");
        return true;
    }

    /**
     * 删除文件
     * 关闭MqppedFile。设置available为false，若是第一次则设置关闭时间戳为当前系统时间，然后调用release（）方法尝试释放资源。
     * release如果引用计数小于0，则调用cleanup（）方法，cleanup（）方法会判断当前mappedFile是否可用，或者资源是否被清理完成，如果没有，对于堆外内存，调用堆外内存的cleanup方法清除，维护MappedFile类变量TOTAL_MAPPED_VIRTUAL_MEMORY、TOTAL_MAPPED_FILES。
     * （判断是否清理完成，标准是引用次数小于等于0并且cleanupOver为true，cleanupOver为true的触发条件是release成功将MappedByteBuffer资源释放。）
     * 关闭文件通道，删除物理文件
     *
     * @param intervalForcibly If {@code true} then this method will destroy the file forcibly and ignore the reference intervalForcibly表示拒
     *                         绝被销毁的最大存活时间
     * @return
     */
    @Override
    public boolean destroy(final long intervalForcibly) {
        //        =》删除引用
        this.shutdown(intervalForcibly);
        //已经清楚了文件的引用
        if (this.isCleanupOver()) {
            try {
                long lastModified = getLastModifiedTimestamp();
                //关闭文件通道，删除物理文件
                //                关闭文件channel
                this.fileChannel.close();
                log.info("close file channel " + this.fileName + " OK");

                long beginTime = System.currentTimeMillis();
                //                删除文件
                boolean result = this.file.delete();
                log.info("delete file[REF:" + this.getRefCount() + "] " + this.fileName
                        + (result ? " OK, " : " Failed, ") + "W:" + this.getWrotePosition() + " M:"
                        + this.getFlushedPosition() + ", "
                        + UtilAll.computeElapsedTimeMilliseconds(beginTime)
                        + "," + (System.currentTimeMillis() - lastModified));
            } catch (Exception e) {
                log.warn("close file channel " + this.fileName + " Failed. ", e);
            }

            return true;
        } else {
            log.warn("destroy mapped file[REF:" + this.getRefCount() + "] " + this.fileName
                    + " Failed. cleanupOver: " + this.cleanupOver);
        }

        return false;
    }

    @Override
    public int getWrotePosition() {
        return WROTE_POSITION_UPDATER.get(this);
    }

    @Override
    public void setWrotePosition(int pos) {
        WROTE_POSITION_UPDATER.set(this, pos);
    }

    /**
     * RocketMQ文件的一个组织方式是内存映射，预先申请一块连续且
     * 固定大小的内存，需要一套指针标识当前最大有效数据的位置，获取
     * 最大有效数据偏移量的方法由MappedFile的getReadPosition()方法实
     * 现
     *
     * @return The max position which have valid data
     */
    @Override
    public int getReadPosition() {
        //获取当前文件最大的可读指针，如代码清单4-23所示。如果
        //writeBuffer为空，则直接返回当前的写指针。如果writeBuffer不为
        //空，则返回上一次提交的指针。在MappedFile设计中，只有提交了的
        //数据（写入MappedByteBuffer或FileChannel中的数据）才是安全的数
        //据。
        return this.writeBuffer == null ? WROTE_POSITION_UPDATER.get(this) : COMMITTED_POSITION_UPDATER.get(this);
    }

    @Override
    public void setCommittedPosition(int pos) {
        COMMITTED_POSITION_UPDATER.set(this, pos);
    }

    @Override
    public void warmMappedFile(FlushDiskType type, int pages) {
        this.mappedByteBufferAccessCountSinceLastSwap++;

        long beginTime = System.currentTimeMillis();
        ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
        int flush = 0;
        long time = System.currentTimeMillis();
        for (int i = 0, j = 0; i < this.fileSize; i += DefaultMappedFile.OS_PAGE_SIZE, j++) {
            byteBuffer.put(i, (byte) 0);
            // force flush when flush disk type is sync
            if (type == FlushDiskType.SYNC_FLUSH) {
                if ((i / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE) >= pages) {
                    flush = i;
                    mappedByteBuffer.force();
                }
            }

            // prevent gc
            if (j % 1000 == 0) {
                log.info("j={}, costTime={}", j, System.currentTimeMillis() - time);
                time = System.currentTimeMillis();
                try {
                    Thread.sleep(0);
                } catch (InterruptedException e) {
                    log.error("Interrupted", e);
                }
            }
        }

        // force flush when prepare load finished
        if (type == FlushDiskType.SYNC_FLUSH) {
            log.info("mapped file warm-up done, force to disk, mappedFile={}, costTime={}",
                    this.getFileName(), System.currentTimeMillis() - beginTime);
            mappedByteBuffer.force();
        }
        log.info("mapped file warm-up done. mappedFile={}, costTime={}", this.getFileName(),
                System.currentTimeMillis() - beginTime);

        this.mlock();
    }

    @Override
    public boolean swapMap() {
        if (getRefCount() == 1 && this.mappedByteBufferWaitToClean == null) {

            if (!hold()) {
                log.warn("in swapMap, hold failed, fileName: " + this.fileName);
                return false;
            }
            try {
                this.mappedByteBufferWaitToClean = this.mappedByteBuffer;
                this.mappedByteBuffer = this.fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, fileSize);
                this.mappedByteBufferAccessCountSinceLastSwap = 0L;
                this.swapMapTime = System.currentTimeMillis();
                log.info("swap file " + this.fileName + " success.");
                return true;
            } catch (Exception e) {
                log.error("swapMap file " + this.fileName + " Failed. ", e);
            } finally {
                this.release();
            }
        } else {
            log.info("Will not swap file: " + this.fileName + ", ref=" + getRefCount());
        }
        return false;
    }

    @Override
    public void cleanSwapedMap(boolean force) {
        try {
            if (this.mappedByteBufferWaitToClean == null) {
                return;
            }
            long minGapTime = 120 * 1000L;
            long gapTime = System.currentTimeMillis() - this.swapMapTime;
            if (!force && gapTime < minGapTime) {
                Thread.sleep(minGapTime - gapTime);
            }
            UtilAll.cleanBuffer(this.mappedByteBufferWaitToClean);
            mappedByteBufferWaitToClean = null;
            log.info("cleanSwapedMap file " + this.fileName + " success.");
        } catch (Exception e) {
            log.error("cleanSwapedMap file " + this.fileName + " Failed. ", e);
        }
    }

    @Override
    public long getRecentSwapMapTime() {
        return 0;
    }

    @Override
    public long getMappedByteBufferAccessCountSinceLastSwap() {
        return this.mappedByteBufferAccessCountSinceLastSwap;
    }

    @Override
    public long getLastFlushTime() {
        return this.lastFlushTime;
    }

    @Override
    public String getFileName() {
        return fileName;
    }

    @Override
    public MappedByteBuffer getMappedByteBuffer() {
        this.mappedByteBufferAccessCountSinceLastSwap++;
        return mappedByteBuffer;
    }

    @Override
    public ByteBuffer sliceByteBuffer() {
        this.mappedByteBufferAccessCountSinceLastSwap++;
        return this.mappedByteBuffer.slice();
    }

    @Override
    public long getStoreTimestamp() {
        return storeTimestamp;
    }

    @Override
    public boolean isFirstCreateInQueue() {
        return firstCreateInQueue;
    }

    @Override
    public void setFirstCreateInQueue(boolean firstCreateInQueue) {
        this.firstCreateInQueue = firstCreateInQueue;
    }

    @Override
    public void mlock() {
        final long beginTime = System.currentTimeMillis();
        final long address = ((DirectBuffer) (this.mappedByteBuffer)).address();
        Pointer pointer = new Pointer(address);
        {
            int ret = LibC.INSTANCE.mlock(pointer, new NativeLong(this.fileSize));
            log.info("mlock {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
        }

        {
            int ret = LibC.INSTANCE.madvise(pointer, new NativeLong(this.fileSize), LibC.MADV_WILLNEED);
            log.info("madvise {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
        }
    }

    @Override
    public void munlock() {
        final long beginTime = System.currentTimeMillis();
        final long address = ((DirectBuffer) (this.mappedByteBuffer)).address();
        Pointer pointer = new Pointer(address);
        int ret = LibC.INSTANCE.munlock(pointer, new NativeLong(this.fileSize));
        log.info("munlock {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
    }

    @Override
    public File getFile() {
        return this.file;
    }

    @Override
    public String toString() {
        return this.fileName;
    }

}
