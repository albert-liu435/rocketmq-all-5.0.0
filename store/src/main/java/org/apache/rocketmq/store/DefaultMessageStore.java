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

import com.google.common.hash.Hashing;
import io.openmessaging.storage.dledger.entry.DLedgerEntry;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.Inet6Address;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.FileLock;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.AbstractBrokerRunnable;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.BrokerIdentity;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.SystemClock;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.attribute.CQType;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBatch;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.common.protocol.body.HARuntimeInfo;
import org.apache.rocketmq.common.running.RunningStats;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.common.utils.QueueTypeUtils;
import org.apache.rocketmq.common.utils.ServiceProvider;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.config.FlushDiskType;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.config.StorePathConfigHelper;
import org.apache.rocketmq.store.dledger.DLedgerCommitLog;
import org.apache.rocketmq.store.ha.DefaultHAService;
import org.apache.rocketmq.store.ha.HAService;
import org.apache.rocketmq.store.ha.autoswitch.AutoSwitchHAService;
import org.apache.rocketmq.store.hook.PutMessageHook;
import org.apache.rocketmq.store.hook.SendMessageBackHook;
import org.apache.rocketmq.store.index.IndexService;
import org.apache.rocketmq.store.index.QueryOffsetResult;
import org.apache.rocketmq.store.logfile.MappedFile;
import org.apache.rocketmq.store.queue.ConsumeQueueInterface;
import org.apache.rocketmq.store.queue.ConsumeQueueStore;
import org.apache.rocketmq.store.queue.CqUnit;
import org.apache.rocketmq.store.queue.ReferredIterator;
import org.apache.rocketmq.store.stats.BrokerStatsManager;
import org.apache.rocketmq.store.timer.TimerMessageStore;
import org.apache.rocketmq.store.util.PerfCounter;

/***
 * 默认消息存储类
 */
public class DefaultMessageStore implements MessageStore {
    private static final InternalLogger LOGGER = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    public final PerfCounter.Ticks perfs = new PerfCounter.Ticks(LOGGER);
    //存储相关的配置，例如存储路径、commitLog文件大小，刷盘频次等等。
    private final MessageStoreConfig messageStoreConfig;
    // CommitLog
    //comitLog 的核心处理类，消息存储在 commitlog 文件中。
    //CommitLog文件的存储实现类。
    private final CommitLog commitLog;

    private final ConsumeQueueStore consumeQueueStore;
    //ConsumeQueue 文件刷盘服务线程。
    private final FlushConsumeQueueService flushConsumeQueueService;
    //commitLog 过期文件删除线程。
    private final CleanCommitLogService cleanCommitLogService;
    //consumeQueue 过期文件删除线程。、
    private final CleanConsumeQueueService cleanConsumeQueueService;

    private final CorrectLogicOffsetService correctLogicOffsetService;
    //索引服务。
    private final IndexService indexService;
    //MappedFile 分配线程，RocketMQ 使用内存映射处理 commitlog、consumeQueue文件。
    private final AllocateMappedFileService allocateMappedFileService;
    //reput 转发线程（负责 Commitlog 转发到 Consumequeue、Index文件）。
    private ReputMessageService reputMessageService;
    //主从同步高可用实现服务。
    private HAService haService;
    //存储统计服务。
    private final StoreStatsService storeStatsService;
    //ByteBuffer 池，消息堆内存缓存
    private final TransientStorePool transientStorePool;
    //存储服务状态。
    private final RunningFlags runningFlags = new RunningFlags();
    private final SystemClock systemClock = new SystemClock();

    private final ScheduledExecutorService scheduledExecutorService;
    //Broker 统计服务。
    private final BrokerStatsManager brokerStatsManager;
    //消息达到监听器。
    //在消息拉取长轮询模式下的消息达到监听器
    private final MessageArrivingListener messageArrivingListener;
    //Broker配置属性
    private final BrokerConfig brokerConfig;

    private volatile boolean shutdown = true;
    //文件刷盘检测点。
    private StoreCheckpoint storeCheckpoint;
    private TimerMessageStore timerMessageStore;

    private AtomicLong printTimes = new AtomicLong(0);
    //转发 comitlog 日志，主要是从 commitlog 转发到 consumeQueue、index 文件。
    private final LinkedList<CommitLogDispatcher> dispatcherList;

    private RandomAccessFile lockFile;

    private FileLock lock;

    boolean shutDownNormal = false;
    // Max pull msg size
    private final static int MAX_PULL_MSG_SIZE = 128 * 1024 * 1024;

    private volatile int aliveReplicasNum = 1;

    // Refer the MessageStore of MasterBroker in the same process.
    // If current broker is master, this reference point to null or itself.
    // If current broker is slave, this reference point to the store of master broker, and the two stores belong to
    // different broker groups.
    private MessageStore masterStoreInProcess = null;

    private volatile long masterFlushedOffset = -1L;

    private volatile long brokerInitMaxOffset = -1L;

    protected List<PutMessageHook> putMessageHookList = new ArrayList<>();

    private SendMessageBackHook sendMessageBackHook;

    private final ConcurrentMap<Integer /* level */, Long/* delay timeMillis */> delayLevelTable =
            new ConcurrentHashMap<Integer, Long>(32);

    private int maxDelayLevel;

    public DefaultMessageStore(final MessageStoreConfig messageStoreConfig, final BrokerStatsManager brokerStatsManager,
                               final MessageArrivingListener messageArrivingListener, final BrokerConfig brokerConfig) throws IOException {
        this.messageArrivingListener = messageArrivingListener;
        this.brokerConfig = brokerConfig;
        this.messageStoreConfig = messageStoreConfig;
        this.aliveReplicasNum = messageStoreConfig.getTotalReplicas();
        this.brokerStatsManager = brokerStatsManager;
        this.allocateMappedFileService = new AllocateMappedFileService(this);
        if (messageStoreConfig.isEnableDLegerCommitLog()) {
            this.commitLog = new DLedgerCommitLog(this);
        } else {
            this.commitLog = new CommitLog(this);
        }
        this.consumeQueueStore = new ConsumeQueueStore(this, this.messageStoreConfig);

        this.flushConsumeQueueService = new FlushConsumeQueueService();
        this.cleanCommitLogService = new CleanCommitLogService();
        this.cleanConsumeQueueService = new CleanConsumeQueueService();
        this.correctLogicOffsetService = new CorrectLogicOffsetService();
        this.storeStatsService = new StoreStatsService(getBrokerIdentity());
        this.indexService = new IndexService(this);

        if (!messageStoreConfig.isEnableDLegerCommitLog() && !this.messageStoreConfig.isDuplicationEnable()) {
            if (brokerConfig.isEnableControllerMode()) {
                this.haService = new AutoSwitchHAService();
                LOGGER.warn("Load AutoSwitch HA Service: {}", AutoSwitchHAService.class.getSimpleName());
            } else {
                this.haService = ServiceProvider.loadClass(ServiceProvider.HA_SERVICE_ID, HAService.class);
                if (null == this.haService) {
                    this.haService = new DefaultHAService();
                    LOGGER.warn("Load default HA Service: {}", DefaultHAService.class.getSimpleName());
                }
            }
        }

        this.reputMessageService = new ReputMessageService();

        this.transientStorePool = new TransientStorePool(messageStoreConfig);

        this.scheduledExecutorService =
                Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("StoreScheduledThread", getBrokerIdentity()));

        this.dispatcherList = new LinkedList<>();
        this.dispatcherList.addLast(new CommitLogDispatcherBuildConsumeQueue());
        this.dispatcherList.addLast(new CommitLogDispatcherBuildIndex());

        File file = new File(StorePathConfigHelper.getLockFile(messageStoreConfig.getStorePathRootDir()));
        UtilAll.ensureDirOK(file.getParent());
        UtilAll.ensureDirOK(getStorePathPhysic());
        UtilAll.ensureDirOK(getStorePathLogic());
        lockFile = new RandomAccessFile(file, "rw");

        parseDelayLevel();
    }

    public boolean parseDelayLevel() {
        HashMap<String, Long> timeUnitTable = new HashMap<String, Long>();
        timeUnitTable.put("s", 1000L);
        timeUnitTable.put("m", 1000L * 60);
        timeUnitTable.put("h", 1000L * 60 * 60);
        timeUnitTable.put("d", 1000L * 60 * 60 * 24);

        String levelString = messageStoreConfig.getMessageDelayLevel();
        try {
            String[] levelArray = levelString.split(" ");
            for (int i = 0; i < levelArray.length; i++) {
                String value = levelArray[i];
                String ch = value.substring(value.length() - 1);
                Long tu = timeUnitTable.get(ch);

                int level = i + 1;
                if (level > this.maxDelayLevel) {
                    this.maxDelayLevel = level;
                }
                long num = Long.parseLong(value.substring(0, value.length() - 1));
                long delayTimeMillis = tu * num;
                this.delayLevelTable.put(level, delayTimeMillis);
            }
        } catch (Exception e) {
            LOGGER.error("parseDelayLevel exception", e);
            LOGGER.info("levelString String = {}", levelString);
            return false;
        }

        return true;
    }

    @Override
    public void truncateDirtyLogicFiles(long phyOffset) {
        this.consumeQueueStore.truncateDirty(phyOffset);
    }

    /**
     * @throws IOException
     */
    @Override
    public boolean load() {
        boolean result = true;

        try {
            //第一步：判断上一次退出是否正常。其实现机制是Broker在启动
            //时创建${ROCKET_HOME}/store/abort文件，在退出时通过注册JVM钩子
            //函数删除abort文件。如果下一次启动时存在abort文件。说明Broker
            //是异常退出的，CommitLog与ConsumeQueue数据有可能不一致，需要进
            //行修复
            boolean lastExitOK = !this.isTempFileExist();
            LOGGER.info("last shutdown {}, root dir: {}", lastExitOK ? "normally" : "abnormally", messageStoreConfig.getStorePathRootDir());

            // load Commit Log
            result = result && this.commitLog.load();

            // load Consume Queue
            result = result && this.consumeQueueStore.load();
            //第三步：加载CommitLog文件，加载
            //${ROCKET_HOME}/store/commitlog目录下所有文件并按照文件名进行
            //排序。如果文件与配置文件的单个文件大小不一致，将忽略该目录下
            //的所有文件，然后创建MappedFile对象。注意load()方法将
            //wrotePosition、flushedPosition、committedPosition三个指针都设
            //置为文件大小。
            //第四步：加载消息消费队列，调用
            //DefaultMessageStore#loadConsumeQueue，其思路与CommitLog大体一
            //致，遍历消息消费队列根目录，获取该Broker存储的所有主题，然后
            //遍历每个主题目录，获取该主题下的所有消息消费队列，最后分别加
            //载每个消息消费队列下的文件，构建ConsumeQueue对象，主要初始化
            //ConsumeQueue的topic、queueId、storePath、mappedFileSize属性，

            //第五步：加载并存储checkpoint文件，主要用于记录CommitLog文
            //件、ConsumeQueue文件、Inde文件的刷盘点

            if (result) {
                this.storeCheckpoint =
                        new StoreCheckpoint(StorePathConfigHelper.getStoreCheckpoint(this.messageStoreConfig.getStorePathRootDir()));
                this.masterFlushedOffset = this.storeCheckpoint.getMasterFlushedOffset();
                this.indexService.load(lastExitOK);
                //第六步：加载Index文件，如果上次异常退出，而且Index文件刷
                //盘时间小于该文件最大的消息时间戳，则该文件将立即销毁
                this.recover(lastExitOK);

                LOGGER.info("load over, and the max phy offset = {}", this.getMaxPhyOffset());
            }

            long maxOffset = this.getMaxPhyOffset();
            this.setBrokerInitMaxOffset(maxOffset);
            LOGGER.info("load over, and the max phy offset = {}", maxOffset);
        } catch (Exception e) {
            LOGGER.error("load exception", e);
            result = false;
        }

        if (!result) {
            this.allocateMappedFileService.shutdown();
        }

        return result;
    }

    /**
     * 因为ConsumeQueue文件、Index文件都是基于CommitLog文件构建
     * 的，所以当消息生产者提交的消息存储到CommitLog文件中时，
     * ConsumeQueue文件、Index文件需要及时更新，否则消息无法及时被消
     * 费，根据消息属性查找消息也会出现较大延迟。RocketMQ通过开启一
     * 个线程ReputMessageServcie来准实时转发CommitLog文件的更新事
     * 件，相应的任务处理器根据转发的消息及时更新ConsumeQueue文件、
     * Index文件
     *
     * @throws Exception
     */
    @Override
    public void start() throws Exception {
        if (!messageStoreConfig.isEnableDLegerCommitLog() && !this.messageStoreConfig.isDuplicationEnable()) {
            this.haService.init(this);
        }

        if (messageStoreConfig.isTransientStorePoolEnable()) {
            this.transientStorePool.init();
        }

        this.allocateMappedFileService.start();

        this.indexService.start();

        lock = lockFile.getChannel().tryLock(0, 1, false);
        if (lock == null || lock.isShared() || !lock.isValid()) {
            throw new RuntimeException("Lock failed,MQ already started");
        }

        lockFile.getChannel().write(ByteBuffer.wrap("lock".getBytes(StandardCharsets.UTF_8)));
        lockFile.getChannel().force(true);

        if (this.getMessageStoreConfig().isDuplicationEnable()) {
            this.reputMessageService.setReputFromOffset(this.commitLog.getConfirmOffset());
        } else {
            // It is [recover]'s responsibility to fully dispatch the commit log data before the max offset of commit log.
            this.reputMessageService.setReputFromOffset(this.commitLog.getMaxOffset());
        }
        //Broker服务器在启动时会启动ReputMessageService线程，并初始
        //化一个非常关键的参数reputFromOffset，该参数的含义是
        //ReputMessageService从哪个物理偏移量开始转发消息给ConsumeQueue
        //和Index文件。如果允许重复转发，将reputFromOffset设置为
        //CommitLog文件的提交指针。如果不允许重复转发，将
        //reputFromOffset设置为CommitLog文件的内存中最大偏移量
        this.reputMessageService.start();

        // Checking is not necessary, as long as the dLedger's implementation exactly follows the definition of Recover,
        // which is eliminating the dispatch inconsistency between the commitLog and consumeQueue at the end of recovery.
        this.doRecheckReputOffsetFromCq();

        this.flushConsumeQueueService.start();
        this.commitLog.start();
        this.storeStatsService.start();

        if (this.haService != null) {
            this.haService.start();
        }

        this.createTempFile();
        this.addScheduleTask();
        this.perfs.start();
        this.shutdown = false;
    }

    private void doRecheckReputOffsetFromCq() throws InterruptedException {
        if (!messageStoreConfig.isRecheckReputOffsetFromCq()) {
            return;
        }

        /**
         * 1. Make sure the fast-forward messages to be truncated during the recovering according to the max physical offset of the commitlog;
         * 2. DLedger committedPos may be missing, so the maxPhysicalPosInLogicQueue maybe bigger that maxOffset returned by DLedgerCommitLog, just let it go;
         * 3. Calculate the reput offset according to the consume queue;
         * 4. Make sure the fall-behind messages to be dispatched before starting the commitlog, especially when the broker role are automatically changed.
         */
        long maxPhysicalPosInLogicQueue = commitLog.getMinOffset();
        for (ConcurrentMap<Integer, ConsumeQueueInterface> maps : this.getConsumeQueueTable().values()) {
            for (ConsumeQueueInterface logic : maps.values()) {
                if (logic.getMaxPhysicOffset() > maxPhysicalPosInLogicQueue) {
                    maxPhysicalPosInLogicQueue = logic.getMaxPhysicOffset();
                }
            }
        }
        // If maxPhyPos(CQs) < minPhyPos(CommitLog), some newly deleted topics may be re-dispatched into cqs mistakenly.
        if (maxPhysicalPosInLogicQueue < 0) {
            maxPhysicalPosInLogicQueue = 0;
        }
        if (maxPhysicalPosInLogicQueue < this.commitLog.getMinOffset()) {
            maxPhysicalPosInLogicQueue = this.commitLog.getMinOffset();
            /**
             * This happens in following conditions:
             * 1. If someone removes all the consumequeue files or the disk get damaged.
             * 2. Launch a new broker, and copy the commitlog from other brokers.
             *
             * All the conditions has the same in common that the maxPhysicalPosInLogicQueue should be 0.
             * If the maxPhysicalPosInLogicQueue is gt 0, there maybe something wrong.
             */
            LOGGER.warn("[TooSmallCqOffset] maxPhysicalPosInLogicQueue={} clMinOffset={}", maxPhysicalPosInLogicQueue, this.commitLog.getMinOffset());
        }
        LOGGER.info("[SetReputOffset] maxPhysicalPosInLogicQueue={} clMinOffset={} clMaxOffset={} clConfirmedOffset={}",
                maxPhysicalPosInLogicQueue, this.commitLog.getMinOffset(), this.commitLog.getMaxOffset(), this.commitLog.getConfirmOffset());
        this.reputMessageService.setReputFromOffset(maxPhysicalPosInLogicQueue);

        /**
         *  1. Finish dispatching the messages fall behind, then to start other services.
         *  2. DLedger committedPos may be missing, so here just require dispatchBehindBytes <= 0
         */
        while (true) {
            if (dispatchBehindBytes() <= 0) {
                break;
            }
            Thread.sleep(1000);
            LOGGER.info("Try to finish doing reput the messages fall behind during the starting, reputOffset={} maxOffset={} behind={}", this.reputMessageService.getReputFromOffset(), this.getMaxPhyOffset(), this.dispatchBehindBytes());
        }
        this.recoverTopicQueueTable();
    }

    @Override
    public void shutdown() {
        if (!this.shutdown) {
            this.shutdown = true;

            this.scheduledExecutorService.shutdown();
            try {
                Thread.sleep(1000 * 3);
            } catch (InterruptedException e) {
                LOGGER.error("shutdown Exception, ", e);
            }

            if (this.haService != null) {
                this.haService.shutdown();
            }

            this.storeStatsService.shutdown();
            this.indexService.shutdown();
            this.commitLog.shutdown();
            this.reputMessageService.shutdown();
            this.flushConsumeQueueService.shutdown();
            this.allocateMappedFileService.shutdown();
            this.storeCheckpoint.flush();
            this.storeCheckpoint.shutdown();

            this.perfs.shutdown();

            if (this.runningFlags.isWriteable() && dispatchBehindBytes() == 0) {
                this.deleteFile(StorePathConfigHelper.getAbortFile(this.messageStoreConfig.getStorePathRootDir()));
                shutDownNormal = true;
            } else {
                LOGGER.warn("the store may be wrong, so shutdown abnormally, and keep abort file.");
            }
        }

        this.transientStorePool.destroy();

        if (lockFile != null && lock != null) {
            try {
                lock.release();
                lockFile.close();
            } catch (IOException e) {
            }
        }
    }

    @Override
    public void destroy() {
        this.destroyLogics();
        this.commitLog.destroy();
        this.indexService.destroy();
        this.deleteFile(StorePathConfigHelper.getAbortFile(this.messageStoreConfig.getStorePathRootDir()));
        this.deleteFile(StorePathConfigHelper.getStoreCheckpoint(this.messageStoreConfig.getStorePathRootDir()));
    }

    public long getMajorFileSize() {
        long commitLogSize = 0;
        if (this.commitLog != null) {
            commitLogSize = this.commitLog.getTotalSize();
        }

        long consumeQueueSize = 0;
        if (this.consumeQueueStore != null) {
            consumeQueueSize = this.consumeQueueStore.getTotalSize();
        }

        long indexFileSize = 0;
        if (this.indexService != null) {
            indexFileSize = this.indexService.getTotalSize();
        }

        return commitLogSize + consumeQueueSize + indexFileSize;
    }

    @Override
    public void destroyLogics() {
        this.consumeQueueStore.destroy();
    }

    /**
     * 异步消息存储方法
     *
     * @param msg MessageInstance to store
     * @return
     */
    @Override
    public CompletableFuture<PutMessageResult> asyncPutMessage(MessageExtBrokerInner msg) {
        //遍历消息存储钩子函数
        for (PutMessageHook putMessageHook : putMessageHookList) {
            PutMessageResult handleResult = putMessageHook.executeBeforePutMessage(msg);
            //如果不为null的话表示执行的时候出现了异常或者其他问题，此时直接返回即可
            if (handleResult != null) {
                return CompletableFuture.completedFuture(handleResult);
            }
        }
        //校验消息
        if (msg.getProperties().containsKey(MessageConst.PROPERTY_INNER_NUM)
                && !MessageSysFlag.check(msg.getSysFlag(), MessageSysFlag.INNER_BATCH_FLAG)) {
            LOGGER.warn("[BUG]The message had property {} but is not an inner batch", MessageConst.PROPERTY_INNER_NUM);
            return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, null));
        }
        //校验消息
        if (MessageSysFlag.check(msg.getSysFlag(), MessageSysFlag.INNER_BATCH_FLAG)) {
            Optional<TopicConfig> topicConfig = this.getTopicConfig(msg.getTopic());
            if (!QueueTypeUtils.isBatchCq(topicConfig)) {
                LOGGER.error("[BUG]The message is an inner batch but cq type is not batch cq");
                return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, null));
            }
        }
        //开始时间
        long beginTime = this.getSystemClock().now();
        //将消息写入CommitLog 文件，具体实现类 CommitLog。
        CompletableFuture<PutMessageResult> putResultFuture = this.commitLog.asyncPutMessage(msg);

        putResultFuture.thenAccept(result -> {
            long elapsedTime = this.getSystemClock().now() - beginTime;
            if (elapsedTime > 500) {
                LOGGER.warn("DefaultMessageStore#putMessage: CommitLog#putMessage cost {}ms, topic={}, bodyLength={}",
                        elapsedTime, msg.getTopic(), msg.getBody().length);
            }
            //代码@3：记录相关统计信息。
            this.storeStatsService.setPutMessageEntireTimeMax(elapsedTime);

            if (null == result || !result.isOk()) {
                //代码@4：记录写commitlog 失败次数。
                this.storeStatsService.getPutMessageFailedTimes().add(1);
            }
        });

        return putResultFuture;
    }

    @Override
    public CompletableFuture<PutMessageResult> asyncPutMessages(MessageExtBatch messageExtBatch) {

        for (PutMessageHook putMessageHook : putMessageHookList) {
            PutMessageResult handleResult = putMessageHook.executeBeforePutMessage(messageExtBatch);
            if (handleResult != null) {
                return CompletableFuture.completedFuture(handleResult);
            }
        }

        long beginTime = this.getSystemClock().now();
        CompletableFuture<PutMessageResult> putResultFuture = this.commitLog.asyncPutMessages(messageExtBatch);

        putResultFuture.thenAccept(result -> {
            long eclipseTime = this.getSystemClock().now() - beginTime;
            if (eclipseTime > 500) {
                LOGGER.warn("not in lock eclipse time(ms)={}, bodyLength={}", eclipseTime, messageExtBatch.getBody().length);
            }
            this.storeStatsService.setPutMessageEntireTimeMax(eclipseTime);

            if (null == result || !result.isOk()) {
                this.storeStatsService.getPutMessageFailedTimes().add(1);
            }
        });

        return putResultFuture;
    }

    /**
     * 消息存储方法
     *
     * @param msg Message instance to store
     * @return
     */
    @Override
    public PutMessageResult putMessage(MessageExtBrokerInner msg) {
        return waitForPutResult(asyncPutMessage(msg));
    }

    @Override
    public PutMessageResult putMessages(MessageExtBatch messageExtBatch) {
        return waitForPutResult(asyncPutMessages(messageExtBatch));
    }

    /**
     * 等待返回结果的消息存储方法
     *
     * @param putMessageResultFuture
     * @return
     */
    private PutMessageResult waitForPutResult(CompletableFuture<PutMessageResult> putMessageResultFuture) {
        try {
            int putMessageTimeout =
                    Math.max(this.messageStoreConfig.getSyncFlushTimeout(),
                            this.messageStoreConfig.getSlaveTimeout()) + 5000;
            return putMessageResultFuture.get(putMessageTimeout, TimeUnit.MILLISECONDS);
        } catch (ExecutionException | InterruptedException e) {
            return new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, null);
        } catch (TimeoutException e) {
            LOGGER.error("usually it will never timeout, putMessageTimeout is much bigger than slaveTimeout and "
                    + "flushTimeout so the result can be got anyway, but in some situations timeout will happen like full gc "
                    + "process hangs or other unexpected situations.");
            return new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, null);
        }
    }

    //代码@1：检测操作系统页写入是否繁忙。
    @Override
    public boolean isOSPageCacheBusy() {
        long begin = this.getCommitLog().getBeginTimeInLock();
        long diff = this.systemClock.now() - begin;

        return diff < 10000000
                && diff > this.messageStoreConfig.getOsPageCacheBusyTimeOutMills();
    }

    @Override
    public long lockTimeMills() {
        return this.commitLog.lockTimeMills();
    }

    @Override
    public long getMasterFlushedOffset() {
        return this.masterFlushedOffset;
    }

    @Override
    public void setMasterFlushedOffset(long masterFlushedOffset) {
        this.masterFlushedOffset = masterFlushedOffset;
        this.storeCheckpoint.setMasterFlushedOffset(masterFlushedOffset);
    }

    @Override
    public long getBrokerInitMaxOffset() {
        return this.brokerInitMaxOffset;
    }

    @Override
    public void setBrokerInitMaxOffset(long brokerInitMaxOffset) {
        this.brokerInitMaxOffset = brokerInitMaxOffset;
    }

    public SystemClock getSystemClock() {
        return systemClock;
    }

    @Override
    public CommitLog getCommitLog() {
        return commitLog;
    }

    public void truncateDirtyFiles(long offsetToTruncate) {
        if (offsetToTruncate >= this.getMaxPhyOffset()) {
            return;
        }

        this.reputMessageService.shutdown();

        long oldReputFromOffset = this.reputMessageService.getReputFromOffset();

        // truncate commitLog
        this.commitLog.truncateDirtyFiles(offsetToTruncate);

        // truncate consume queue
        this.truncateDirtyLogicFiles(offsetToTruncate);

        recoverTopicQueueTable();

        this.reputMessageService = new ReputMessageService();
        this.reputMessageService.setReputFromOffset(Math.min(oldReputFromOffset, offsetToTruncate));
        this.reputMessageService.start();
    }

    @Override
    public boolean truncateFiles(long offsetToTruncate) {
        if (offsetToTruncate >= this.getMaxPhyOffset()) {
            return true;
        }

        if (!isOffsetAligned(offsetToTruncate)) {
            LOGGER.error("Offset {} not align, truncate failed, need manual fix");
            return false;
        }
        truncateDirtyFiles(offsetToTruncate);
        return true;
    }

    @Override
    public boolean isOffsetAligned(long offset) {
        SelectMappedBufferResult mappedBufferResult = this.getCommitLogData(offset);

        if (mappedBufferResult == null) {
            return true;
        }

        DispatchRequest dispatchRequest = this.commitLog.checkMessageAndReturnSize(mappedBufferResult.getByteBuffer(), true, false);
        return dispatchRequest.isSuccess();
    }

    /**
     * @param group         Consumer group that launches this query. 消费组名称。
     * @param topic         Topic to query. 主题名称。
     * @param queueId       Queue ID to query. 队列ID。
     * @param offset        Logical offset to start from. 待拉取偏移量。
     * @param maxMsgNums    Maximum count of messages to query. 最大拉取消息条数。
     * @param messageFilter Message filter used to screen desired messages. 消息过滤器。
     * @return
     */
    @Override
    public GetMessageResult getMessage(final String group, final String topic, final int queueId, final long offset,
                                       final int maxMsgNums,
                                       final MessageFilter messageFilter) {
        return getMessage(group, topic, queueId, offset, maxMsgNums, MAX_PULL_MSG_SIZE, messageFilter);
    }

    @Override
    public GetMessageResult getMessage(final String group, final String topic, final int queueId, final long offset,
                                       final int maxMsgNums,
                                       final int maxTotalMsgSize,
                                       final MessageFilter messageFilter) {
        if (this.shutdown) {
            LOGGER.warn("message store has shutdown, so getMessage is forbidden");
            return null;
        }

        if (!this.runningFlags.isReadable()) {
            LOGGER.warn("message store is not readable, so getMessage is forbidden " + this.runningFlags.getFlagBits());
            return null;
        }

        long beginTime = this.getSystemClock().now();
        //根据主题名称与队列编号获取消息消费队列
        GetMessageStatus status = GetMessageStatus.NO_MESSAGE_IN_QUEUE;
        //待查找队列的偏移量
        long nextBeginOffset = offset;
        //当前消息队列的最小偏移量
        long minOffset = 0;
        //当前消息队列的最大偏移量。
        long maxOffset = 0;

        //根据主题名称与队列编号获取消息消费队列
        GetMessageResult getResult = new GetMessageResult();
        //当前CommitLog文件的最大偏移量。
        final long maxOffsetPy = this.commitLog.getMaxOffset();

        //第四步：消息偏移量异常情况校对下一次拉取偏移量。
        //1）maxOffset=0：表示当前消费队列中没有消息，拉取结果为
        //NO_MESSAGE_IN_QUEUE。如果当前Broker为主节点，下次拉取偏移量为
        //0。如果当前Broker为从节点并且offsetCheckInSlave为true，设置下
        //次拉取偏移量为0。其他情况下次拉取时使用原偏移量。
        //2）offset<minOffset：表示待拉取消息偏移量小于队列的起始偏
        //移量，拉取结果为OFFSET_TOO_SMALL。如果当前Broker为主节点，下
        //次拉取偏移量为队列的最小偏移量。如果当前Broker为从节点并且
        //offsetCheckInSlave为true，下次拉取偏移量为队列的最小偏移量。
        //其他情况下次拉取时使用原偏移量。
        //3）offset==maxOffset：如果待拉取偏移量等于队列最大偏移
        //量，拉取结果为OFFSET_OVERFLOW_ONE，则下次拉取偏移量依然为
        //offset。
        //4）offset>maxOffset：表示偏移量越界，拉取结果为
        //OFFSET_OVERFLOW_BADLY。此时需要考虑当前队列的偏移量是否为0，
        //如果当前队列的最小偏移量为0，则使用最小偏移量纠正下次拉取偏移
        //量。如果当前队列的最小偏移量不为0，则使用该队列的最大偏移量来
        //纠正下次拉取偏移量。纠正逻辑与1）、2）相同。
        //第五步：如果待拉取偏移量大于minOffset并且小于maxOffset，
        //从当前offset处尝试拉取32条消息，在第4章详细介绍了根据消息队列
        //偏移量（ConsumeQueue）从CommitLog文件中查找消息的过程，在这里
        //就不重复介绍了。
        ConsumeQueueInterface consumeQueue = findConsumeQueue(topic, queueId);
        if (consumeQueue != null) {
            minOffset = consumeQueue.getMinOffsetInQueue();
            maxOffset = consumeQueue.getMaxOffsetInQueue();
            //1）maxOffset=0：表示当前消费队列中没有消息，拉取结果为
            //NO_MESSAGE_IN_QUEUE。如果当前Broker为主节点，下次拉取偏移量为
            //0。如果当前Broker为从节点并且offsetCheckInSlave为true，设置下
            //次拉取偏移量为0。其他情况下次拉取时使用原偏移量。
            if (maxOffset == 0) {
                status = GetMessageStatus.NO_MESSAGE_IN_QUEUE;
                nextBeginOffset = nextOffsetCorrection(offset, 0);
            } else if (offset < minOffset) {
                //2）offset<minOffset：表示待拉取消息偏移量小于队列的起始偏
                //移量，拉取结果为OFFSET_TOO_SMALL。如果当前Broker为主节点，下
                //次拉取偏移量为队列的最小偏移量。如果当前Broker为从节点并且
                //offsetCheckInSlave为true，下次拉取偏移量为队列的最小偏移量。
                //其他情况下次拉取时使用原偏移量。
                status = GetMessageStatus.OFFSET_TOO_SMALL;
                nextBeginOffset = nextOffsetCorrection(offset, minOffset);
            } else if (offset == maxOffset) {
                //3）offset==maxOffset：如果待拉取偏移量等于队列最大偏移
                //量，拉取结果为OFFSET_OVERFLOW_ONE，则下次拉取偏移量依然为
                //offset。
                status = GetMessageStatus.OFFSET_OVERFLOW_ONE;
                nextBeginOffset = nextOffsetCorrection(offset, offset);
            } else if (offset > maxOffset) {
                //4）offset>maxOffset：表示偏移量越界，拉取结果为
                //OFFSET_OVERFLOW_BADLY。此时需要考虑当前队列的偏移量是否为0，
                //如果当前队列的最小偏移量为0，则使用最小偏移量纠正下次拉取偏移
                //量。如果当前队列的最小偏移量不为0，则使用该队列的最大偏移量来
                //纠正下次拉取偏移量。纠正逻辑与1）、2）相同。
                status = GetMessageStatus.OFFSET_OVERFLOW_BADLY;
                nextBeginOffset = nextOffsetCorrection(offset, maxOffset);
            } else {
                final int maxFilterMessageCount = Math.max(16000, maxMsgNums * ConsumeQueue.CQ_STORE_UNIT_SIZE);
                final boolean diskFallRecorded = this.messageStoreConfig.isDiskFallRecorded();

                long maxPullSize = Math.max(maxTotalMsgSize, 100);
                if (maxPullSize > MAX_PULL_MSG_SIZE) {
                    LOGGER.warn("The max pull size is too large maxPullSize={} topic={} queueId={}", maxPullSize, topic, queueId);
                    maxPullSize = MAX_PULL_MSG_SIZE;
                }
                status = GetMessageStatus.NO_MATCHED_MESSAGE;
                long maxPhyOffsetPulling = 0;
                int cqFileNum = 0;

                while (getResult.getBufferTotalSize() <= 0
                        && nextBeginOffset < maxOffset
                        && cqFileNum++ < this.messageStoreConfig.getTravelCqFileNumWhenGetMessage()) {
                    ReferredIterator<CqUnit> bufferConsumeQueue = consumeQueue.iterateFrom(nextBeginOffset);

                    if (bufferConsumeQueue == null) {
                        status = GetMessageStatus.OFFSET_FOUND_NULL;
                        nextBeginOffset = nextOffsetCorrection(nextBeginOffset, this.consumeQueueStore.rollNextFile(consumeQueue, nextBeginOffset));
                        LOGGER.warn("consumer request topic: " + topic + "offset: " + offset + " minOffset: " + minOffset + " maxOffset: "
                                + maxOffset + ", but access logic queue failed. Correct nextBeginOffset to " + nextBeginOffset);
                        break;
                    }

                    try {
                        long nextPhyFileStartOffset = Long.MIN_VALUE;
                        while (bufferConsumeQueue.hasNext()
                                && nextBeginOffset < maxOffset) {
                            CqUnit cqUnit = bufferConsumeQueue.next();
                            long offsetPy = cqUnit.getPos();
                            int sizePy = cqUnit.getSize();

                            boolean isInDisk = checkInDiskByCommitOffset(offsetPy, maxOffsetPy);

                            if (cqUnit.getQueueOffset() - offset > maxFilterMessageCount) {
                                break;
                            }

                            if (this.isTheBatchFull(sizePy, cqUnit.getBatchNum(), maxMsgNums, maxPullSize, getResult.getBufferTotalSize(), getResult.getMessageCount(), isInDisk)) {
                                break;
                            }

                            if (getResult.getBufferTotalSize() >= maxPullSize) {
                                break;
                            }

                            maxPhyOffsetPulling = offsetPy;

                            //Be careful, here should before the isTheBatchFull
                            nextBeginOffset = cqUnit.getQueueOffset() + cqUnit.getBatchNum();

                            if (nextPhyFileStartOffset != Long.MIN_VALUE) {
                                if (offsetPy < nextPhyFileStartOffset) {
                                    continue;
                                }
                            }

                            if (messageFilter != null
                                    && !messageFilter.isMatchedByConsumeQueue(cqUnit.getValidTagsCodeAsLong(), cqUnit.getCqExtUnit())) {
                                if (getResult.getBufferTotalSize() == 0) {
                                    status = GetMessageStatus.NO_MATCHED_MESSAGE;
                                }

                                continue;
                            }

                            SelectMappedBufferResult selectResult = this.commitLog.getMessage(offsetPy, sizePy);
                            if (null == selectResult) {
                                if (getResult.getBufferTotalSize() == 0) {
                                    status = GetMessageStatus.MESSAGE_WAS_REMOVING;
                                }

                                nextPhyFileStartOffset = this.commitLog.rollNextFile(offsetPy);
                                continue;
                            }

                            if (messageFilter != null
                                    && !messageFilter.isMatchedByCommitLog(selectResult.getByteBuffer().slice(), null)) {
                                if (getResult.getBufferTotalSize() == 0) {
                                    status = GetMessageStatus.NO_MATCHED_MESSAGE;
                                }
                                // release...
                                selectResult.release();
                                continue;
                            }

                            this.storeStatsService.getGetMessageTransferredMsgCount().add(cqUnit.getBatchNum());
                            getResult.addMessage(selectResult, cqUnit.getQueueOffset(), cqUnit.getBatchNum());
                            status = GetMessageStatus.FOUND;
                            nextPhyFileStartOffset = Long.MIN_VALUE;
                        }
                    } finally {
                        bufferConsumeQueue.release();
                    }
                }

                if (diskFallRecorded) {
                    long fallBehind = maxOffsetPy - maxPhyOffsetPulling;
                    brokerStatsManager.recordDiskFallBehindSize(group, topic, queueId, fallBehind);
                }

                long diff = maxOffsetPy - maxPhyOffsetPulling;
                long memory = (long) (StoreUtil.TOTAL_PHYSICAL_MEMORY_SIZE
                        * (this.messageStoreConfig.getAccessMessageInMemoryMaxRatio() / 100.0));
                getResult.setSuggestPullingFromSlave(diff > memory);
            }
        } else {
            status = GetMessageStatus.NO_MATCHED_LOGIC_QUEUE;
            nextBeginOffset = nextOffsetCorrection(offset, 0);
        }

        if (GetMessageStatus.FOUND == status) {
            this.storeStatsService.getGetMessageTimesTotalFound().add(1);
        } else {
            this.storeStatsService.getGetMessageTimesTotalMiss().add(1);
        }
        long elapsedTime = this.getSystemClock().now() - beginTime;
        this.storeStatsService.setGetMessageEntireTimeMax(elapsedTime);

        // lazy init no data found.
        if (getResult == null) {
            getResult = new GetMessageResult(0);
        }

        getResult.setStatus(status);
        getResult.setNextBeginOffset(nextBeginOffset);
        getResult.setMaxOffset(maxOffset);
        getResult.setMinOffset(minOffset);
        return getResult;
    }

    @Override
    public long getMaxOffsetInQueue(String topic, int queueId) {
        return getMaxOffsetInQueue(topic, queueId, true);
    }

    @Override
    public long getMaxOffsetInQueue(String topic, int queueId, boolean committed) {
        if (committed) {
            ConsumeQueueInterface logic = this.findConsumeQueue(topic, queueId);
            if (logic != null) {
                return logic.getMaxOffsetInQueue();
            }
        } else {
            Long offset = this.consumeQueueStore.getMaxOffset(topic, queueId);
            if (offset != null) {
                return offset;
            }
        }

        return 0;
    }

    @Override
    public long getMinOffsetInQueue(String topic, int queueId) {
        ConsumeQueueInterface logic = this.findConsumeQueue(topic, queueId);
        if (logic != null) {
            return logic.getMinOffsetInQueue();
        }

        return -1;
    }

    @Override
    public TimerMessageStore getTimerMessageStore() {
        return this.timerMessageStore;
    }

    @Override
    public void setTimerMessageStore(TimerMessageStore timerMessageStore) {
        this.timerMessageStore = timerMessageStore;
    }

    @Override
    public long getCommitLogOffsetInQueue(String topic, int queueId, long consumeQueueOffset) {
        ConsumeQueueInterface consumeQueue = findConsumeQueue(topic, queueId);
        if (consumeQueue != null) {

            ReferredIterator<CqUnit> bufferConsumeQueue = consumeQueue.iterateFrom(consumeQueueOffset);
            if (bufferConsumeQueue != null) {
                try {
                    if (bufferConsumeQueue.hasNext()) {
                        long offsetPy = bufferConsumeQueue.next().getPos();
                        return offsetPy;
                    }
                } finally {
                    bufferConsumeQueue.release();
                }
            }
        }

        return 0;
    }

    @Override
    public long getOffsetInQueueByTime(String topic, int queueId, long timestamp) {
        ConsumeQueueInterface logic = this.findConsumeQueue(topic, queueId);
        if (logic != null) {
            long resultOffset = logic.getOffsetInQueueByTime(timestamp);
            // Make sure the result offset is in valid range.
            resultOffset = Math.max(resultOffset, logic.getMinOffsetInQueue());
            resultOffset = Math.min(resultOffset, logic.getMaxOffsetInQueue());
            return resultOffset;
        }

        return 0;
    }

    @Override
    public MessageExt lookMessageByOffset(long commitLogOffset) {
        SelectMappedBufferResult sbr = this.commitLog.getMessage(commitLogOffset, 4);
        if (null != sbr) {
            try {
                // 1 TOTALSIZE
                int size = sbr.getByteBuffer().getInt();
                return lookMessageByOffset(commitLogOffset, size);
            } finally {
                sbr.release();
            }
        }

        return null;
    }

    @Override
    public SelectMappedBufferResult selectOneMessageByOffset(long commitLogOffset) {
        SelectMappedBufferResult sbr = this.commitLog.getMessage(commitLogOffset, 4);
        if (null != sbr) {
            try {
                // 1 TOTALSIZE
                int size = sbr.getByteBuffer().getInt();
                return this.commitLog.getMessage(commitLogOffset, size);
            } finally {
                sbr.release();
            }
        }

        return null;
    }

    @Override
    public SelectMappedBufferResult selectOneMessageByOffset(long commitLogOffset, int msgSize) {
        return this.commitLog.getMessage(commitLogOffset, msgSize);
    }

    @Override
    public String getRunningDataInfo() {
        return this.storeStatsService.toString();
    }

    public String getStorePathPhysic() {
        String storePathPhysic;
        if (DefaultMessageStore.this.getMessageStoreConfig().isEnableDLegerCommitLog()) {
            storePathPhysic = ((DLedgerCommitLog) DefaultMessageStore.this.getCommitLog()).getdLedgerServer().getdLedgerConfig().getDataStorePath();
        } else {
            storePathPhysic = DefaultMessageStore.this.getMessageStoreConfig().getStorePathCommitLog();
        }
        return storePathPhysic;
    }

    public String getStorePathLogic() {
        return StorePathConfigHelper.getStorePathConsumeQueue(this.messageStoreConfig.getStorePathRootDir());
    }

    @Override
    public HashMap<String, String> getRuntimeInfo() {
        HashMap<String, String> result = this.storeStatsService.getRuntimeInfo();

        {
            double minPhysicsUsedRatio = Double.MAX_VALUE;
            String commitLogStorePath = getStorePathPhysic();
            String[] paths = commitLogStorePath.trim().split(MixAll.MULTI_PATH_SPLITTER);
            for (String clPath : paths) {
                double physicRatio = UtilAll.isPathExists(clPath) ?
                        UtilAll.getDiskPartitionSpaceUsedPercent(clPath) : -1;
                result.put(RunningStats.commitLogDiskRatio.name() + "_" + clPath, String.valueOf(physicRatio));
                minPhysicsUsedRatio = Math.min(minPhysicsUsedRatio, physicRatio);
            }
            result.put(RunningStats.commitLogDiskRatio.name(), String.valueOf(minPhysicsUsedRatio));
        }

        {
            double logicsRatio = UtilAll.getDiskPartitionSpaceUsedPercent(getStorePathLogic());
            result.put(RunningStats.consumeQueueDiskRatio.name(), String.valueOf(logicsRatio));
        }

        result.put(RunningStats.commitLogMinOffset.name(), String.valueOf(DefaultMessageStore.this.getMinPhyOffset()));
        result.put(RunningStats.commitLogMaxOffset.name(), String.valueOf(DefaultMessageStore.this.getMaxPhyOffset()));

        return result;
    }

    @Override
    public long getMaxPhyOffset() {
        return this.commitLog.getMaxOffset();
    }

    @Override
    public long getMinPhyOffset() {
        return this.commitLog.getMinOffset();
    }

    @Override
    public long getLastFileFromOffset() {
        return this.commitLog.getLastFileFromOffset();
    }

    @Override
    public boolean getLastMappedFile(long startOffset) {
        return this.commitLog.getLastMappedFile(startOffset);
    }

    @Override
    public long getEarliestMessageTime(String topic, int queueId) {
        ConsumeQueueInterface logicQueue = this.findConsumeQueue(topic, queueId);
        if (logicQueue != null) {
            return getStoreTime(logicQueue.getEarliestUnit());
        }

        return -1;
    }

    protected long getStoreTime(CqUnit result) {
        if (result != null) {
            try {
                final long phyOffset = result.getPos();
                final int size = result.getSize();
                long storeTime = this.getCommitLog().pickupStoreTimestamp(phyOffset, size);
                return storeTime;
            } catch (Exception e) {
            }
        }
        return -1;
    }

    @Override
    public long getEarliestMessageTime() {
        long minPhyOffset = this.getMinPhyOffset();
        if (this.getCommitLog() instanceof DLedgerCommitLog) {
            minPhyOffset += DLedgerEntry.BODY_OFFSET;
        }
        final int size = this.messageStoreConfig.getMaxMessageSize() * 2;
        return this.getCommitLog().pickupStoreTimestamp(minPhyOffset, size);
    }

    @Override
    public long getMessageStoreTimeStamp(String topic, int queueId, long consumeQueueOffset) {
        ConsumeQueueInterface logicQueue = this.findConsumeQueue(topic, queueId);
        if (logicQueue != null) {
            return getStoreTime(logicQueue.get(consumeQueueOffset));
        }

        return -1;
    }

    @Override
    public long getMessageTotalInQueue(String topic, int queueId) {
        ConsumeQueueInterface logicQueue = this.findConsumeQueue(topic, queueId);
        if (logicQueue != null) {
            return logicQueue.getMessageTotalInQueue();
        }

        return -1;
    }

    @Override
    public SelectMappedBufferResult getCommitLogData(final long offset) {
        if (this.shutdown) {
            LOGGER.warn("message store has shutdown, so getPhyQueueData is forbidden");
            return null;
        }

        return this.commitLog.getData(offset);
    }

    @Override
    public List<SelectMappedBufferResult> getBulkCommitLogData(final long offset, final int size) {
        if (this.shutdown) {
            LOGGER.warn("message store has shutdown, so getBulkCommitLogData is forbidden");
            return null;
        }

        return this.commitLog.getBulkData(offset, size);
    }

    @Override
    public boolean appendToCommitLog(long startOffset, byte[] data, int dataStart, int dataLength) {
        if (this.shutdown) {
            LOGGER.warn("message store has shutdown, so appendToCommitLog is forbidden");
            return false;
        }

        boolean result = this.commitLog.appendData(startOffset, data, dataStart, dataLength);
        if (result) {
            this.reputMessageService.wakeup();
        } else {
            LOGGER.error(
                    "DefaultMessageStore#appendToCommitLog: failed to append data to commitLog, physical offset={}, data "
                            + "length={}", startOffset, data.length);
        }

        return result;
    }

    @Override
    public void executeDeleteFilesManually() {
        this.cleanCommitLogService.executeDeleteFilesManually();
    }

    @Override
    public QueryMessageResult queryMessage(String topic, String key, int maxNum, long begin, long end) {
        QueryMessageResult queryMessageResult = new QueryMessageResult();

        long lastQueryMsgTime = end;

        for (int i = 0; i < 3; i++) {
            QueryOffsetResult queryOffsetResult = this.indexService.queryOffset(topic, key, maxNum, begin, lastQueryMsgTime);
            if (queryOffsetResult.getPhyOffsets().isEmpty()) {
                break;
            }

            Collections.sort(queryOffsetResult.getPhyOffsets());

            queryMessageResult.setIndexLastUpdatePhyoffset(queryOffsetResult.getIndexLastUpdatePhyoffset());
            queryMessageResult.setIndexLastUpdateTimestamp(queryOffsetResult.getIndexLastUpdateTimestamp());

            for (int m = 0; m < queryOffsetResult.getPhyOffsets().size(); m++) {
                long offset = queryOffsetResult.getPhyOffsets().get(m);

                try {
                    MessageExt msg = this.lookMessageByOffset(offset);
                    if (0 == m) {
                        lastQueryMsgTime = msg.getStoreTimestamp();
                    }

                    SelectMappedBufferResult result = this.commitLog.getData(offset, false);
                    if (result != null) {
                        int size = result.getByteBuffer().getInt(0);
                        result.getByteBuffer().limit(size);
                        result.setSize(size);
                        queryMessageResult.addMessage(result);
                    }
                } catch (Exception e) {
                    LOGGER.error("queryMessage exception", e);
                }
            }

            if (queryMessageResult.getBufferTotalSize() > 0) {
                break;
            }

            if (lastQueryMsgTime < begin) {
                break;
            }
        }

        return queryMessageResult;
    }

    @Override
    public void updateHaMasterAddress(String newAddr) {
        if (this.haService != null) {
            this.haService.updateHaMasterAddress(newAddr);
        }
    }

    @Override
    public void updateMasterAddress(String newAddr) {
        if (this.haService != null) {
            this.haService.updateMasterAddress(newAddr);
        }
    }

    @Override
    public void setAliveReplicaNumInGroup(int aliveReplicaNums) {
        this.aliveReplicasNum = aliveReplicaNums;
    }

    @Override
    public void wakeupHAClient() {
        if (this.haService != null) {
            this.haService.getHAClient().wakeup();
        }
    }

    @Override
    public int getAliveReplicaNumInGroup() {
        return this.aliveReplicasNum;
    }

    @Override
    public long slaveFallBehindMuch() {
        if (this.haService == null || this.messageStoreConfig.isDuplicationEnable() || this.messageStoreConfig.isEnableDLegerCommitLog()) {
            LOGGER.warn("haServer is null or duplication is enable or enableDLegerCommitLog is true");
            return -1;
        } else {
            return this.commitLog.getMaxOffset() - this.haService.getPush2SlaveMaxOffset().get();
        }

    }

    @Override
    public long now() {
        return this.systemClock.now();
    }

    @Override
    public int cleanUnusedTopic(Set<String> topics) {
        Iterator<Entry<String, ConcurrentMap<Integer, ConsumeQueueInterface>>> it = this.getConsumeQueueTable().entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, ConcurrentMap<Integer, ConsumeQueueInterface>> next = it.next();
            String topic = next.getKey();

            if (!topics.contains(topic) && !TopicValidator.isSystemTopic(topic) && !MixAll.isLmq(topic)) {
                ConcurrentMap<Integer, ConsumeQueueInterface> queueTable = next.getValue();
                for (ConsumeQueueInterface cq : queueTable.values()) {
                    this.consumeQueueStore.destroy(cq);
                    LOGGER.info("cleanUnusedTopic: {} {} ConsumeQueue cleaned",
                            cq.getTopic(),
                            cq.getQueueId()
                    );

                    this.consumeQueueStore.removeTopicQueueTable(cq.getTopic(), cq.getQueueId());
                }
                it.remove();

                if (this.brokerConfig.isAutoDeleteUnusedStats()) {
                    this.brokerStatsManager.onTopicDeleted(topic);
                }

                LOGGER.info("cleanUnusedTopic: {},topic destroyed", topic);
            }
        }

        return 0;
    }

    @Override
    public void cleanExpiredConsumerQueue() {
        long minCommitLogOffset = this.commitLog.getMinOffset();

        this.consumeQueueStore.cleanExpired(minCommitLogOffset);
    }

    public Map<String, Long> getMessageIds(final String topic, final int queueId, long minOffset, long maxOffset,
                                           SocketAddress storeHost) {
        Map<String, Long> messageIds = new HashMap<String, Long>();
        if (this.shutdown) {
            return messageIds;
        }

        ConsumeQueueInterface consumeQueue = findConsumeQueue(topic, queueId);
        if (consumeQueue != null) {
            minOffset = Math.max(minOffset, consumeQueue.getMinOffsetInQueue());
            maxOffset = Math.min(maxOffset, consumeQueue.getMaxOffsetInQueue());

            if (maxOffset == 0) {
                return messageIds;
            }

            long nextOffset = minOffset;
            while (nextOffset < maxOffset) {
                ReferredIterator<CqUnit> bufferConsumeQueue = consumeQueue.iterateFrom(nextOffset);
                try {
                    if (bufferConsumeQueue != null && bufferConsumeQueue.hasNext()) {
                        while (bufferConsumeQueue.hasNext()) {
                            CqUnit cqUnit = bufferConsumeQueue.next();
                            long offsetPy = cqUnit.getPos();
                            InetSocketAddress inetSocketAddress = (InetSocketAddress) storeHost;
                            int msgIdLength = (inetSocketAddress.getAddress() instanceof Inet6Address) ? 16 + 4 + 8 : 4 + 4 + 8;
                            final ByteBuffer msgIdMemory = ByteBuffer.allocate(msgIdLength);
                            String msgId =
                                    MessageDecoder.createMessageId(msgIdMemory, MessageExt.socketAddress2ByteBuffer(storeHost), offsetPy);
                            messageIds.put(msgId, cqUnit.getQueueOffset());
                            nextOffset = cqUnit.getQueueOffset() + cqUnit.getBatchNum();
                            if (nextOffset >= maxOffset) {
                                return messageIds;
                            }
                        }
                    } else {
                        return messageIds;
                    }
                } finally {
                    if (bufferConsumeQueue != null) {
                        bufferConsumeQueue.release();
                    }
                }
            }
        }
        return messageIds;
    }

    @Override
    public boolean checkInDiskByConsumeOffset(final String topic, final int queueId, long consumeOffset) {
        //当前CommitLog文件的最大偏移量。
        final long maxOffsetPy = this.commitLog.getMaxOffset();

        ConsumeQueueInterface consumeQueue = findConsumeQueue(topic, queueId);
        if (consumeQueue != null) {
            CqUnit cqUnit = consumeQueue.get(consumeOffset);

            if (cqUnit != null) {
                long offsetPy = cqUnit.getPos();
                return checkInDiskByCommitOffset(offsetPy, maxOffsetPy);
            } else {
                return false;
            }
        }
        return false;
    }

    @Override
    public long dispatchBehindBytes() {
        return this.reputMessageService.behind();
    }

    @Override
    public long flush() {
        return this.commitLog.flush();
    }

    @Override
    public long getFlushedWhere() {
        return this.commitLog.getFlushedWhere();
    }

    @Override
    public boolean resetWriteOffset(long phyOffset) {
        //copy a new map
        ConcurrentHashMap<String, Long> newMap = new ConcurrentHashMap<>(consumeQueueStore.getTopicQueueTable());
        SelectMappedBufferResult lastBuffer = null;
        long startReadOffset = phyOffset == -1 ? 0 : phyOffset;
        while ((lastBuffer = selectOneMessageByOffset(startReadOffset)) != null) {
            try {
                if (lastBuffer.getStartOffset() > startReadOffset) {
                    startReadOffset = lastBuffer.getStartOffset();
                    continue;
                }

                ByteBuffer bb = lastBuffer.getByteBuffer();
                int magicCode = bb.getInt(bb.position() + 4);
                if (magicCode == CommitLog.BLANK_MAGIC_CODE) {
                    startReadOffset += bb.getInt(bb.position());
                    continue;
                } else if (magicCode != MessageDecoder.MESSAGE_MAGIC_CODE) {
                    throw new RuntimeException("Unknown magicCode: " + magicCode);
                }

                lastBuffer.getByteBuffer().mark();

                DispatchRequest dispatchRequest = checkMessageAndReturnSize(lastBuffer.getByteBuffer(), true, messageStoreConfig.isDuplicationEnable(), true);
                if (!dispatchRequest.isSuccess())
                    break;

                lastBuffer.getByteBuffer().reset();

                MessageExt msg = MessageDecoder.decode(lastBuffer.getByteBuffer(), true, false, false, false, true);
                if (msg == null) {
                    break;
                }
                String key = msg.getTopic() + "-" + msg.getQueueId();
                Long cur = newMap.get(key);
                if (cur != null && cur > msg.getQueueOffset()) {
                    newMap.put(key, msg.getQueueOffset());
                }
                startReadOffset += msg.getStoreSize();
            } catch (Throwable e) {
                LOGGER.error("resetWriteOffset error.", e);
            } finally {
                if (lastBuffer != null)
                    lastBuffer.release();
            }
        }
        if (this.commitLog.resetOffset(phyOffset)) {
            this.consumeQueueStore.setTopicQueueTable(newMap);
            return true;
        } else {
            return false;
        }
    }

    @Override
    public long getConfirmOffset() {
        if (this.brokerConfig.isEnableControllerMode()) {
            return ((AutoSwitchHAService) this.haService).getConfirmOffset();
        }
        return this.commitLog.getConfirmOffset();
    }

    @Override
    public void setConfirmOffset(long phyOffset) {
        this.commitLog.setConfirmOffset(phyOffset);
    }

    @Override
    public byte[] calcDeltaChecksum(long from, long to) {
        if (from < 0 || to <= from) {
            return new byte[0];
        }

        int size = (int) (to - from);

        if (size > this.messageStoreConfig.getMaxChecksumRange()) {
            LOGGER.error("Checksum range from {}, size {} exceeds threshold {}", from, size, this.messageStoreConfig.getMaxChecksumRange());
            return null;
        }

        List<MessageExt> msgList = new ArrayList<>();
        List<SelectMappedBufferResult> bufferResultList = this.getBulkCommitLogData(from, size);
        if (bufferResultList.isEmpty()) {
            return new byte[0];
        }

        for (SelectMappedBufferResult bufferResult : bufferResultList) {
            msgList.addAll(MessageDecoder.decodesBatch(bufferResult.getByteBuffer(), true, false, false));
            bufferResult.release();
        }

        if (msgList.isEmpty()) {
            return new byte[0];
        }

        ByteBuffer byteBuffer = ByteBuffer.allocate(size);
        for (MessageExt msg : msgList) {
            try {
                byteBuffer.put(MessageDecoder.encodeUniquely(msg, false));
            } catch (IOException ignore) {
            }
        }

        return Hashing.murmur3_128().hashBytes(byteBuffer.array()).asBytes();
    }

    @Override
    public void setPhysicalOffset(long phyOffset) {
        this.commitLog.setMappedFileQueueOffset(phyOffset);
    }

    @Override
    public boolean isMappedFilesEmpty() {
        return this.commitLog.isMappedFilesEmpty();
    }

    @Override
    public MessageExt lookMessageByOffset(long commitLogOffset, int size) {
        SelectMappedBufferResult sbr = this.commitLog.getMessage(commitLogOffset, size);
        if (null != sbr) {
            try {
                return MessageDecoder.decode(sbr.getByteBuffer(), true, false);
            } finally {
                sbr.release();
            }
        }

        return null;
    }

    @Override
    public ConsumeQueueInterface findConsumeQueue(String topic, int queueId) {
        return this.consumeQueueStore.findOrCreateConsumeQueue(topic, queueId);
    }

    private long nextOffsetCorrection(long oldOffset, long newOffset) {
        long nextOffset = oldOffset;
        if (this.getMessageStoreConfig().getBrokerRole() != BrokerRole.SLAVE ||
                this.getMessageStoreConfig().isOffsetCheckInSlave()) {
            nextOffset = newOffset;
        }
        return nextOffset;
    }

    private boolean checkInDiskByCommitOffset(long offsetPy, long maxOffsetPy) {
        long memory = (long) (StoreUtil.TOTAL_PHYSICAL_MEMORY_SIZE * (this.messageStoreConfig.getAccessMessageInMemoryMaxRatio() / 100.0));
        return (maxOffsetPy - offsetPy) > memory;
    }

    private boolean isTheBatchFull(int sizePy, int unitBatchNum, int maxMsgNums, long maxMsgSize, int bufferTotal,
                                   int messageTotal, boolean isInDisk) {

        if (0 == bufferTotal || 0 == messageTotal) {
            return false;
        }

        if (messageTotal + unitBatchNum > maxMsgNums) {
            return true;
        }

        if (bufferTotal + sizePy > maxMsgSize) {
            return true;
        }

        if (isInDisk) {
            if ((bufferTotal + sizePy) > this.messageStoreConfig.getMaxTransferBytesOnMessageInDisk()) {
                return true;
            }

            if (messageTotal > this.messageStoreConfig.getMaxTransferCountOnMessageInDisk() - 1) {
                return true;
            }
        } else {
            if ((bufferTotal + sizePy) > this.messageStoreConfig.getMaxTransferBytesOnMessageInMemory()) {
                return true;
            }

            if (messageTotal > this.messageStoreConfig.getMaxTransferCountOnMessageInMemory() - 1) {
                return true;
            }
        }

        return false;
    }

    private void deleteFile(final String fileName) {
        File file = new File(fileName);
        boolean result = file.delete();
        LOGGER.info(fileName + (result ? " delete OK" : " delete Failed"));
    }

    /**
     * @throws IOException
     */
    private void createTempFile() throws IOException {
        String fileName = StorePathConfigHelper.getAbortFile(this.messageStoreConfig.getStorePathRootDir());
        File file = new File(fileName);
        UtilAll.ensureDirOK(file.getParent());
        boolean result = file.createNewFile();
        LOGGER.info(fileName + (result ? " create OK" : " already exists"));
        MixAll.string2File(Long.toString(MixAll.getPID()), file.getAbsolutePath());
    }

    private void addScheduleTask() {

        this.scheduledExecutorService.scheduleAtFixedRate(new AbstractBrokerRunnable(this.getBrokerIdentity()) {
            @Override
            public void run2() {
                DefaultMessageStore.this.cleanFilesPeriodically();
            }
        }, 1000 * 60, this.messageStoreConfig.getCleanResourceInterval(), TimeUnit.MILLISECONDS);

        this.scheduledExecutorService.scheduleAtFixedRate(new AbstractBrokerRunnable(this.getBrokerIdentity()) {
            @Override
            public void run2() {
                DefaultMessageStore.this.checkSelf();
            }
        }, 1, 10, TimeUnit.MINUTES);

        this.scheduledExecutorService.scheduleAtFixedRate(new AbstractBrokerRunnable(this.getBrokerIdentity()) {
            @Override
            public void run2() {
                if (DefaultMessageStore.this.getMessageStoreConfig().isDebugLockEnable()) {
                    try {
                        if (DefaultMessageStore.this.commitLog.getBeginTimeInLock() != 0) {
                            long lockTime = System.currentTimeMillis() - DefaultMessageStore.this.commitLog.getBeginTimeInLock();
                            if (lockTime > 1000 && lockTime < 10000000) {

                                String stack = UtilAll.jstack();
                                final String fileName = System.getProperty("user.home") + File.separator + "debug/lock/stack-"
                                        + DefaultMessageStore.this.commitLog.getBeginTimeInLock() + "-" + lockTime;
                                MixAll.string2FileNotSafe(stack, fileName);
                            }
                        }
                    } catch (Exception e) {
                    }
                }
            }
        }, 1, 1, TimeUnit.SECONDS);

        this.scheduledExecutorService.scheduleAtFixedRate(new AbstractBrokerRunnable(this.getBrokerIdentity()) {
            @Override
            public void run2() {
                DefaultMessageStore.this.storeCheckpoint.flush();
            }
        }, 1, 1, TimeUnit.SECONDS);

        // this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
        // @Override
        // public void run() {
        // DefaultMessageStore.this.cleanExpiredConsumerQueue();
        // }
        // }, 1, 1, TimeUnit.HOURS);
    }

    private void cleanFilesPeriodically() {
        this.cleanCommitLogService.run();
        this.cleanConsumeQueueService.run();
        this.correctLogicOffsetService.run();
    }

    private void checkSelf() {
        this.commitLog.checkSelf();
        this.consumeQueueStore.checkSelf();
    }

    /**
     * 判断临时文件是否存在
     *
     * @return
     */
    private boolean isTempFileExist() {
        String fileName = StorePathConfigHelper.getAbortFile(this.messageStoreConfig.getStorePathRootDir());
        File file = new File(fileName);
        return file.exists();
    }

    private void recover(final boolean lastExitOK) {
        long recoverCqStart = System.currentTimeMillis();
        long maxPhyOffsetOfConsumeQueue = this.recoverConsumeQueue();
        long recoverCqEnd = System.currentTimeMillis();

        if (lastExitOK) {
            this.commitLog.recoverNormally(maxPhyOffsetOfConsumeQueue);
        } else {
            this.commitLog.recoverAbnormally(maxPhyOffsetOfConsumeQueue);
        }
        long recoverClogEnd = System.currentTimeMillis();
        this.recoverTopicQueueTable();
        long recoverOffsetEnd = System.currentTimeMillis();

        LOGGER.info("Recover end total:{} recoverCq:{} recoverClog:{} recoverOffset:{}",
                recoverOffsetEnd - recoverCqStart, recoverCqEnd - recoverCqStart, recoverClogEnd - recoverCqEnd, recoverOffsetEnd - recoverClogEnd);
    }

    @Override
    public long getTimingMessageCount(String topic) {
        if (null == timerMessageStore) {
            return 0L;
        } else {
            return timerMessageStore.getTimerMetrics().getTimingCount(topic);
        }
    }

    @Override
    public MessageStoreConfig getMessageStoreConfig() {
        return messageStoreConfig;
    }

    @Override
    public TransientStorePool getTransientStorePool() {
        return transientStorePool;
    }

    private long recoverConsumeQueue() {
        return this.consumeQueueStore.recover();
    }

    /**
     * 第七步：根据Broker是否为正常停止，执行不同的恢复策略，下
     * 文将分别介绍异常停止、正常停止的文件恢复机制，
     */
    public void recoverTopicQueueTable() {
        long minPhyOffset = this.commitLog.getMinOffset();
        this.consumeQueueStore.recoverOffsetTable(minPhyOffset);
    }

    @Override
    public AllocateMappedFileService getAllocateMappedFileService() {
        return allocateMappedFileService;
    }

    @Override
    public StoreStatsService getStoreStatsService() {
        return storeStatsService;
    }

    public RunningFlags getAccessRights() {
        return runningFlags;
    }

    public ConcurrentMap<String, ConcurrentMap<Integer, ConsumeQueueInterface>> getConsumeQueueTable() {
        return consumeQueueStore.getConsumeQueueTable();
    }

    @Override
    public StoreCheckpoint getStoreCheckpoint() {
        return storeCheckpoint;
    }

    @Override
    public HAService getHaService() {
        return haService;
    }

    @Override
    public RunningFlags getRunningFlags() {
        return runningFlags;
    }

    public void doDispatch(DispatchRequest req) {
        for (CommitLogDispatcher dispatcher : this.dispatcherList) {
            dispatcher.dispatch(req);
        }
    }

    public void putMessagePositionInfo(DispatchRequest dispatchRequest) {
        this.consumeQueueStore.putMessagePositionInfoWrapper(dispatchRequest);
    }

    @Override
    public DispatchRequest checkMessageAndReturnSize(final ByteBuffer byteBuffer, final boolean checkCRC,
                                                     final boolean checkDupInfo, final boolean readBody) {
        return this.commitLog.checkMessageAndReturnSize(byteBuffer, checkCRC, checkDupInfo, readBody);
    }

    @Override
    public long getStateMachineVersion() {
        return 0L;
    }

    public BrokerStatsManager getBrokerStatsManager() {
        return brokerStatsManager;
    }

    public BrokerConfig getBrokerConfig() {
        return brokerConfig;
    }

    public int remainTransientStoreBufferNumbs() {
        return this.transientStorePool.availableBufferNums();
    }

    @Override
    public boolean isTransientStorePoolDeficient() {
        return remainTransientStoreBufferNumbs() == 0;
    }

    @Override
    public long remainHowManyDataToCommit() {
        return this.commitLog.remainHowManyDataToCommit();
    }

    @Override
    public long remainHowManyDataToFlush() {
        return this.commitLog.remainHowManyDataToFlush();
    }

    @Override
    public LinkedList<CommitLogDispatcher> getDispatcherList() {
        return this.dispatcherList;
    }

    @Override
    public void setMasterStoreInProcess(MessageStore masterStoreInProcess) {
        this.masterStoreInProcess = masterStoreInProcess;
    }

    @Override
    public MessageStore getMasterStoreInProcess() {
        return this.masterStoreInProcess;
    }

    @Override
    public boolean getData(long offset, int size, ByteBuffer byteBuffer) {
        return this.commitLog.getData(offset, size, byteBuffer);
    }

    @Override
    public ConsumeQueueInterface getConsumeQueue(String topic, int queueId) {
        ConcurrentMap<Integer, ConsumeQueueInterface> map = this.getConsumeQueueTable().get(topic);
        if (map == null) {
            return null;
        }
        return map.get(queueId);
    }

    @Override
    public void unlockMappedFile(final MappedFile mappedFile) {
        this.scheduledExecutorService.schedule(new Runnable() {
            @Override
            public void run() {
                mappedFile.munlock();
            }
        }, 6, TimeUnit.SECONDS);
    }

    @Override
    public PerfCounter.Ticks getPerfCounter() {
        return perfs;
    }

    @Override
    public ConsumeQueueStore getQueueStore() {
        return consumeQueueStore;
    }

    @Override
    public void onCommitLogAppend(MessageExtBrokerInner msg, AppendMessageResult result, MappedFile commitLogFile) {
        // empty
    }

    @Override
    public void onCommitLogDispatch(DispatchRequest dispatchRequest, boolean doDispatch, MappedFile commitLogFile,
                                    boolean isRecover, boolean isFileEnd) {
        if (doDispatch && !isFileEnd) {
            this.doDispatch(dispatchRequest);
        }
    }

    @Override
    public boolean isSyncDiskFlush() {
        return FlushDiskType.SYNC_FLUSH == this.getMessageStoreConfig().getFlushDiskType();
    }

    @Override
    public boolean isSyncMaster() {
        return BrokerRole.SYNC_MASTER == this.getMessageStoreConfig().getBrokerRole();
    }

    @Override
    public void assignOffset(MessageExtBrokerInner msg, short messageNum) {
        final int tranType = MessageSysFlag.getTransactionValue(msg.getSysFlag());

        if (tranType == MessageSysFlag.TRANSACTION_NOT_TYPE || tranType == MessageSysFlag.TRANSACTION_COMMIT_TYPE) {
            this.consumeQueueStore.assignQueueOffset(msg, messageNum);
        }
    }

    @Override
    public Optional<TopicConfig> getTopicConfig(String topic) {
        return this.consumeQueueStore.getTopicConfig(topic);
    }

    public void setTopicConfigTable(ConcurrentMap<String, TopicConfig> topicConfigTable) {
        this.consumeQueueStore.setTopicConfigTable(topicConfigTable);
    }

    public BrokerIdentity getBrokerIdentity() {
        if (messageStoreConfig.isEnableDLegerCommitLog()) {
            return new BrokerIdentity(
                    brokerConfig.getBrokerClusterName(), brokerConfig.getBrokerName(),
                    Integer.parseInt(messageStoreConfig.getdLegerSelfId().substring(1)), brokerConfig.isInBrokerContainer());
        } else {
            return new BrokerIdentity(
                    brokerConfig.getBrokerClusterName(), brokerConfig.getBrokerName(),
                    brokerConfig.getBrokerId(), brokerConfig.isInBrokerContainer());
        }
    }

    class CommitLogDispatcherBuildConsumeQueue implements CommitLogDispatcher {

        @Override
        public void dispatch(DispatchRequest request) {
            final int tranType = MessageSysFlag.getTransactionValue(request.getSysFlag());
            switch (tranType) {
                case MessageSysFlag.TRANSACTION_NOT_TYPE:
                case MessageSysFlag.TRANSACTION_COMMIT_TYPE:
                    DefaultMessageStore.this.putMessagePositionInfo(request);
                    break;
                case MessageSysFlag.TRANSACTION_PREPARED_TYPE:
                case MessageSysFlag.TRANSACTION_ROLLBACK_TYPE:
                    break;
            }
        }
    }

    class CommitLogDispatcherBuildIndex implements CommitLogDispatcher {
        /**
         * 如果messsageIndexEnable设置为true，则调用
         * IndexService#buildIndex构建哈希索引，否则忽略本次转发任务
         *
         * @param request dispatch message request
         */
        @Override
        public void dispatch(DispatchRequest request) {
            if (DefaultMessageStore.this.messageStoreConfig.isMessageIndexEnable()) {
                DefaultMessageStore.this.indexService.buildIndex(request);
            }
        }
    }

    /**
     * commitLog 过期文件删除服务
     * <p>
     * 因为RocketMQ操作CommitLog、ConsumeQueue文件是基于内存映射
     * 机制并在启动的时候会加载commitlog、consumequeue目录下的所有文
     * 件，所以为了避免内存与磁盘的浪费，不可能将消息永久存储在消息
     * 服务器上，这就需要引入一种机制来删除已过期的文件。RocketMQ顺
     * 序写CommitLog文件、ConsumeQueue文件，所有写操作全部落在最后一
     * 个CommitLog或ConsumeQueue文件上，之前的文件在下一个文件创建后
     * 将不会再被更新。RocketMQ清除过期文件的方法：如果非当前写文件
     * 在一定时间间隔内没有再次更新，则认为是过期文件，可以被删除，
     * RocketMQ不会关注这个文件上的消息是否全部被消费。默认每个文件
     * 的过期时间为72h，通过在broker配置文件中设置fileReservedTime来
     * 改变过期时间，单位为小时。接下来详细分析RocketMQ是如何设计与
     * 实现上述机制的
     * <p>
     * 第一步：解释一下这个3个配置属性的含义。
     * 1）fileReservedTime：文件保留时间，如果超过了该时间，则认
     * 为是过期文件，可以被删除。
     * 2）deletePhysicFilesInterval：删除物理文件的间隔时间，在
     * 一次清除过程中，可能需要被删除的文件不止一个，该值指定两次删
     * 除文件的间隔时间。
     * 3）destroyMapedFileIntervalForcibly：在清除过期文件时，如
     * 果该文件被其他线程占用（引用次数大于0，比如读取消息），此时会
     * 阻止此次删除任务，同时在第一次试图删除该文件时记录当前时间
     * 戳，destroyMapedFileIntervalForcibly表示第一次拒绝删除之后能
     * 保留文件的最大时间，在此时间内，同样可以被拒绝删除，超过该时
     * 间后，会将引用次数设置为负数，文件将被强制删除
     * <p>
     * 第二步：RocketMQ满足如下任意一种情况将继续执行删除文件的
     * 操作。
     * 1）指定删除文件的时间点，RocketMQ通过deleteWhen设置每天在
     * 固定时间执行一次删除过期文件操作，默认凌晨4点。
     * 2）检查磁盘空间是否充足，如果磁盘空间不充足，则返回true，
     * 表示应该触发过期文件删除操作。
     * 3）预留手工触发机制，可以通过调用excuteDeleteFilesManualy
     * 方法手工触发删除过期文件的操作，目前RocketMQ暂未封装手工触发
     * 文件删除的命令。
     * <p>
     * 首先解释一下几个参数的含义。
     * 1）diskMaxUsedSpaceRatio：表示CommitLog文件、ConsumeQueue
     * 文件所在磁盘分区的最大使用量，如果超过该值，则需要立即清除过
     * 期文件。
     * 2）cleanImmediately：表示是否需要立即执行清除过期文件的操
     * 作。
     * 3）physicRatio：当前CommitLog目录所在的磁盘分区的磁盘使用
     * 率，通过File#getTotal Space方法获取文件所在磁盘分区的总容量，
     * 通过File#getFreeSpace方法获取文件所在磁盘分区的剩余容量。
     * 4）diskSpaceWarningLevelRatio：通过系统参数
     * Drocketmq.broker.diskSpaceWarningLevelRatio进行设置，默认
     * 0.90。如果磁盘分区使用率超过该阈值，将设置磁盘为不可写，此时
     * 会拒绝写入新消息。
     * 5）diskSpaceCleanForciblyRatio：通过系统参数
     * Drocketmq.broker.diskSpaceCleanForcibly-Ratio进行设置，默认
     * 0.85。如果磁盘分区使用超过该阈值，建议立即执行过期文件删除，
     * 但不会拒绝写入新消息。
     * <p>
     * 如果当前磁盘分区使用率大于diskSpaceWarningLevelRatio，应
     * 该立即启动过期文件删除操作。如果当前磁盘分区使用率大于
     * diskSpaceCleanForciblyRatio，建议立即执行过期文件清除，如果磁
     * 盘使用率低于diskSpaceCleanForciblyRatio将恢复磁盘可写。如果当
     * 前磁盘使用率小于diskMaxUsedSpaceRatio，则返回false，表示磁盘
     * 使用率正常，否则返回true，需要执行删除过期文件，
     */
    class CleanCommitLogService {

        private final static int MAX_MANUAL_DELETE_FILE_TIMES = 20;
        private final String diskSpaceWarningLevelRatio =
                System.getProperty("rocketmq.broker.diskSpaceWarningLevelRatio", "");

        private final String diskSpaceCleanForciblyRatio =
                System.getProperty("rocketmq.broker.diskSpaceCleanForciblyRatio", "");
        private long lastRedeleteTimestamp = 0;

        private volatile int manualDeleteFileSeveralTimes = 0;

        private volatile boolean cleanImmediately = false;

        private int forceCleanFailedTimes = 0;

        double getDiskSpaceWarningLevelRatio() {
            double finalDiskSpaceWarningLevelRatio;
            if ("".equals(diskSpaceWarningLevelRatio)) {
                finalDiskSpaceWarningLevelRatio = DefaultMessageStore.this.getMessageStoreConfig().getDiskSpaceWarningLevelRatio() / 100.0;
            } else {
                finalDiskSpaceWarningLevelRatio = Double.parseDouble(diskSpaceWarningLevelRatio);
            }

            if (finalDiskSpaceWarningLevelRatio > 0.90) {
                finalDiskSpaceWarningLevelRatio = 0.90;
            }
            if (finalDiskSpaceWarningLevelRatio < 0.35) {
                finalDiskSpaceWarningLevelRatio = 0.35;
            }

            return finalDiskSpaceWarningLevelRatio;
        }

        double getDiskSpaceCleanForciblyRatio() {
            double finalDiskSpaceCleanForciblyRatio;
            if ("".equals(diskSpaceCleanForciblyRatio)) {
                finalDiskSpaceCleanForciblyRatio = DefaultMessageStore.this.getMessageStoreConfig().getDiskSpaceCleanForciblyRatio() / 100.0;
            } else {
                finalDiskSpaceCleanForciblyRatio = Double.parseDouble(diskSpaceCleanForciblyRatio);
            }

            if (finalDiskSpaceCleanForciblyRatio > 0.85) {
                finalDiskSpaceCleanForciblyRatio = 0.85;
            }
            if (finalDiskSpaceCleanForciblyRatio < 0.30) {
                finalDiskSpaceCleanForciblyRatio = 0.30;
            }

            return finalDiskSpaceCleanForciblyRatio;
        }

        public void executeDeleteFilesManually() {
            this.manualDeleteFileSeveralTimes = MAX_MANUAL_DELETE_FILE_TIMES;
            DefaultMessageStore.LOGGER.info("executeDeleteFilesManually was invoked");
        }

        public void run() {
            try {
                this.deleteExpiredFiles();
                this.reDeleteHangedFile();
            } catch (Throwable e) {
                DefaultMessageStore.LOGGER.warn(this.getServiceName() + " service has exception. ", e);
            }
        }

        private void deleteExpiredFiles() {
            int deleteCount = 0;
            long fileReservedTime = DefaultMessageStore.this.getMessageStoreConfig().getFileReservedTime();
            int deletePhysicFilesInterval = DefaultMessageStore.this.getMessageStoreConfig().getDeleteCommitLogFilesInterval();
            int destroyMappedFileIntervalForcibly = DefaultMessageStore.this.getMessageStoreConfig().getDestroyMapedFileIntervalForcibly();
            int deleteFileBatchMax = DefaultMessageStore.this.getMessageStoreConfig().getDeleteFileBatchMax();

            boolean isTimeUp = this.isTimeToDelete();
            boolean isUsageExceedsThreshold = this.isSpaceToDelete();
            boolean isManualDelete = this.manualDeleteFileSeveralTimes > 0;

            if (isTimeUp || isUsageExceedsThreshold || isManualDelete) {

                if (isManualDelete) {
                    this.manualDeleteFileSeveralTimes--;
                }

                boolean cleanAtOnce = DefaultMessageStore.this.getMessageStoreConfig().isCleanFileForciblyEnable() && this.cleanImmediately;

                LOGGER.info("begin to delete before {} hours file. isTimeUp: {} isUsageExceedsThreshold: {} manualDeleteFileSeveralTimes: {} cleanAtOnce: {} deleteFileBatchMax: {}",
                        fileReservedTime,
                        isTimeUp,
                        isUsageExceedsThreshold,
                        manualDeleteFileSeveralTimes,
                        cleanAtOnce,
                        deleteFileBatchMax);

                fileReservedTime *= 60 * 60 * 1000;

                deleteCount = DefaultMessageStore.this.commitLog.deleteExpiredFile(fileReservedTime, deletePhysicFilesInterval,
                        destroyMappedFileIntervalForcibly, cleanAtOnce, deleteFileBatchMax);
                if (deleteCount > 0) {
                    // If in the controller mode, we should notify the AutoSwitchHaService to truncateEpochFile
                    if (DefaultMessageStore.this.brokerConfig.isEnableControllerMode()) {
                        if (DefaultMessageStore.this.haService instanceof AutoSwitchHAService) {
                            final long minPhyOffset = getMinPhyOffset();
                            ((AutoSwitchHAService) DefaultMessageStore.this.haService).truncateEpochFilePrefix(minPhyOffset - 1);
                        }
                    }
                } else if (isUsageExceedsThreshold) {
                    LOGGER.warn("disk space will be full soon, but delete file failed.");
                }
            }
        }

        private void reDeleteHangedFile() {
            int interval = DefaultMessageStore.this.getMessageStoreConfig().getRedeleteHangedFileInterval();
            long currentTimestamp = System.currentTimeMillis();
            if ((currentTimestamp - this.lastRedeleteTimestamp) > interval) {
                this.lastRedeleteTimestamp = currentTimestamp;
                int destroyMappedFileIntervalForcibly =
                        DefaultMessageStore.this.getMessageStoreConfig().getDestroyMapedFileIntervalForcibly();
                if (DefaultMessageStore.this.commitLog.retryDeleteFirstFile(destroyMappedFileIntervalForcibly)) {
                }
            }
        }

        public String getServiceName() {
            return DefaultMessageStore.this.brokerConfig.getLoggerIdentifier() + CleanCommitLogService.class.getSimpleName();
        }

        private boolean isTimeToDelete() {
            String when = DefaultMessageStore.this.getMessageStoreConfig().getDeleteWhen();
            if (UtilAll.isItTimeToDo(when)) {
                DefaultMessageStore.LOGGER.info("it's time to reclaim disk space, " + when);
                return true;
            }

            return false;
        }

        private boolean isSpaceToDelete() {
            cleanImmediately = false;

            String commitLogStorePath = DefaultMessageStore.this.getMessageStoreConfig().getStorePathCommitLog();
            String[] storePaths = commitLogStorePath.trim().split(MixAll.MULTI_PATH_SPLITTER);
            Set<String> fullStorePath = new HashSet<>();
            double minPhysicRatio = 100;
            String minStorePath = null;
            for (String storePathPhysic : storePaths) {
                double physicRatio = UtilAll.getDiskPartitionSpaceUsedPercent(storePathPhysic);
                if (minPhysicRatio > physicRatio) {
                    minPhysicRatio = physicRatio;
                    minStorePath = storePathPhysic;
                }
                if (physicRatio > getDiskSpaceCleanForciblyRatio()) {
                    fullStorePath.add(storePathPhysic);
                }
            }
            DefaultMessageStore.this.commitLog.setFullStorePaths(fullStorePath);
            if (minPhysicRatio > getDiskSpaceWarningLevelRatio()) {
                boolean diskFull = DefaultMessageStore.this.runningFlags.getAndMakeDiskFull();
                if (diskFull) {
                    DefaultMessageStore.LOGGER.error("physic disk maybe full soon " + minPhysicRatio +
                            ", so mark disk full, storePathPhysic=" + minStorePath);
                }

                cleanImmediately = true;
                return true;
            } else if (minPhysicRatio > getDiskSpaceCleanForciblyRatio()) {
                cleanImmediately = true;
                return true;
            } else {
                boolean diskOK = DefaultMessageStore.this.runningFlags.getAndMakeDiskOK();
                if (!diskOK) {
                    DefaultMessageStore.LOGGER.info("physic disk space OK " + minPhysicRatio +
                            ", so mark disk ok, storePathPhysic=" + minStorePath);
                }
            }

            String storePathLogics = StorePathConfigHelper
                    .getStorePathConsumeQueue(DefaultMessageStore.this.getMessageStoreConfig().getStorePathRootDir());
            double logicsRatio = UtilAll.getDiskPartitionSpaceUsedPercent(storePathLogics);
            if (logicsRatio > getDiskSpaceWarningLevelRatio()) {
                boolean diskOK = DefaultMessageStore.this.runningFlags.getAndMakeDiskFull();
                if (diskOK) {
                    DefaultMessageStore.LOGGER.error("logics disk maybe full soon " + logicsRatio + ", so mark disk full");
                }

                cleanImmediately = true;
                return true;
            } else if (logicsRatio > getDiskSpaceCleanForciblyRatio()) {
                cleanImmediately = true;
                return true;
            } else {
                boolean diskOK = DefaultMessageStore.this.runningFlags.getAndMakeDiskOK();
                if (!diskOK) {
                    DefaultMessageStore.LOGGER.info("logics disk space OK " + logicsRatio + ", so mark disk ok");
                }
            }

            double ratio = DefaultMessageStore.this.getMessageStoreConfig().getDiskMaxUsedSpaceRatio() / 100.0;
            int replicasPerPartition = DefaultMessageStore.this.getMessageStoreConfig().getReplicasPerDiskPartition();
            // Only one commitLog in node
            if (replicasPerPartition <= 1) {
                if (minPhysicRatio < 0 || minPhysicRatio > ratio) {
                    DefaultMessageStore.LOGGER.info("commitLog disk maybe full soon, so reclaim space, " + minPhysicRatio);
                    return true;
                }

                if (logicsRatio < 0 || logicsRatio > ratio) {
                    DefaultMessageStore.LOGGER.info("consumeQueue disk maybe full soon, so reclaim space, " + logicsRatio);
                    return true;
                }
                return false;
            } else {
                long majorFileSize = DefaultMessageStore.this.getMajorFileSize();
                long partitionLogicalSize = UtilAll.getDiskPartitionTotalSpace(minStorePath) / replicasPerPartition;
                double logicalRatio = 1.0 * majorFileSize / partitionLogicalSize;

                if (logicalRatio > DefaultMessageStore.this.getMessageStoreConfig().getLogicalDiskSpaceCleanForciblyThreshold()) {
                    // if logical ratio exceeds 0.80, then clean immediately
                    DefaultMessageStore.LOGGER.info("Logical disk usage {} exceeds logical disk space clean forcibly threshold {}, forcibly: {}",
                            logicalRatio, minPhysicRatio, cleanImmediately);
                    cleanImmediately = true;
                    return true;
                }

                boolean isUsageExceedsThreshold = logicalRatio > ratio;
                if (isUsageExceedsThreshold) {
                    DefaultMessageStore.LOGGER.info("Logical disk usage {} exceeds clean threshold {}, forcibly: {}",
                            logicalRatio, ratio, cleanImmediately);
                }
                return isUsageExceedsThreshold;
            }
        }

        public int getManualDeleteFileSeveralTimes() {
            return manualDeleteFileSeveralTimes;
        }

        public void setManualDeleteFileSeveralTimes(int manualDeleteFileSeveralTimes) {
            this.manualDeleteFileSeveralTimes = manualDeleteFileSeveralTimes;
        }

        public double calcStorePathPhysicRatio() {
            Set<String> fullStorePath = new HashSet<>();
            String storePath = getStorePathPhysic();
            String[] paths = storePath.trim().split(MixAll.MULTI_PATH_SPLITTER);
            double minPhysicRatio = 100;
            for (String path : paths) {
                double physicRatio = UtilAll.isPathExists(path) ?
                        UtilAll.getDiskPartitionSpaceUsedPercent(path) : -1;
                minPhysicRatio = Math.min(minPhysicRatio, physicRatio);
                if (physicRatio > getDiskSpaceCleanForciblyRatio()) {
                    fullStorePath.add(path);
                }
            }
            DefaultMessageStore.this.commitLog.setFullStorePaths(fullStorePath);
            return minPhysicRatio;

        }

        public boolean isSpaceFull() {
            double physicRatio = calcStorePathPhysicRatio();
            double ratio = DefaultMessageStore.this.getMessageStoreConfig().getDiskMaxUsedSpaceRatio() / 100.0;
            if (physicRatio > ratio) {
                DefaultMessageStore.LOGGER.info("physic disk of commitLog used: " + physicRatio);
            }
            if (physicRatio > this.getDiskSpaceWarningLevelRatio()) {
                boolean diskok = DefaultMessageStore.this.runningFlags.getAndMakeDiskFull();
                if (diskok) {
                    DefaultMessageStore.LOGGER.error("physic disk of commitLog maybe full soon, used " + physicRatio + ", so mark disk full");
                }

                return true;
            } else {
                boolean diskok = DefaultMessageStore.this.runningFlags.getAndMakeDiskOK();

                if (!diskok) {
                    DefaultMessageStore.LOGGER.info("physic disk space of commitLog OK " + physicRatio + ", so mark disk ok");
                }

                return false;
            }
        }
    }

    class CleanConsumeQueueService {
        private long lastPhysicalMinOffset = 0;

        public void run() {
            try {
                this.deleteExpiredFiles();
            } catch (Throwable e) {
                DefaultMessageStore.LOGGER.warn(this.getServiceName() + " service has exception. ", e);
            }
        }

        private void deleteExpiredFiles() {
            int deleteLogicsFilesInterval = DefaultMessageStore.this.getMessageStoreConfig().getDeleteConsumeQueueFilesInterval();

            long minOffset = DefaultMessageStore.this.commitLog.getMinOffset();
            if (minOffset > this.lastPhysicalMinOffset) {
                this.lastPhysicalMinOffset = minOffset;

                ConcurrentMap<String, ConcurrentMap<Integer, ConsumeQueueInterface>> tables = DefaultMessageStore.this.getConsumeQueueTable();

                for (ConcurrentMap<Integer, ConsumeQueueInterface> maps : tables.values()) {
                    for (ConsumeQueueInterface logic : maps.values()) {
                        int deleteCount = DefaultMessageStore.this.consumeQueueStore.deleteExpiredFile(logic, minOffset);
                        if (deleteCount > 0 && deleteLogicsFilesInterval > 0) {
                            try {
                                Thread.sleep(deleteLogicsFilesInterval);
                            } catch (InterruptedException ignored) {
                            }
                        }
                    }
                }

                DefaultMessageStore.this.indexService.deleteExpiredFile(minOffset);
            }
        }

        public String getServiceName() {
            return DefaultMessageStore.this.brokerConfig.getLoggerIdentifier() + CleanConsumeQueueService.class.getSimpleName();
        }
    }

    class CorrectLogicOffsetService {
        private long lastForceCorrectTime = -1L;

        public void run() {
            try {
                this.correctLogicMinOffset();
            } catch (Throwable e) {
                LOGGER.warn(this.getServiceName() + " service has exception. ", e);
            }
        }

        private boolean needCorrect(ConsumeQueueInterface logic, long minPhyOffset, long lastForeCorrectTimeCurRun) {
            if (logic == null) {
                return false;
            }
            // If first exist and not available, it means first file may destroy failed, delete it.
            if (DefaultMessageStore.this.consumeQueueStore.isFirstFileExist(logic) && !DefaultMessageStore.this.consumeQueueStore.isFirstFileAvailable(logic)) {
                LOGGER.error("CorrectLogicOffsetService.needCorrect. first file not available, trigger correct." +
                                " topic:{}, queue:{}, maxPhyOffset in queue:{}, minPhyOffset " +
                                "in commit log:{}, minOffset in queue:{}, maxOffset in queue:{}, cqType:{}"
                        , logic.getTopic(), logic.getQueueId(), logic.getMaxPhysicOffset()
                        , minPhyOffset, logic.getMinOffsetInQueue(), logic.getMaxOffsetInQueue(), logic.getCQType());
                return true;
            }

            // logic.getMaxPhysicOffset() or minPhyOffset = -1
            // means there is no message in current queue, so no need to correct.
            if (logic.getMaxPhysicOffset() == -1 || minPhyOffset == -1) {
                return false;
            }

            if (logic.getMaxPhysicOffset() < minPhyOffset) {
                if (logic.getMinOffsetInQueue() < logic.getMaxOffsetInQueue()) {
                    LOGGER.error("CorrectLogicOffsetService.needCorrect. logic max phy offset: {} is less than min phy offset: {}, " +
                                    "but min offset: {} is less than max offset: {}. topic:{}, queue:{}, cqType:{}."
                            , logic.getMaxPhysicOffset(), minPhyOffset, logic.getMinOffsetInQueue()
                            , logic.getMaxOffsetInQueue(), logic.getTopic(), logic.getQueueId(), logic.getCQType());
                    return true;
                } else if (logic.getMinOffsetInQueue() == logic.getMaxOffsetInQueue()) {
                    return false;
                } else {
                    LOGGER.error("CorrectLogicOffsetService.needCorrect. It should not happen, logic max phy offset: {} is less than min phy offset: {}," +
                                    " but min offset: {} is larger than max offset: {}. topic:{}, queue:{}, cqType:{}"
                            , logic.getMaxPhysicOffset(), minPhyOffset, logic.getMinOffsetInQueue()
                            , logic.getMaxOffsetInQueue(), logic.getTopic(), logic.getQueueId(), logic.getCQType());
                    return false;
                }
            }
            //the logic.getMaxPhysicOffset() >= minPhyOffset
            int forceCorrectInterval = DefaultMessageStore.this.getMessageStoreConfig().getCorrectLogicMinOffsetForceInterval();
            if ((System.currentTimeMillis() - lastForeCorrectTimeCurRun) > forceCorrectInterval) {
                lastForceCorrectTime = System.currentTimeMillis();
                CqUnit cqUnit = logic.getEarliestUnit();
                if (cqUnit == null) {
                    if (logic.getMinOffsetInQueue() == logic.getMaxOffsetInQueue()) {
                        return false;
                    } else {
                        LOGGER.error("CorrectLogicOffsetService.needCorrect. cqUnit is null, logic max phy offset: {} is greater than min phy offset: {}, " +
                                        "but min offset: {} is not equal to max offset: {}. topic:{}, queue:{}, cqType:{}."
                                , logic.getMaxPhysicOffset(), minPhyOffset, logic.getMinOffsetInQueue()
                                , logic.getMaxOffsetInQueue(), logic.getTopic(), logic.getQueueId(), logic.getCQType());
                        return true;
                    }
                }

                if (cqUnit.getPos() < minPhyOffset) {
                    LOGGER.error("CorrectLogicOffsetService.needCorrect. logic max phy offset: {} is greater than min phy offset: {}, " +
                                    "but minPhyPos in cq is: {}. min offset in queue: {}, max offset in queue: {}, topic:{}, queue:{}, cqType:{}."
                            , logic.getMaxPhysicOffset(), minPhyOffset, cqUnit.getPos(), logic.getMinOffsetInQueue()
                            , logic.getMaxOffsetInQueue(), logic.getTopic(), logic.getQueueId(), logic.getCQType());
                    return true;
                }

                if (cqUnit.getPos() >= minPhyOffset) {

                    // Normal case, do not need correct.
                    return false;
                }
            }

            return false;
        }

        private void correctLogicMinOffset() {

            long lastForeCorrectTimeCurRun = lastForceCorrectTime;
            long minPhyOffset = getMinPhyOffset();
            ConcurrentMap<String, ConcurrentMap<Integer, ConsumeQueueInterface>> tables = DefaultMessageStore.this.getConsumeQueueTable();
            for (ConcurrentMap<Integer, ConsumeQueueInterface> maps : tables.values()) {
                for (ConsumeQueueInterface logic : maps.values()) {
                    if (Objects.equals(CQType.SimpleCQ, logic.getCQType())) {
                        // cq is not supported for now.
                        continue;
                    }
                    if (needCorrect(logic, minPhyOffset, lastForeCorrectTimeCurRun)) {
                        doCorrect(logic, minPhyOffset);
                    }
                }
            }
        }

        private void doCorrect(ConsumeQueueInterface logic, long minPhyOffset) {
            DefaultMessageStore.this.consumeQueueStore.deleteExpiredFile(logic, minPhyOffset);
            int sleepIntervalWhenCorrectMinOffset = DefaultMessageStore.this.getMessageStoreConfig().getCorrectLogicMinOffsetSleepInterval();
            if (sleepIntervalWhenCorrectMinOffset > 0) {
                try {
                    Thread.sleep(sleepIntervalWhenCorrectMinOffset);
                } catch (InterruptedException ignored) {
                }
            }
        }

        public String getServiceName() {
            if (brokerConfig.isInBrokerContainer()) {
                return brokerConfig.getLoggerIdentifier() + CorrectLogicOffsetService.class.getSimpleName();
            }
            return CorrectLogicOffsetService.class.getSimpleName();
        }
    }

    class FlushConsumeQueueService extends ServiceThread {
        private static final int RETRY_TIMES_OVER = 3;
        private long lastFlushTimestamp = 0;

        private void doFlush(int retryTimes) {
            int flushConsumeQueueLeastPages = DefaultMessageStore.this.getMessageStoreConfig().getFlushConsumeQueueLeastPages();

            if (retryTimes == RETRY_TIMES_OVER) {
                flushConsumeQueueLeastPages = 0;
            }

            long logicsMsgTimestamp = 0;

            int flushConsumeQueueThoroughInterval = DefaultMessageStore.this.getMessageStoreConfig().getFlushConsumeQueueThoroughInterval();
            long currentTimeMillis = System.currentTimeMillis();
            if (currentTimeMillis >= (this.lastFlushTimestamp + flushConsumeQueueThoroughInterval)) {
                this.lastFlushTimestamp = currentTimeMillis;
                flushConsumeQueueLeastPages = 0;
                logicsMsgTimestamp = DefaultMessageStore.this.getStoreCheckpoint().getLogicsMsgTimestamp();
            }

            ConcurrentMap<String, ConcurrentMap<Integer, ConsumeQueueInterface>> tables = DefaultMessageStore.this.getConsumeQueueTable();

            for (ConcurrentMap<Integer, ConsumeQueueInterface> maps : tables.values()) {
                for (ConsumeQueueInterface cq : maps.values()) {
                    boolean result = false;
                    for (int i = 0; i < retryTimes && !result; i++) {
                        result = DefaultMessageStore.this.consumeQueueStore.flush(cq, flushConsumeQueueLeastPages);
                    }
                }
            }

            if (0 == flushConsumeQueueLeastPages) {
                if (logicsMsgTimestamp > 0) {
                    DefaultMessageStore.this.getStoreCheckpoint().setLogicsMsgTimestamp(logicsMsgTimestamp);
                }
                DefaultMessageStore.this.getStoreCheckpoint().flush();
            }
        }

        @Override
        public void run() {
            DefaultMessageStore.LOGGER.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    int interval = DefaultMessageStore.this.getMessageStoreConfig().getFlushIntervalConsumeQueue();
                    this.waitForRunning(interval);
                    this.doFlush(1);
                } catch (Exception e) {
                    DefaultMessageStore.LOGGER.warn(this.getServiceName() + " service has exception. ", e);
                }
            }

            this.doFlush(RETRY_TIMES_OVER);

            DefaultMessageStore.LOGGER.info(this.getServiceName() + " service end");
        }

        @Override
        public String getServiceName() {
            if (DefaultMessageStore.this.brokerConfig.isInBrokerContainer()) {
                return DefaultMessageStore.this.getBrokerIdentity().getLoggerIdentifier() + FlushConsumeQueueService.class.getSimpleName();
            }
            return FlushConsumeQueueService.class.getSimpleName();
        }

        @Override
        public long getJoinTime() {
            return 1000 * 60;
        }
    }

    /**
     * reput 转发线程（负责 Commitlog 转发到 Consumequeue、Index文件）。
     * <p>
     * Broker服务器在启动时会启动ReputMessageService线程，并初始
     * 化一个非常关键的参数reputFromOffset，该参数的含义是
     * ReputMessageService从哪个物理偏移量开始转发消息给ConsumeQueue
     * 和Index文件。如果允许重复转发，将reputFromOffset设置为
     * CommitLog文件的提交指针。如果不允许重复转发，将
     * reputFromOffset设置为CommitLog文件的内存中最大偏移量
     */
    class ReputMessageService extends ServiceThread {

        private volatile long reputFromOffset = 0;

        public long getReputFromOffset() {
            return reputFromOffset;
        }

        public void setReputFromOffset(long reputFromOffset) {
            this.reputFromOffset = reputFromOffset;
        }

        @Override
        public void shutdown() {
            for (int i = 0; i < 50 && this.isCommitLogAvailable(); i++) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException ignored) {
                }
            }

            if (this.isCommitLogAvailable()) {
                LOGGER.warn("shutdown ReputMessageService, but CommitLog have not finish to be dispatched, CommitLog max" +
                                " offset={}, reputFromOffset={}", DefaultMessageStore.this.commitLog.getMaxOffset(),
                        this.reputFromOffset);
            }

            super.shutdown();
        }

        public long behind() {
            return DefaultMessageStore.this.getConfirmOffset() - this.reputFromOffset;
        }

        private boolean isCommitLogAvailable() {
            if (DefaultMessageStore.this.getMessageStoreConfig().isDuplicationEnable()) {
                return this.reputFromOffset <= DefaultMessageStore.this.commitLog.getConfirmOffset();
            }
            if (DefaultMessageStore.this.getBrokerConfig().isEnableControllerMode()) {
                return this.reputFromOffset < ((AutoSwitchHAService) DefaultMessageStore.this.haService).getConfirmOffset();
            }
            return this.reputFromOffset < DefaultMessageStore.this.commitLog.getMaxOffset();
        }

        /**
         * 返回reputFromOffset偏移量开始的全部有效数据
         * （CommitLog文件）。然后循环读取每一条消息
         */
        private void doReput() {
            if (this.reputFromOffset < DefaultMessageStore.this.commitLog.getMinOffset()) {
                LOGGER.warn("The reputFromOffset={} is smaller than minPyOffset={}, this usually indicate that the dispatch behind too much and the commitlog has expired.",
                        this.reputFromOffset, DefaultMessageStore.this.commitLog.getMinOffset());
                this.reputFromOffset = DefaultMessageStore.this.commitLog.getMinOffset();
            }
            for (boolean doNext = true; this.isCommitLogAvailable() && doNext; ) {
                //从result返回的ByteBuffer中循环读取消息，一次读取
                //一条，创建Dispatch Request对象。DispatchRequest类图如图4-20所
                //示，如果消息长度大于0，则调用doDispatch()方法。最终将分别调用
                //CommitLogDispatcherBuildConsumeQueue（构建消息消费队列）、
                //CommitLogDispatcherBuildIndex（构建索引文件）。
                SelectMappedBufferResult result = DefaultMessageStore.this.commitLog.getData(reputFromOffset);
                if (result != null) {
                    try {
                        this.reputFromOffset = result.getStartOffset();

                        for (int readSize = 0; readSize < result.getSize() && reputFromOffset < DefaultMessageStore.this.getConfirmOffset() && doNext; ) {
                            DispatchRequest dispatchRequest =
                                    DefaultMessageStore.this.commitLog.checkMessageAndReturnSize(result.getByteBuffer(), false, false, false);
                            int size = dispatchRequest.getBufferSize() == -1 ? dispatchRequest.getMsgSize() : dispatchRequest.getBufferSize();

                            if (reputFromOffset + size > DefaultMessageStore.this.getConfirmOffset()) {
                                doNext = false;
                                break;
                            }

                            if (dispatchRequest.isSuccess()) {
                                if (size > 0) {


                                    DefaultMessageStore.this.doDispatch(dispatchRequest);
                                    //短轮询：longPollingEnable=false，第一次未拉取到消息后等待
                                    //shortPollingTimeMills时间后再试。shortPollingTimeMills默认为
                                    //1s。

                                    //长轮询：longPollingEnable=true，以消费者端设置的挂起超时
                                    //时间为依据，受Default MQPullConsumer的
                                    //brokerSuspendMaxTimeMillis控制，默认20s，长轮询有两个线程来相
                                    //互实现。PullRequestHoldService默认每隔5s重试一次。
                                    //DefaultMessageStore#ReputMessageService方法在每当有消息到达
                                    //后，转发消息，然后调用PullRequestHoldService线程中的拉取任
                                    //务，尝试拉取，每处理一次，线程休眠1ms，继续下一次检查。
                                    if (DefaultMessageStore.this.brokerConfig.isLongPollingEnable()
                                            && DefaultMessageStore.this.messageArrivingListener != null) {
                                        DefaultMessageStore.this.messageArrivingListener.arriving(dispatchRequest.getTopic(),
                                                dispatchRequest.getQueueId(), dispatchRequest.getConsumeQueueOffset() + 1,
                                                dispatchRequest.getTagsCode(), dispatchRequest.getStoreTimestamp(),
                                                dispatchRequest.getBitMap(), dispatchRequest.getPropertiesMap());
                                        notifyMessageArrive4MultiQueue(dispatchRequest);
                                    }

                                    this.reputFromOffset += size;
                                    readSize += size;
                                    if (!DefaultMessageStore.this.getMessageStoreConfig().isDuplicationEnable() &&
                                            DefaultMessageStore.this.getMessageStoreConfig().getBrokerRole() == BrokerRole.SLAVE) {
                                        DefaultMessageStore.this.storeStatsService
                                                .getSinglePutMessageTopicTimesTotal(dispatchRequest.getTopic()).add(1);
                                        DefaultMessageStore.this.storeStatsService
                                                .getSinglePutMessageTopicSizeTotal(dispatchRequest.getTopic())
                                                .add(dispatchRequest.getMsgSize());
                                    }
                                } else if (size == 0) {
                                    this.reputFromOffset = DefaultMessageStore.this.commitLog.rollNextFile(this.reputFromOffset);
                                    readSize = result.getSize();
                                }
                            } else if (!dispatchRequest.isSuccess()) {

                                if (size > 0) {
                                    LOGGER.error("[BUG]read total count not equals msg total size. reputFromOffset={}", reputFromOffset);
                                    this.reputFromOffset += size;
                                } else {
                                    doNext = false;
                                    // If user open the dledger pattern or the broker is master node,
                                    // it will not ignore the exception and fix the reputFromOffset variable
                                    if (DefaultMessageStore.this.getMessageStoreConfig().isEnableDLegerCommitLog() ||
                                            DefaultMessageStore.this.brokerConfig.getBrokerId() == MixAll.MASTER_ID) {
                                        LOGGER.error("[BUG]dispatch message to consume queue error, COMMITLOG OFFSET: {}",
                                                this.reputFromOffset);
                                        this.reputFromOffset += result.getSize() - readSize;
                                    }
                                }
                            }
                        }
                    } finally {
                        result.release();
                    }
                } else {
                    doNext = false;
                }
            }
        }

        private void notifyMessageArrive4MultiQueue(DispatchRequest dispatchRequest) {
            Map<String, String> prop = dispatchRequest.getPropertiesMap();
            if (prop == null) {
                return;
            }
            String multiDispatchQueue = prop.get(MessageConst.PROPERTY_INNER_MULTI_DISPATCH);
            String multiQueueOffset = prop.get(MessageConst.PROPERTY_INNER_MULTI_QUEUE_OFFSET);
            if (StringUtils.isBlank(multiDispatchQueue) || StringUtils.isBlank(multiQueueOffset)) {
                return;
            }
            String[] queues = multiDispatchQueue.split(MixAll.MULTI_DISPATCH_QUEUE_SPLITTER);
            String[] queueOffsets = multiQueueOffset.split(MixAll.MULTI_DISPATCH_QUEUE_SPLITTER);
            if (queues.length != queueOffsets.length) {
                return;
            }
            for (int i = 0; i < queues.length; i++) {
                String queueName = queues[i];
                long queueOffset = Long.parseLong(queueOffsets[i]);
                int queueId = dispatchRequest.getQueueId();
                if (DefaultMessageStore.this.getMessageStoreConfig().isEnableLmq() && MixAll.isLmq(queueName)) {
                    queueId = 0;
                }
                DefaultMessageStore.this.messageArrivingListener.arriving(
                        queueName, queueId, queueOffset + 1, dispatchRequest.getTagsCode(),
                        dispatchRequest.getStoreTimestamp(), dispatchRequest.getBitMap(), dispatchRequest.getPropertiesMap());
            }
        }

        @Override
        public void run() {
            //ReputMessageService线程每执行一次任务推送，休息1ms后继续
            //尝试推送消息到Consume Queue和Index文件中，消息消费转发由
            //doReput()方法实现
            DefaultMessageStore.LOGGER.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    Thread.sleep(1);
                    this.doReput();
                } catch (Exception e) {
                    DefaultMessageStore.LOGGER.warn(this.getServiceName() + " service has exception. ", e);
                }
            }

            DefaultMessageStore.LOGGER.info(this.getServiceName() + " service end");
        }

        @Override
        public String getServiceName() {
            if (DefaultMessageStore.this.getBrokerConfig().isInBrokerContainer()) {
                return DefaultMessageStore.this.getBrokerIdentity().getLoggerIdentifier() + ReputMessageService.class.getSimpleName();
            }
            return ReputMessageService.class.getSimpleName();
        }

    }

    @Override
    public HARuntimeInfo getHARuntimeInfo() {
        if (haService != null) {
            return this.haService.getRuntimeInfo(this.commitLog.getMaxOffset());
        } else {
            return null;
        }
    }

    public int getMaxDelayLevel() {
        return maxDelayLevel;
    }

    public long computeDeliverTimestamp(final int delayLevel, final long storeTimestamp) {
        Long time = this.delayLevelTable.get(delayLevel);
        if (time != null) {
            return time + storeTimestamp;
        }

        return storeTimestamp + 1000;
    }

    public List<PutMessageHook> getPutMessageHookList() {
        return putMessageHookList;
    }

    @Override
    public void setSendMessageBackHook(SendMessageBackHook sendMessageBackHook) {
        this.sendMessageBackHook = sendMessageBackHook;
    }

    @Override
    public SendMessageBackHook getSendMessageBackHook() {
        return sendMessageBackHook;
    }

    @Override
    public boolean isShutdown() {
        return shutdown;
    }
}
