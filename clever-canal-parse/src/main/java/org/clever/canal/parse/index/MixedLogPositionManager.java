//package org.clever.canal.parse.index;
//
//import org.clever.canal.common.zookeeper.ZkClientx;
//import org.clever.canal.parse.exception.CanalParseException;
//import org.clever.canal.protocol.position.LogPosition;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.util.concurrent.ExecutorService;
//import java.util.concurrent.Executors;
// TODO lzw
//public class MixedLogPositionManager extends AbstractLogPositionManager {
//
//    private final Logger logger = LoggerFactory.getLogger(MixedLogPositionManager.class);
//
//    private final MemoryLogPositionManager memoryLogPositionManager;
//    private final ZooKeeperLogPositionManager zooKeeperLogPositionManager;
//
//    private final ExecutorService executor;
//
//    public MixedLogPositionManager(ZkClientx zkClient) {
//        if (zkClient == null) {
//            throw new NullPointerException("null zkClient");
//        }
//
//        this.memoryLogPositionManager = new MemoryLogPositionManager();
//        this.zooKeeperLogPositionManager = new ZooKeeperLogPositionManager(zkClient);
//
//        this.executor = Executors.newFixedThreadPool(1);
//    }
//
//    @Override
//    public void start() {
//        super.start();
//
//        if (!memoryLogPositionManager.isStart()) {
//            memoryLogPositionManager.start();
//        }
//
//        if (!zooKeeperLogPositionManager.isStart()) {
//            zooKeeperLogPositionManager.start();
//        }
//    }
//
//    @Override
//    public void stop() {
//        super.stop();
//
//        executor.shutdown();
//        zooKeeperLogPositionManager.stop();
//        memoryLogPositionManager.stop();
//    }
//
//    @Override
//    public LogPosition getLatestIndexBy(String destination) {
//        LogPosition logPosition = memoryLogPositionManager.getLatestIndexBy(destination);
//        if (logPosition != null) {
//            return logPosition;
//        }
//        logPosition = zooKeeperLogPositionManager.getLatestIndexBy(destination);
//        // 这里保持和重构前的逻辑一致,重新添加到Memory中
//        if (logPosition != null) {
//            memoryLogPositionManager.persistLogPosition(destination, logPosition);
//        }
//        return logPosition;
//    }
//
//    @Override
//    public void persistLogPosition(final String destination, final LogPosition logPosition) throws CanalParseException {
//        memoryLogPositionManager.persistLogPosition(destination, logPosition);
//        executor.submit(new Runnable() {
//
//            public void run() {
//                try {
//                    zooKeeperLogPositionManager.persistLogPosition(destination, logPosition);
//                } catch (Exception e) {
//                    logger.error("ERROR # persist to zookeeper has an error", e);
//                }
//            }
//        });
//    }
//}
