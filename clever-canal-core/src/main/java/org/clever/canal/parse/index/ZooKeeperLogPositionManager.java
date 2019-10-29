//package org.clever.canal.parse.index;
//
//import org.I0Itec.zkclient.exception.ZkNoNodeException;
//import org.clever.canal.common.utils.JsonUtils;
//import org.clever.canal.common.zookeeper.ZkClientx;
//import org.clever.canal.common.zookeeper.ZookeeperPathUtils;
//import org.clever.canal.parse.exception.CanalParseException;
//import org.clever.canal.protocol.position.LogPosition;
// TODO lzw
//public class ZooKeeperLogPositionManager extends AbstractLogPositionManager {
//
//    private final ZkClientx zkClientx;
//
//    public ZooKeeperLogPositionManager(ZkClientx zkClient) {
//        if (zkClient == null) {
//            throw new NullPointerException("null zkClient");
//        }
//
//        this.zkClientx = zkClient;
//    }
//
//    @Override
//    public LogPosition getLatestIndexBy(String destination) {
//        String path = ZookeeperPathUtils.getParsePath(destination);
//        byte[] data = zkClientx.readData(path, true);
//        if (data == null || data.length == 0) {
//            return null;
//        }
//
//        return JsonUtils.unmarshalFromByte(data, LogPosition.class);
//    }
//
//    @Override
//    public void persistLogPosition(String destination, LogPosition logPosition) throws CanalParseException {
//        String path = ZookeeperPathUtils.getParsePath(destination);
//        byte[] data = JsonUtils.marshalToByte(logPosition);
//        try {
//            zkClientx.writeData(path, data);
//        } catch (ZkNoNodeException e) {
//            zkClientx.createPersistent(path, data, true);
//        }
//    }
//
//}
