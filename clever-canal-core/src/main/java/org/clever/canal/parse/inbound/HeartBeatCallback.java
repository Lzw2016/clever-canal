package org.clever.canal.parse.inbound;

/**
 * 提供mysql heartBeat心跳数据的callback机制
 */
@SuppressWarnings("unused")
public interface HeartBeatCallback {
    /**
     * 心跳发送成功
     */
    void onSuccess(long costTime);

    /**
     * 心跳发送失败
     */
    void onFailed(Throwable e);
}
