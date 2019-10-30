package org.clever.canal.server;

import org.clever.canal.protocol.ClientIdentity;
import org.clever.canal.protocol.Message;
import org.clever.canal.server.exception.CanalServerException;

import java.util.concurrent.TimeUnit;

@SuppressWarnings("unused")
public interface CanalService {

    void subscribe(ClientIdentity clientIdentity) throws CanalServerException;

    void unsubscribe(ClientIdentity clientIdentity) throws CanalServerException;

    Message get(ClientIdentity clientIdentity, int batchSize) throws CanalServerException;

    Message get(ClientIdentity clientIdentity, int batchSize, Long timeout, TimeUnit unit) throws CanalServerException;

    Message getWithoutAck(ClientIdentity clientIdentity, int batchSize) throws CanalServerException;

    Message getWithoutAck(ClientIdentity clientIdentity, int batchSize, Long timeout, TimeUnit unit) throws CanalServerException;

    void ack(ClientIdentity clientIdentity, long batchId) throws CanalServerException;

    void rollback(ClientIdentity clientIdentity) throws CanalServerException;

    void rollback(ClientIdentity clientIdentity, Long batchId) throws CanalServerException;
}
