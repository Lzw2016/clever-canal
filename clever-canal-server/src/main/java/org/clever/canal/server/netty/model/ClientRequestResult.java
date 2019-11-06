package org.clever.canal.server.netty.model;

import com.google.common.base.Preconditions;
import com.google.protobuf.GeneratedMessageV3;
import lombok.Getter;
import lombok.Setter;
import org.clever.canal.protocol.CanalPacket;

import java.io.Serializable;

/**
 * 作者：lizw <br/>
 * 创建时间：2019/11/06 15:38 <br/>
 */
@Getter
public class ClientRequestResult implements Serializable {
    /**
     *
     */
    private String destination;
    /**
     *
     */
    private CanalPacket.PacketType type;
    /**
     *
     */
    private GeneratedMessageV3 request;
    /**
     *
     */
    private int amount;
    /**
     *
     */
    private long latency;
    /**
     *
     */
    private short errorCode;
    /**
     *
     */
    private boolean empty;
    /**
     *
     */
    @Setter
    private Throwable channelError;

    private ClientRequestResult(Builder builder) {
        this.destination = Preconditions.checkNotNull(builder.destination);
        this.type = Preconditions.checkNotNull(builder.type);
        this.request = builder.request;
        this.amount = builder.amount;
        this.latency = builder.latency;
        this.errorCode = builder.errorCode;
        this.empty = builder.empty;
        this.channelError = builder.channelError;
    }

    @SuppressWarnings("unused")
    public static class Builder {
        private String destination;
        private CanalPacket.PacketType type;
        private GeneratedMessageV3 request;
        private int amount;
        private long latency;
        private short errorCode;
        private boolean empty;
        private Throwable channelError;

        public Builder destination(String destination) {
            this.destination = destination;
            return this;
        }

        public Builder type(CanalPacket.PacketType type) {
            this.type = type;
            return this;
        }

        public Builder request(GeneratedMessageV3 request) {
            this.request = request;
            return this;
        }

        public Builder amount(int amount) {
            this.amount = amount;
            return this;
        }

        public Builder latency(long latency) {
            this.latency = latency;
            return this;
        }

        public Builder errorCode(short errorCode) {
            this.errorCode = errorCode;
            return this;
        }

        public Builder empty(boolean empty) {
            this.empty = empty;
            return this;
        }

        public Builder channelError(Throwable channelError) {
            this.channelError = channelError;
            return this;
        }

        @SuppressWarnings("DuplicatedCode")
        public Builder fromPrototype(ClientRequestResult prototype) {
            destination = prototype.destination;
            type = prototype.type;
            request = prototype.request;
            amount = prototype.amount;
            latency = prototype.latency;
            errorCode = prototype.errorCode;
            empty = prototype.empty;
            channelError = prototype.channelError;
            return this;
        }

        public ClientRequestResult build() {
            return new ClientRequestResult(this);
        }
    }
}
