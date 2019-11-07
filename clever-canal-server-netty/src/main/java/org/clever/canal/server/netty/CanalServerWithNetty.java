package org.clever.canal.server.netty;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.clever.canal.common.AbstractCanalLifeCycle;
import org.clever.canal.protocol.CanalPacket;
import org.clever.canal.server.CanalServer;
import org.clever.canal.server.embedded.CanalServerWithEmbedded;
import org.clever.canal.server.netty.handler.ClientAuthenticationHandler;
import org.clever.canal.server.netty.handler.HandshakeHandler;
import org.clever.canal.server.netty.handler.SessionHandler;

import java.util.concurrent.TimeUnit;

/**
 * 基于netty网络服务的server实现
 */
@Slf4j
public class CanalServerWithNetty extends AbstractCanalLifeCycle implements CanalServer {
    /**
     * 单例
     */
    @SuppressWarnings("WeakerAccess")
    public static final CanalServerWithNetty Instance = new CanalServerWithNetty();
    /**
     * 嵌入式server
     */
    @Getter
    private CanalServerWithEmbedded embeddedServer;
    /**
     * bind的IP地址
     */
    @Getter
    @Setter
    private String bindIp;
    /**
     * 监听的端口号
     */
    @Getter
    @Setter
    private int port = 8000;

    // ================================================================================================= netty相关
    private ServerBootstrap bootstrap;
    /**
     * boss线程池
     */
    private EventLoopGroup bossGroup;
    /**
     * worker线程池
     */
    private EventLoopGroup workerGroup;
    /**
     *
     */
    private ChannelFuture channelFuture;

    private CanalServerWithNetty() {
        this.embeddedServer = CanalServerWithEmbedded.Instance;
    }

    @SuppressWarnings("unused")
    public CanalServerWithNetty(CanalServerWithEmbedded embeddedServer, String bindIp, int port) {
        this.embeddedServer = embeddedServer;
        this.bindIp = bindIp;
        this.port = port;
    }

    @Override
    public void start() {
        super.start();
        if (!embeddedServer.isStart()) {
            embeddedServer.start();
        }
        // 初始化 bootstrap
        bootstrap = new ServerBootstrap();
        bossGroup = new NioEventLoopGroup();
        workerGroup = new NioEventLoopGroup();
        // 设NIO线程池
        bootstrap.group(bossGroup, workerGroup);
        // 使用TCP
        bootstrap.channel(NioServerSocketChannel.class);
        // 设置处理逻辑Handler
        bootstrap.childHandler(new ChannelInitializer<SocketChannel>() {
            @SuppressWarnings("SpellCheckingInspection")
            @Override
            protected void initChannel(SocketChannel ch) {
                ChannelPipeline pipeline = ch.pipeline();
                // -------------------------- 解码和编码，应和客户端一致 (传输的协议 Protobuf) -------------------------- //
                // 用于decode前解决半包和粘包问题（利用包头中的包含数组长度来识别半包粘包）
                pipeline.addLast("ProtobufVarint32FrameDecoder", new ProtobufVarint32FrameDecoder());
                // 反序列化指定的 Protobuf 字节数组为 Protobuf 类型
                pipeline.addLast("ProtobufDecoder", new ProtobufDecoder(CanalPacket.Packet.getDefaultInstance()));
                // 用于在序列化的字节数组前加上一个简单的包头，只包含序列化的字节长度
                pipeline.addLast("ProtobufVarint32LengthFieldPrepender", new ProtobufVarint32LengthFieldPrepender());
                // 用于对 Protobuf 类型序列化
                pipeline.addLast("ProtobufEncoder", new ProtobufEncoder());
                // -------------------------- CanalServer业务逻辑 -------------------------- //
                // 连接握手处理
                pipeline.addLast("HandshakeHandler", new HandshakeHandler());
                // 客户端授权处理
                pipeline.addLast("ClientAuthenticationHandler", new ClientAuthenticationHandler(embeddedServer));
                // Canal 数据同步功能处理
                pipeline.addLast("SessionHandler", new SessionHandler(embeddedServer));
            }
        });
        // 优化网络配置
        bootstrap.childOption(ChannelOption.SO_KEEPALIVE, true);
        bootstrap.childOption(ChannelOption.TCP_NODELAY, true);
        // 启动服务
        if (StringUtils.isNotBlank(bindIp)) {
            channelFuture = bootstrap.bind(bindIp, port);
            log.info("[CanalServerWithNetty] start for {}:{}", bindIp, port);
        } else {
            bootstrap.bind(port);
            log.info("[CanalServerWithNetty] start for port:{}", port);
        }
    }

    @Override
    public void stop() {
        log.info("[CanalServerWithNetty] stopping...");
        super.stop();
        try {
            if (channelFuture != null) {
                channelFuture.channel().close().sync();
            }
        } catch (Throwable e) {
            if (bossGroup != null) {
                bossGroup.shutdownGracefully(1L, 3L, TimeUnit.SECONDS);
            }
            if (workerGroup != null) {
                workerGroup.shutdownGracefully(1L, 3L, TimeUnit.SECONDS);
            }
        }
        channelFuture = null;
        bossGroup = null;
        workerGroup = null;
        bootstrap = null;
        if (embeddedServer.isStart()) {
            embeddedServer.stop();
        }
        log.info("[CanalServerWithNetty] stopped!");
    }
}
