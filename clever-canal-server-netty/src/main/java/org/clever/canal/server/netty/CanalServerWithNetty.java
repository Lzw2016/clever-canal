package org.clever.canal.server.netty;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.clever.canal.common.AbstractCanalLifeCycle;
import org.clever.canal.server.CanalServer;
import org.clever.canal.server.embedded.CanalServerWithEmbedded;

import java.util.concurrent.TimeUnit;

/**
 * 基于netty网络服务的server实现
 */
@Slf4j
public class CanalServerWithNetty extends AbstractCanalLifeCycle implements CanalServer {
    /**
     * 单例
     */
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
            @Override
            protected void initChannel(SocketChannel ch) throws Exception {
                ChannelPipeline pipeline = ch.pipeline();

                // 用于decode前解决半包和粘包问题（利用包头中的包含数组长度来识别半包粘包）
                pipeline.addLast("frameDecoder", new ProtobufVarint32FrameDecoder());
                //配置 protobuf 解码处理器，消息接收到了就会自动解码，ProtobufDecoder是netty自带的，Message是自己定义的Protobuf类
                // pipeline.addLast("protobufDecoder", new ProtobufDecoder(Message.getDefaultInstance()));
                // 用于在序列化的字节数组前加上一个简单的包头，只包含序列化的字节长度
                pipeline.addLast("frameEncoder", new ProtobufVarint32LengthFieldPrepender());
                // 配置 protobuf 编码器，发送的消息会先经过编码
                pipeline.addLast("ProtobufEncoder", new ProtobufEncoder());


//                pipeline.addLast("FixedHeaderFrameDecoder", new FixedHeaderFrameDecoder());
//                // 支持维护child socket通道
//                pipeline.addLast("HandshakeInitializationHandler", new HandshakeInitializationHandler(childGroups));
                // 客户端授权处理
//                pipeline.addLast("ClientAuthenticationHandler", new ClientAuthenticationHandler(embeddedServer));
            }
        });
        // 优化网络配置
        bootstrap.childOption(ChannelOption.SO_KEEPALIVE, true);
        bootstrap.childOption(ChannelOption.TCP_NODELAY, true);
        // 启动服务
        if (StringUtils.isNotEmpty(bindIp)) {
            channelFuture = bootstrap.bind(bindIp, port);
            log.info("CanalServerWithNetty start for {}:{}", bindIp, port);
        } else {
            bootstrap.bind(port);
            log.info("CanalServerWithNetty start for {}:{}", bindIp, port);
        }

        // 构造对应的pipeline
//        bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
//            public ChannelPipeline getPipeline() throws Exception {
//                ChannelPipeline pipelines = Channels.pipeline();
//
//                SessionHandler sessionHandler = new SessionHandler(embeddedServer);
//                pipelines.addLast(SessionHandler.class.getName(), sessionHandler);
//                return pipelines;
//            }
//        });
    }

    @Override
    public void stop() {
        super.stop();
        if (channelFuture != null) {
            channelFuture.channel().close().awaitUninterruptibly(1000);
        }
        if (bossGroup != null) {
            bossGroup.shutdownGracefully(1L, 1L, TimeUnit.SECONDS);
        }
        if (workerGroup != null) {
            workerGroup.shutdownGracefully(1L, 1L, TimeUnit.SECONDS);
        }
        channelFuture = null;
        bossGroup = null;
        workerGroup = null;
        bootstrap = null;
        if (embeddedServer.isStart()) {
            embeddedServer.stop();
        }
    }
}
