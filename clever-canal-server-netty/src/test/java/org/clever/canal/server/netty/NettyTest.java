package org.clever.canal.server.netty;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.DelimiterBasedFrameDecoder;
import io.netty.handler.codec.Delimiters;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import io.netty.util.concurrent.GlobalEventExecutor;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

/**
 * 作者：lizw <br/>
 * 创建时间：2019/11/07 09:01 <br/>
 */
@Slf4j
public class NettyTest {
    private static final String host = "127.0.0.1";
    private static final int port = 8000;

    @Test
    public void server() throws InterruptedException {
        EventLoopGroup bossGroup = new NioEventLoopGroup();
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        ChannelGroup channels = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
        try {
            ServerBootstrap server = new ServerBootstrap();
            server.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            log.info("[Server] {} 连接上", ch.remoteAddress());

                            ChannelPipeline pipeline = ch.pipeline();
                            pipeline.addLast("framer", new DelimiterBasedFrameDecoder(8192, Delimiters.lineDelimiter()));
                            pipeline.addLast("decoder", new StringDecoder());
                            pipeline.addLast("encoder", new StringEncoder());
                            pipeline.addLast("handler", new SimpleChannelInboundHandler<String>() {
                                @Override
                                public void handlerAdded(ChannelHandlerContext ctx) {
                                    Channel incoming = ctx.channel();
                                    for (Channel channel : channels) {
                                        channel.writeAndFlush("[SERVER] - " + incoming.remoteAddress() + " 加入\n");
                                        log.info("### 加入");
                                    }
                                    channels.add(ctx.channel());
                                }

                                @Override
                                public void handlerRemoved(ChannelHandlerContext ctx) {
                                    Channel incoming = ctx.channel();
                                    for (Channel channel : channels) {
                                        channel.writeAndFlush("[SERVER] - " + incoming.remoteAddress() + " 离开\n");
                                        log.info("### 离开");
                                    }
                                    channels.remove(ctx.channel());
                                }

                                @Override
                                protected void channelRead0(ChannelHandlerContext ctx, String s) {
                                    Channel incoming = ctx.channel();
                                    for (Channel channel : channels) {
                                        if (channel != incoming) {
                                            channel.writeAndFlush("[" + incoming.remoteAddress() + "]" + s + "\n");
                                        } else {
                                            channel.writeAndFlush("[you]" + s + "\n");
                                        }
                                    }
                                }

                                @Override
                                public void channelActive(ChannelHandlerContext ctx) {
                                    Channel incoming = ctx.channel();
                                    log.info("[Server] {} 上线", incoming.remoteAddress());
                                }

                                @Override
                                public void channelInactive(ChannelHandlerContext ctx) {
                                    Channel incoming = ctx.channel();
                                    log.info("[Server] {} 掉线", incoming.remoteAddress());
                                }

                                @Override
                                public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                                    Channel incoming = ctx.channel();
                                    log.info("[Server] {} 异常", incoming.remoteAddress(), cause);
                                    ctx.close();
                                }
                            });
                        }
                    })
                    .option(ChannelOption.SO_BACKLOG, 128)
                    .childOption(ChannelOption.SO_KEEPALIVE, true);
            // 绑定端口，开始接收进来的连接
            ChannelFuture channelFuture = server.bind(port).sync();
            log.info("Server Started!");
            // 等待服务器  socket 关闭
            // 在这个例子中，这不会发生，但你可以优雅地关闭你的服务器
            channelFuture.channel().closeFuture().sync();
        } finally {
            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();
            log.info("Server Shutdown!");
        }
    }

    @Test
    public void client() throws InterruptedException {
        EventLoopGroup group = new NioEventLoopGroup();
        try {
            Bootstrap client = new Bootstrap()
                    .group(group)
                    .channel(NioSocketChannel.class)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ChannelPipeline pipeline = ch.pipeline();
                            pipeline.addLast("framer", new DelimiterBasedFrameDecoder(8192, Delimiters.lineDelimiter()));
                            pipeline.addLast("decoder", new StringDecoder());
                            pipeline.addLast("encoder", new StringEncoder());
                            pipeline.addLast("handler", new SimpleChannelInboundHandler<String>() {
                                @Override
                                protected void channelRead0(ChannelHandlerContext ctx, String msg) {
                                    log.info(" --> {}", msg);
                                }
                            });
                        }
                    });
            Channel channel = client.connect(host, port).sync().channel();
            for (int i = 1; i <= 10; i++) {
                String msg = "abc !!! " + i + "\n";
                channel.writeAndFlush(msg);
                log.info("### {}", msg);
                Thread.sleep(100 * i);
            }
            channel.close();
        } finally {
            group.shutdownGracefully();
        }
    }

    @Test
    public void clients() throws InterruptedException {
        for (int i = 1; i <= 5; i++) {
            new Thread(() -> {
                try {
                    client();
                    Thread.sleep(300);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }).start();
        }
        Thread.sleep(1000 * 1000);
    }
}

