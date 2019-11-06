package org.clever.canal.server.netty;

import org.apache.commons.lang3.StringUtils;
import org.clever.canal.common.AbstractCanalLifeCycle;
import org.clever.canal.server.CanalServer;
import org.clever.canal.server.embedded.CanalServerWithEmbedded;
import org.clever.canal.server.netty.handler.ClientAuthenticationHandler;
import org.clever.canal.server.netty.handler.FixedHeaderFrameDecoder;
import org.clever.canal.server.netty.handler.HandshakeInitializationHandler;
import org.clever.canal.server.netty.handler.SessionHandler;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

/**
 * 基于netty网络服务的server实现
 */
public class CanalServerWithNetty extends AbstractCanalLifeCycle implements CanalServer {
    public static final CanalServerWithNetty Instance = new CanalServerWithNetty();
    /**
     * 嵌入式server
     */
    private CanalServerWithEmbedded embeddedServer;
    private String ip;
    private int port;
    private Channel serverChannel = null;
    private ServerBootstrap bootstrap = null;
    private ChannelGroup childGroups = null; // socket channel
    // container, used to
    // close sockets
    // explicitly.

    private static class SingletonHolder {
        private static final CanalServerWithNetty CANAL_SERVER_WITH_NETTY = new CanalServerWithNetty();
    }

    private CanalServerWithNetty() {
        this.embeddedServer = CanalServerWithEmbedded.Instance;
        this.childGroups = new DefaultChannelGroup();
    }


    public void start() {
        super.start();
        if (!embeddedServer.isStart()) {
            embeddedServer.start();
        }

        this.bootstrap = new ServerBootstrap(new NioServerSocketChannelFactory(Executors.newCachedThreadPool(), Executors.newCachedThreadPool()));
        /*
         * enable keep-alive mechanism, handle abnormal network connection
         * scenarios on OS level. the threshold parameters are depended on OS.
         * e.g. On Linux: net.ipv4.tcp_keepalive_time = 300
         * net.ipv4.tcp_keepalive_probes = 2 net.ipv4.tcp_keepalive_intvl = 30
         */
        bootstrap.setOption("child.keepAlive", true);
        /*
         * optional parameter.
         */
        bootstrap.setOption("child.tcpNoDelay", true);

        // 构造对应的pipeline
        bootstrap.setPipelineFactory(new ChannelPipelineFactory() {

            public ChannelPipeline getPipeline() throws Exception {
                ChannelPipeline pipelines = Channels.pipeline();
                pipelines.addLast(FixedHeaderFrameDecoder.class.getName(), new FixedHeaderFrameDecoder());
                // support to maintain child socket channel.
                pipelines.addLast(HandshakeInitializationHandler.class.getName(),
                        new HandshakeInitializationHandler(childGroups));
                pipelines.addLast(ClientAuthenticationHandler.class.getName(),
                        new ClientAuthenticationHandler(embeddedServer));

                SessionHandler sessionHandler = new SessionHandler(embeddedServer);
                pipelines.addLast(SessionHandler.class.getName(), sessionHandler);
                return pipelines;
            }
        });

        // 启动
        if (StringUtils.isNotEmpty(ip)) {
            this.serverChannel = bootstrap.bind(new InetSocketAddress(this.ip, this.port));
        } else {
            this.serverChannel = bootstrap.bind(new InetSocketAddress(this.port));
        }
    }

    public void stop() {
        super.stop();

        if (this.serverChannel != null) {
            this.serverChannel.close().awaitUninterruptibly(1000);
        }

        // close sockets explicitly to reduce socket channel hung in complicated
        // network environment.
        if (this.childGroups != null) {
            this.childGroups.close().awaitUninterruptibly(5000);
        }

        if (this.bootstrap != null) {
            this.bootstrap.releaseExternalResources();
        }

        if (embeddedServer.isStart()) {
            embeddedServer.stop();
        }
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public void setEmbeddedServer(CanalServerWithEmbedded embeddedServer) {
        this.embeddedServer = embeddedServer;
    }

}