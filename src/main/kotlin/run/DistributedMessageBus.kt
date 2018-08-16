package run

import io.netty.bootstrap.Bootstrap
import io.netty.bootstrap.ServerBootstrap
import io.netty.buffer.*
import io.netty.channel.*
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.pool.AbstractChannelPoolMap
import io.netty.channel.pool.ChannelPoolHandler
import io.netty.channel.pool.FixedChannelPool
import io.netty.channel.pool.SimpleChannelPool
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.channel.socket.nio.NioSocketChannel
import io.netty.handler.codec.LineBasedFrameDecoder
import io.netty.util.ReferenceCountUtil
import kotlinx.coroutines.experimental.*
import java.net.BindException
import java.net.InetSocketAddress

class DistributedMessageBus(
    val portRange: Iterable<Int>,
    val activeNodes: (String) -> List<Registration>
) {
    val listeners = mutableListOf<suspend (Message) -> Unit>()

    private val serverBootstrap = ServerBootstrap()
        .group(NioEventLoopGroup(1), NioEventLoopGroup())
        .channel(NioServerSocketChannel::class.java)
        .childHandler(object : ChannelInitializer<SocketChannel>() {
            override fun initChannel(ch: SocketChannel) {
                val pipeline = ch.pipeline()
                pipeline.addLast(LineBasedFrameDecoder(512 * 1024))
                pipeline.addLast(ObjectMapperCodec())
                pipeline.addLast(object : SimpleChannelInboundHandler<Message>() {
                    override fun channelRead0(ctx: ChannelHandlerContext, msg: Message) {
                        launch {
                            listeners.forEach { it(msg) }
                        }
                    }
                })
            }
        })


    val port = allocatePort()

    private val clientBootstrap = Bootstrap()
        .group(NioEventLoopGroup())
        .channel(NioSocketChannel::class.java)

    private val NEW_LINE = Unpooled.copiedBuffer("\n", Charsets.US_ASCII)

    private var poolMap = object : AbstractChannelPoolMap<InetSocketAddress, SimpleChannelPool>() {
        override fun newPool(key: InetSocketAddress): SimpleChannelPool {
            return FixedChannelPool(
                clientBootstrap.clone().remoteAddress(key),
                object : ChannelPoolHandler {
                    override fun channelReleased(ch: Channel) {
                    }

                    override fun channelAcquired(ch: Channel) {
                    }


                    override fun channelCreated(ch: Channel) {
                        val pipeline = ch.pipeline()
                        pipeline.addLast(object : ChannelOutboundHandlerAdapter() {
                            override fun write(
                                ctx: ChannelHandlerContext,
                                msg: Any,
                                promise: ChannelPromise
                            ) {
                                ReferenceCountUtil.retain(msg)

                                launch {
                                    try {
                                        ctx.writeAndFlush(msg)
                                            .waitComplete()

                                        ctx.writeAndFlush(NEW_LINE.retain())
                                            .waitComplete()

                                        promise.setSuccess()
                                    } catch (ex: Exception) {
                                        promise.setFailure(ex)
                                    } finally {
                                        ReferenceCountUtil.release(msg)
                                    }
                                }
                            }
                        })
                        pipeline.addLast(ObjectMapperCodec())
                    }
                },
                10
            )
        }
    }

    private fun allocatePort(): Int {
        for (port in portRange) {
            try {
                val future = serverBootstrap.bind(port).sync()
                if (future.isSuccess) {
                    return port
                }
            } catch (ex: BindException) {
                // skip
            }
        }
        throw RuntimeException("failed to allocate port")
    }


    suspend fun broadcast(org: String, vararg message: Message) {
        activeNodes(org).map { node ->
            val addr = InetSocketAddress(node.host, node.port)
            async {
                val pool = poolMap[addr]
                val channel = pool
                    .acquire()
                    .waitComplete()

                try {
                    for (msg in message) {
                        channel.writeAndFlush(msg)
                            .waitComplete()
                    }
                } catch (ex: Exception) {
                    // skip
                } finally {
                    pool.release(channel)
                        .waitComplete()
                }

            }
        }.forEach { it.await() }

    }

    fun close() {
        serverBootstrap.config().childGroup().shutdownGracefully()
        serverBootstrap.config().group().shutdownGracefully()
        clientBootstrap.config().group().shutdownGracefully()
    }
}