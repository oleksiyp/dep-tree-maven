package run

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import com.google.common.net.HostAndPort
import com.orbitz.consul.Consul
import com.orbitz.consul.model.catalog.ImmutableCatalogDeregistration
import com.orbitz.consul.model.catalog.ImmutableCatalogRegistration
import com.orbitz.consul.model.health.ImmutableService
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
import io.netty.handler.codec.ByteToMessageCodec
import io.netty.handler.codec.LineBasedFrameDecoder
import io.netty.util.ReferenceCountUtil
import io.netty.util.concurrent.Future
import kotlinx.coroutines.experimental.CompletableDeferred
import kotlinx.coroutines.experimental.async
import kotlinx.coroutines.experimental.launch
import kotlinx.coroutines.experimental.runBlocking
import kotlinx.coroutines.experimental.selects.select
import java.io.InputStream
import java.io.OutputStream
import java.net.BindException
import java.net.InetAddress
import java.net.InetSocketAddress
import kotlin.coroutines.experimental.suspendCoroutine

data class Message(val type: String, val msg: String)

class DistributedMessageBus(
    val clusterName: String,
    val orgs: List<String>,
    val portRange: Iterable<Int>,
    val consulHostAndPort: String = "localhost:8500",
    val host: String = InetAddress.getLocalHost().hostName
) {

    val serviceName = "$clusterName-" + orgs.sorted().joinToString("-")

    private val firstDiscoveryIterationDone = CompletableDeferred<Boolean>()

    private val consul = Consul.builder()
        .withHostAndPort(HostAndPort.fromString(consulHostAndPort))
        .build()

    private class ObjectMapperCodec : ByteToMessageCodec<Message>() {
        private val mapper = ObjectMapper()
            .registerKotlinModule()

        override fun encode(ctx: ChannelHandlerContext, msg: Message, out: ByteBuf) {
            ByteBufOutputStream(out).use {
                mapper.writeValue(it as OutputStream, msg)
            }
        }

        override fun decode(ctx: ChannelHandlerContext, input: ByteBuf, out: MutableList<Any>) {
            val inputStream = ByteBufInputStream(input) as InputStream
            val decodedMessage = mapper.readValue(inputStream, Message::class.java)
            out.add(decodedMessage)
        }

    }

    @Volatile
    private var activeNodes: List<Pair<String, Int>> = listOf()

    val listeners = mutableListOf<suspend (Message) -> Unit>()

    private val serverBootstrap = ServerBootstrap()
        .apply { group(NioEventLoopGroup(1), NioEventLoopGroup()) }
        .apply { channel(NioServerSocketChannel::class.java) }
        .apply {
            childHandler(object : ChannelInitializer<SocketChannel>() {
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
        }


    val port = allocatePort()

    private val serviceDiscoveryThread = Thread({
        selfRegister()
        discoverServices()
        while (!Thread.interrupted()) {
            selfRegister()
            discoverServices()
            firstDiscoveryIterationDone.complete(true)
            try {
                Thread.sleep(1000)
            } catch (ex: InterruptedException) {
                break
            }
        }
    }, "service-discovery")

    init {
        serviceDiscoveryThread.start()
        runBlocking {
            firstDiscoveryIterationDone.await()
        }
    }

    private val clientBootstrap = Bootstrap()
        .apply { group(NioEventLoopGroup()) }
        .apply { channel(NioSocketChannel::class.java) }

    private val NEW_LINE = Unpooled.copiedBuffer("\n", Charsets.US_ASCII)

    private var poolMap = object : AbstractChannelPoolMap<InetSocketAddress, SimpleChannelPool>() {
        override fun newPool(key: InetSocketAddress): SimpleChannelPool {
            return FixedChannelPool(clientBootstrap.clone().remoteAddress(key), object : ChannelPoolHandler {
                override fun channelReleased(ch: Channel) {
                    println("Released")
                }

                override fun channelAcquired(ch: Channel) {
                    println("Acquired")
                }


                override fun channelCreated(ch: Channel) {
                    println("Created")
                    val pipeline = ch.pipeline()
                    pipeline.addLast(object : ChannelOutboundHandlerAdapter() {
                        override fun write(ctx: ChannelHandlerContext, msg: Any, promise: ChannelPromise) {
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
            }, 10)
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


    private fun selfRegister() {
        try {
            consul.catalogClient().register(
                ImmutableCatalogRegistration.builder()
                    .address(host)
                    .node(serviceName)
                    .service(
                        ImmutableService.builder()
                            .id("$clusterName-$host-$port")
                            .service(serviceName)
                            .addAllTags(orgs)
                            .port(port)
                            .address(host)
                            .build()
                    )
                    .build()
            )
        } catch (ex: Exception) {
            // skip
        }
    }

    private fun discoverServices() {
        try {
            val catalogClient = consul.catalogClient()

            val serviceIds = catalogClient.services.response
                .mapNotNull {
                    val (name, tags) = it.toPair()
                    if (tags.any { orgs.contains(it) }) {
                        name
                    } else {
                        null
                    }
                }

            activeNodes = serviceIds.flatMap {
                try {
                    catalogClient.getService(it).response
                        .map { Pair(it.serviceAddress, it.servicePort) }
                } catch (ex: Exception) {
                    listOf<Pair<String, Int>>()
                }
            }
        } catch (ex: Exception) {
            // skip
        }
    }

    suspend fun broadcast(vararg message: Message) {
        activeNodes.map { node ->
            val addr = InetSocketAddress(node.first, node.second)
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
                } finally {
                    pool.release(channel)
                        .waitComplete()
                }

            }
        }.forEach { it.await() }

    }

    fun close() {
        clientBootstrap.config().group().shutdownGracefully()

        serviceDiscoveryThread.interrupt()
        serviceDiscoveryThread.join()

        consul.catalogClient().deregister(
            ImmutableCatalogDeregistration.builder()
                .node(serviceName)
                .serviceId("m-$host-$port")
                .build()
        )

        serverBootstrap.config().group().shutdownGracefully()
        serverBootstrap.config().childGroup().shutdownGracefully()
    }
}

suspend fun ChannelFuture.waitComplete(): Channel {
    return suspendCoroutine<Channel> { cont ->
        this.addListener { future ->
            if (future.isSuccess) {
                cont.resume(this.channel())
            } else {
                cont.resumeWithException(future.cause())
            }
        }
    }
}

suspend fun <T> Future<T>.waitComplete(): T {
    return suspendCoroutine<T> { cont ->
        this.addListener { future ->
            if (future.isSuccess) {
                cont.resume(this.get())
            } else {
                cont.resumeWithException(future.cause())
            }
        }
    }
}

fun main(args: Array<String>) {
    val node1 = DistributedMessageBus("ms", listOf("org1"), 65431 downTo 1)
    val node2 = DistributedMessageBus("ms", listOf("org1"), 65431 downTo 1)
    val node3 = DistributedMessageBus("ms", listOf("org1"), 65431 downTo 1)

    node1.listeners += { println("node1: $it") }
    node2.listeners += { println("node2: $it") }
    node3.listeners += { println("node3: $it") }

    Thread.sleep(5000)

    runBlocking {
        for (i in 1..100) {
            node1.broadcast(
                Message("abc$i", "def1"),
                Message("abc$i", "def2"),
                Message("abc$i", "def3"),
                Message("abc$i", "def4"),
                Message("abc$i", "def5"),
                Message("abc$i", "def6"),
                Message("abc$i", "def7")
            )
        }
    }

    node1.close()
    node2.close()
    node3.close()
}