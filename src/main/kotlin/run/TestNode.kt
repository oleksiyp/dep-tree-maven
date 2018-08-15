package run

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonTypeRef
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.ktor.application.call
import io.ktor.application.install
import io.ktor.request.uri
import io.ktor.response.respond
import io.ktor.routing.Routing
import io.ktor.routing.get
import io.ktor.routing.routing
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty
import kotlinx.coroutines.experimental.delay
import java.net.BindException
import java.net.ServerSocket
import java.util.*

class TestNode(vararg val orgs: String) {
    val mapper = ObjectMapper()
        .registerKotlinModule()

    val registry = ServiceRegistry(
        "ms",
        orgs.toList(),
        listOf("bus", "http")
    )

    val busActiveNodes = { org: String -> (registry.activeNodes["bus"] ?: mapOf())[org] ?: listOf() }
    val messageBus = DistributedMessageBus(65431 downTo 1, busActiveNodes)

    init {
        registry.announcePort("bus", messageBus.port)
    }

    fun allocatePort(range: Iterable<Int>): Int {
        for (port in range) {
            try {
                ServerSocket(port).close()
                return port
            } catch (ex: BindException) {
                // skip
            }
        }
        throw RuntimeException("no port available in $range")
    }

    val httpPort = allocatePort(65432..65535)

    val server = embeddedServer(
        Netty,
        httpPort,
        module = {
            install(Routing)
            val rnd = Random()
            routing {
                for (org in orgs) {
                    get("/$org/test") {
                        val value = cachedExecutor.submit(org) {
                            //                        if (rnd.nextDouble() > 0.8) {
//                            throw Exception(this + "value")
//                        } else {
                            delay(rnd.nextInt(2500) + 500)
                            "$org value " + rnd.nextLong()
//                        }
                        }
                        call.respond(httpPort.toString() + " " +value.await() )
                    }
                }
                get {
                    call.respond("Error ${call.request.uri}")
                }
            }
        }
    )

    init {
        server.start()
        val pathes = orgs.joinToString(", ") { "/$it" }
        registry.announcePort(
            "http",
            httpPort,
            "traefik.enable=true",
            "traefik.frontend.rule=Host:localhost;PathPrefix:$pathes"
        )

        registry.startDiscovery()

        println("Discovered: " + registry.activeNodes)
    }

    val listener: suspend (String, String?, Exception?) -> Unit =
        { key, value, exception -> calculationDone(key, value, exception) }

    val cachedExecutor = CachedExecutor<String, String>(
        5, 5000, listener
    )

    init {
        messageBus.listeners += {
            if (it.type == "cache") {
                val keyValue = mapper.readValue<Pair<String, String>>(
                    it.msg,
                    jacksonTypeRef<Pair<String, String>>()
                )
                cachedExecutor.addCache(keyValue.first, keyValue.second)
            } else if (it.type == "cache-exception") {
                val keyValue = mapper.readValue<Pair<String, String>>(
                    it.msg,
                    jacksonTypeRef<Pair<String, String>>()
                )
                cachedExecutor.addCacheException(keyValue.first, Exception(keyValue.second))
            }
        }
    }


    private suspend fun calculationDone(key: String, value: String?, exception: Exception?) {
        if (value != null) {
            val msg = mapper.writeValueAsString(Pair(key, value))
            messageBus.broadcast(key, Message("cache", msg))
        } else if (exception != null) {
            val msg = mapper.writeValueAsString(Pair(key, exception.message))
            messageBus.broadcast(key, Message("cache-exception", msg))
        }
    }

    fun close() {
        cachedExecutor.stop()
        messageBus.close()
        registry.stop()
    }

    override fun toString(): String {
        return "port:" + messageBus.port
    }
}

fun main(args: Array<String>) {
    val nodes = listOf(
        TestNode("org1"),
        TestNode("org2"),
        TestNode("org3"),
        TestNode("org4"),
        TestNode("org5"),
        TestNode("org6"),
        TestNode("org1", "org2"),
        TestNode("org2", "org3"),
        TestNode("org3", "org4"),
        TestNode("org4", "org5"),
        TestNode("org5", "org6"),
        TestNode("org6", "org1")
    )

    Runtime.getRuntime().addShutdownHook(Thread {
        nodes.forEach { it.close() }
    })
}
