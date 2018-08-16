package run

import com.fasterxml.jackson.module.kotlin.jacksonTypeRef
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
import kotlinx.coroutines.experimental.launch
import java.net.BindException
import java.net.ServerSocket
import java.util.*

class TestNode(vararg val orgs: String) {
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
                        val value = cachedExecutor.submit(OrgKey(org, call.parameters["key"] ?: "default")) {
                            delay(rnd.nextInt(2500) + 500)
                            "$this value " + rnd.nextLong()
                        }
                        call.respond(httpPort.toString() + " " + value.await())
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

    val cachedExecutor = CachedExecutor<OrgKey, String>(
        10, 5000, 500
    )

    val printJob = launch {
        while (isActive) {
            delay(1000)
            val use = cachedExecutor.use()
            if (use.isNonZero()) {
                println(orgs.joinToString("-") + " " + use)
            }
        }
    }

    init {
        cachedExecutor.bindToMessageBus(
            EncoderDecoder("cache", jacksonTypeRef()),
            EncoderDecoder("cache-exceptions", jacksonTypeRef()),
            messageBus,
            OrgKey::org
        )
    }


    fun close() {
        printJob.cancel()
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
