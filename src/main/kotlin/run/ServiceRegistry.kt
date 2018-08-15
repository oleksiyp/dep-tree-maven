package run

import com.google.common.net.HostAndPort
import com.orbitz.consul.Consul
import com.orbitz.consul.model.catalog.ImmutableCatalogRegistration
import com.orbitz.consul.model.health.ImmutableService
import kotlinx.coroutines.experimental.CompletableDeferred
import kotlinx.coroutines.experimental.runBlocking
import java.net.InetAddress
import java.util.*

class ServiceRegistry(
    val clusterName: String,
    val federations: List<String>,
    val categories: List<String>,
    val consulHostAndPort: String = "localhost:8500",
    val host: String = InetAddress.getLocalHost().hostAddress
) {

    private val consul = Consul.builder()
        .withHostAndPort(HostAndPort.fromString(consulHostAndPort))
        .build()

    private val firstDiscoveryIterationDone = CompletableDeferred<Boolean>()

    private val serviceName = "$clusterName-" + federations.sorted().joinToString("-")

    @Volatile
    var activeNodes: Map<String, Map<String, List<Registration>>> =
        categories.map { it to mapOf<String, List<Registration>>() }.toMap()

    val registered = Collections.synchronizedMap(mutableMapOf<String, Int>())

    fun announcePort(category: String, port: Int, vararg tags: String) {
        consul.catalogClient().register(
            ImmutableCatalogRegistration.builder()
                .address(host)
                .node(serviceName)
                .service(
                    ImmutableService.builder()
                        .id("$category-$clusterName-$host-$port")
                        .service("$category-$serviceName")
                        .addAllTags(federations + tags.toList() + category)
                        .port(port)
                        .address(host)
                        .build()
                )
                .build()
        )
        registered[category] = port
    }


    private fun discoverServices() {
        try {
            val catalogClient = consul.catalogClient()

            val serviceIds = catalogClient.services.response
                .flatMap {
                    val (id, tags) = it.toPair()
                    if (tags.any { federations.contains(it) } && tags.any { categories.contains(it) }) {
                        catalogClient.getService(id).response
                    } else {
                        listOf()
                    }
                }

            activeNodes = categories.map { category ->
                category to federations.map { federation ->
                    federation to serviceIds.mapNotNull { serviceDef ->
                        val isFromCategory = serviceDef.serviceTags.contains(category)
                        val isFromFederation = serviceDef.serviceTags.contains(federation)
                        if (isFromCategory && isFromFederation) {
                            Registration(serviceDef.serviceAddress, serviceDef.servicePort)
                        } else {
                            null
                        }
                    }
                }.toMap()
            }.toMap()
        } catch (ex: Exception) {
            // skip
        }
    }

    private val serviceDiscoveryThread = Thread({
        while (!Thread.interrupted()) {
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


}