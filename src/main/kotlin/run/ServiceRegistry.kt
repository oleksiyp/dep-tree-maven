package run

import com.google.common.net.HostAndPort
import com.orbitz.consul.CatalogClient
import com.orbitz.consul.Consul
import com.orbitz.consul.model.agent.ImmutableRegCheck
import com.orbitz.consul.model.catalog.CatalogService
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
        .withPing(false)
        .build()

    private val firstDiscoveryIterationDone = CompletableDeferred<Boolean>()

    private val serviceName = "$clusterName-" + federations.sorted().joinToString("-")

    @Volatile
    var activeNodes: Map<String, Map<String, List<Registration>>> =
        categories.map { it to mapOf<String, List<Registration>>() }.toMap()

    val announcedPorts = Collections.synchronizedMap(mutableMapOf<String, Registration>())

    fun announcePort(category: String, port: Int, vararg tags: String) {
        val registration = Registration(host, port, tags.toList() + category + federations)
        selfRegister(category, registration)
        announcedPorts[category] = registration
    }

    private val CatalogService.serviceHost: String
        get() = if (serviceAddress.isBlank()) host else serviceAddress

    private fun discoverServices() {
        try {
            val catalogClient = consul.catalogClient()
            var services = fetchCatalogServices(catalogClient)

            val toRegister = whatToRegisterMore(services)

            if (toRegister.isNotEmpty()) {
                for ((category, registration) in toRegister) {
                    selfRegister(category, registration)
                }

                services = fetchCatalogServices(catalogClient)
            }

            activeNodes = groupActiveNodes(services)
        } catch (ex: Exception) {
            // skip
        }
    }

    private fun whatToRegisterMore(services: List<CatalogService>): List<Pair<String, Registration>> {
        return synchronized(announcedPorts) { announcedPorts.toList() }
            .mapNotNull { (category, registration) ->

                if (!services.any { serviceDef ->
                        serviceDef.serviceHost == registration.host &&
                                serviceDef.servicePort == registration.port &&
                                serviceDef.serviceTags.toSet() == registration.tags.toSet()
                    }) category to registration else null
            }
    }

    private fun selfRegister(category: String, registration: Registration) {
        val (host, port, tags) = registration
        try {
            consul.agentClient().register(
                port,
                ImmutableRegCheck.builder()
                    .tcp("$host:$port")
                    .interval("10s")
                    .timeout("10s")
                    .deregisterCriticalServiceAfter("1m")
                    .build(),
                "$category-$serviceName",
                "$category-$clusterName-$host-$port",
                tags.toList(),
                mutableMapOf()
            )
        } catch (ex: Exception) {
            // skip
        }
    }

    private fun groupActiveNodes(services: List<CatalogService>): Map<String, Map<String, List<Registration>>> {
        return categories.map { category ->
            category to federations.map { federation ->
                federation to services.mapNotNull { serviceDef ->

                    val isFromCategory = serviceDef.serviceTags.contains(category)
                    val isFromFederation = serviceDef.serviceTags.contains(federation)

                    if (!isFromCategory || !isFromFederation) {
                        return@mapNotNull null
                    }

                    Registration(
                        serviceDef.serviceHost,
                        serviceDef.servicePort,
                        serviceDef.serviceTags
                    )
                }
            }.toMap()
        }.toMap()
    }

    private fun fetchCatalogServices(catalogClient: CatalogClient): List<CatalogService> {
        return catalogClient.services.response
            .flatMap {
                val (id, tags) = it.toPair()

                val isFromCategory = tags.any { categories.contains(it) }
                val isFromFederation = tags.any { federations.contains(it) }

                if (isFromFederation && isFromCategory) {
                    catalogClient.getService(id).response
                } else {
                    listOf()
                }
            }
    }

    private val serviceDiscoveryThread = Thread({
        while (!Thread.interrupted()) {
            discoverServices()
            firstDiscoveryIterationDone.complete(true)
            try {
                Thread.sleep(10000)
            } catch (ex: InterruptedException) {
                break
            }
        }
    }, "service-discovery")

    fun startDiscovery() {
        serviceDiscoveryThread.start()
        runBlocking {
            firstDiscoveryIterationDone.await()
        }
    }

    fun stop(deregister: Boolean = true) {
        serviceDiscoveryThread.interrupt()
        serviceDiscoveryThread.join()

        if (deregister) {
            synchronized(announcedPorts) { announcedPorts.toList() }.forEach { (category, registration) ->
                val (host, port, _) = registration
                consul.agentClient()
                    .deregister("$category-$clusterName-$host-$port")
            }
        }
    }
}

