package run

import kotlinx.coroutines.experimental.*
import kotlinx.coroutines.experimental.channels.Channel
import kotlinx.coroutines.experimental.selects.select
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicIntegerArray
import java.util.concurrent.atomic.AtomicLong

class CachedExecutor<K, R>(
    val nWorkers: Int,
    val cacheTime: Long,
    val queueCapacity: Int = 1024
) {
    var listener: (suspend (K, R?, Exception?) -> Unit)? = null

    inner class DeferredTask(
        val key: K,
        val taskFunc: suspend K.() -> R,
        val deferred: CompletableDeferred<R>
    )

    inner class CacheEntry(
        val key: K,
        val result: () -> R,
        var evictJob: Job? = null
    )

    fun addCache(key: K, value: R) {
        addCache(CacheEntry(key, { value }))
        tasks[key]?.deferred?.complete(value)
    }

    private fun addCache(entry: CacheEntry) {
        entry.evictJob = launch {
            delay(cacheTime)
            cache.remove(entry.key)
        }
        cache[entry.key]?.evictJob?.cancel()
        val oldEntry = cache.put(entry.key, entry)
        oldEntry?.evictJob?.cancel()
    }

    fun addCacheException(key: K, exception: Exception) {
        addCache(CacheEntry(key, { throw exception }))
        tasks[key]?.deferred?.completeExceptionally(exception)
    }

    private val cache = ConcurrentHashMap<K, CacheEntry>()
    private val tasks = ConcurrentHashMap<K, DeferredTask>()
    private val channel = Channel<DeferredTask>(queueCapacity)
    private val stats = Stats()

    private val submitCount = AtomicLong()
    private val submitSpeed = SpeedMeasurement(submitCount)

    fun reportStats(stats: MutableList<StatsEntry>) {
        stats.add(StatsEntry.RpsStatsEntry("Submit speed", submitSpeed.speed))
        stats.add(StatsEntry.UseStatsEntry("Executor use", this.stats.use))
        stats.add(StatsEntry.CountStatsEntry("Cache entries", cache.size.toLong()))
        stats.add(StatsEntry.CountStatsEntry("Task entries", tasks.size.toLong()))
    }

    init {
        repeat(nWorkers) { n ->
            launch {
                stats.workersUtilization.set(n, 0)
                for (record in channel) {
                    stats.itemsDequeued.incrementAndGet()
                    stats.workersUtilization.set(n, 1)
                    try {
                        val result = record.taskFunc(record.key)
                        record.deferred.complete(result)
                        listener?.invoke(record.key, result, null)
                    } catch (ex: Exception) {
                        record.deferred.completeExceptionally(ex)
                        listener?.invoke(record.key, null, ex)
                    }

                    if (cacheTime <= 0) {
                        tasks.remove(record.key)
                    } else {
                        launch {
                            delay(cacheTime)
                            tasks.remove(record.key)
                        }
                    }
                    stats.workersUtilization.set(n, 0)
                }
            }
        }
    }

    suspend fun submit(
        key: K,
        waitTimeout: Long = 0,
        defaultValue: () -> R? = { null },
        taskFunc: suspend K.() -> R
    ): Deferred<R> {
        submitCount.incrementAndGet()

        cache[key]?.result?.let { return async { it() } }

        val task = DeferredTask(key, taskFunc, CompletableDeferred())
        val otherTask = tasks.putIfAbsent(key, task)
        val deferred = if (otherTask != null) {
            otherTask.deferred
        } else {
            if (!channel.offer(task)) {
                tasks.remove(key)
                stats.errorCount.incrementAndGet()
                throw RuntimeException("queue overflow ${stats.use}")
            } else {
                stats.itemsEnqueued.incrementAndGet()
            }
            task.deferred
        }

        if (waitTimeout > 0) {
            val value = defaultValue() ?: return deferred
            return async {
                select<R> {
                    deferred.onAwait { it }
                    onTimeout(waitTimeout) { value }
                }
            }
        }

        return deferred
    }

    fun stop() {
        stats.stop()
        channel.close()
    }

    private inner class Stats {
        val workersUtilization = AtomicIntegerArray(nWorkers)
        val itemsEnqueued = AtomicLong()
        val itemsDequeued = AtomicLong()
        val errorCount = AtomicLong()

        private val windowSize = 30
        private val measurementDelay = 100L

        @Volatile
        var use = UseStats(0.0, 0.0, 0.0, 0, 0.0)

        val job = launch {
            val utilizationWindow = mutableListOf<List<Int>>()

            val dequeueSpeed = SpeedMeasurement(
                itemsDequeued,
                windowSize,
                measurementDelay,
                this.coroutineContext[Job]
            )
            val errorSpeed = SpeedMeasurement(
                errorCount,
                windowSize,
                measurementDelay,
                this.coroutineContext[Job]
            )

            while (isActive) {
                delay(measurementDelay)

                val momentUtilization = (0 until nWorkers).map { workersUtilization.get(it) }
                utilizationWindow.add(momentUtilization)
                while (utilizationWindow.size > windowSize) {
                    utilizationWindow.removeAt(0)
                }

                val utilizationValue =
                    utilizationWindow.sumByDouble { it.sum().toDouble() } / (utilizationWindow.size * nWorkers)

                val saturationValue = (itemsEnqueued.get() - itemsDequeued.get()).toDouble() / queueCapacity

                use = UseStats(
                    dequeueSpeed.speed,
                    utilizationValue,
                    saturationValue,
                    errorCount.get(),
                    errorSpeed.speed
                )
            }
        }

        fun stop() {
            job.cancel()
        }
    }
}

