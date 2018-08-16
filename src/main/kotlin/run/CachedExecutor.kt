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
        var job: Job? = null
    )

    fun addCache(key: K, value: R) {
        addCache(CacheEntry(key, { value }))
        map[key]?.deferred?.complete(value)
    }

    private fun addCache(entry: CacheEntry) {
        entry.job = launch {
            delay(cacheTime)
            cache.remove(entry.key)
        }
        val oldEntry = cache.put(entry.key, entry)
        oldEntry?.job?.cancel()
    }

    fun addCacheException(key: K, exception: Exception) {
        addCache(CacheEntry(key, { throw exception }))
        map[key]?.deferred?.completeExceptionally(exception)
    }

    private val cache = ConcurrentHashMap<K, CacheEntry>()
    private val map = ConcurrentHashMap<K, DeferredTask>()
    private val channel = Channel<DeferredTask>(queueCapacity)
    private val stats = Stats()

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
                        map.remove(record.key)
                    } else {
                        launch {
                            delay(cacheTime)
                            map.remove(record.key)
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

        cache[key]?.result?.let { return async { it() } }

        val task = DeferredTask(key, taskFunc, CompletableDeferred())
        val otherTask = map.putIfAbsent(key, task)
        val deferred = if (otherTask != null) {
            otherTask.deferred
        } else {
            if (!channel.offer(task)) {
                map.remove(key)
                stats.errorCount.incrementAndGet()
                throw RuntimeException("queue overflow ${use()}")
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

    fun use() = stats.use

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
        private val measurementDelay = 100

        @Volatile
        var use = UseStats(0.0, 0.0, 0.0, 0, 0)


        val job = launch {
            val utilizationWindow = mutableListOf<List<Int>>()
            val errorsWindow = mutableListOf<Long>()
            val dequeueSpeedWindow = mutableListOf<Long>()

            var lastErrorCount = errorCount.get()
            var lastItemsDequeued = itemsDequeued.get()
            while (isActive) {
                delay(measurementDelay)

                val newItemsDequeued = itemsDequeued.get()
                dequeueSpeedWindow.add(newItemsDequeued - lastItemsDequeued)
                while (dequeueSpeedWindow.size > windowSize) {
                    dequeueSpeedWindow.removeAt(0)
                }

                val dequeueSpeed =
                    dequeueSpeedWindow.sum().toDouble() * 1000 / (dequeueSpeedWindow.size * measurementDelay)

                val momentUtilization = (0 until nWorkers).map { workersUtilization.get(it) }
                utilizationWindow.add(momentUtilization)
                while (utilizationWindow.size > windowSize) {
                    utilizationWindow.removeAt(0)
                }

                val utilizationValue =
                    utilizationWindow.sumByDouble { it.sum().toDouble() } / (utilizationWindow.size * nWorkers)

                val saturationValue = (itemsEnqueued.get() - itemsDequeued.get()).toDouble() / queueCapacity

                val newErrorCount = errorCount.get()
                errorsWindow.add(newErrorCount - lastErrorCount)
                while (errorsWindow.size > windowSize) {
                    errorsWindow.removeAt(0)
                }

                use = UseStats(dequeueSpeed, utilizationValue, saturationValue, errorCount.get(), errorsWindow.sum())
                lastErrorCount = newErrorCount
                lastItemsDequeued = newItemsDequeued
            }
        }

        fun stop() {
            job.cancel()
        }
    }
}

