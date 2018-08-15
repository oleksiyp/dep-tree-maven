package run

import kotlinx.coroutines.experimental.*
import kotlinx.coroutines.experimental.channels.Channel
import kotlinx.coroutines.experimental.selects.select
import java.util.concurrent.ConcurrentHashMap

class CachedExecutor<K, R>(
    val nWorkers: Int,
    val cacheTime: Long,
    var listener: (suspend (K, R?, Exception?) -> Unit)? = null
) {
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
    private val channel = Channel<DeferredTask>(100 * 1024)

    init {
        repeat(nWorkers) {
            launch {
                for (record in channel) {
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
            channel.send(task)
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
        channel.close()
    }
}

