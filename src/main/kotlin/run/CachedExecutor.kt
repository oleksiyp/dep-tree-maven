package run

import kotlinx.coroutines.experimental.*
import kotlinx.coroutines.experimental.channels.Channel
import kotlinx.coroutines.experimental.selects.select
import java.util.concurrent.ConcurrentHashMap

class CachedExecutor<K, R>(
    val nWorkers: Int,
    val cacheTime: Long
) {
    inner class DeferredTask(
        val key: K,
        val taskFunc: suspend () -> R,
        val deferred: CompletableDeferred<R>
    )

    val map = ConcurrentHashMap<K, DeferredTask>()

    private val channel = Channel<DeferredTask>()

    init {
        repeat(nWorkers) {
            launch {
                for (record in channel) {
                    try {
                        val result = record.taskFunc()
                        record.deferred.complete(result)
                    } catch (ex: Exception) {
                        record.deferred.completeExceptionally(ex)
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
        taskFunc: suspend () -> R
    ): Deferred<R> {

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
}

