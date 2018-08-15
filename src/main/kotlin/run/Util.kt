package run

import io.netty.channel.Channel
import io.netty.channel.ChannelFuture
import io.netty.util.concurrent.Future
import kotlin.coroutines.experimental.suspendCoroutine

suspend fun ChannelFuture.waitComplete(): Channel {
    return suspendCoroutine { cont ->
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
    return suspendCoroutine { cont ->
        this.addListener { future ->
            if (future.isSuccess) {
                cont.resume(this.get())
            } else {
                cont.resumeWithException(future.cause())
            }
        }
    }
}
