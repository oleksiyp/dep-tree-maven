package run

import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule

class EncoderDecoder<K, V>(
    val type: String,
    val keyValueType: TypeReference<Pair<K, V>>
) {
    val mapper = ObjectMapper()
        .registerKotlinModule()

    suspend fun encodeAndSend(destination: String, key: K, value: V, messageBus: DistributedMessageBus) {
        val msg = mapper.writeValueAsString(Pair(key, value))
        messageBus.broadcast(destination, Message(type, msg))
    }

    fun listenAndDecode(messageBus: DistributedMessageBus, handler: suspend (K, V) -> Unit) {
        messageBus.listeners += {
            val keyValue = mapper.readValue<Pair<K, V>>(
                it.msg,
                keyValueType
            )
            handler(keyValue.first, keyValue.second)
        }
    }
}

fun <K, V> CachedExecutor<K, V>.bindToMessageBus(
    valuesED: EncoderDecoder<K, V>,
    excpetionsED: EncoderDecoder<K, String>,
    messageBus: DistributedMessageBus,
    destination: (K) -> String
) {
    listener = { key, value, exception ->
        if (value != null) {
            valuesED.encodeAndSend(destination(key), key, value, messageBus)
        } else if (exception != null) {
            excpetionsED.encodeAndSend(destination(key), key, exception.message!!, messageBus)
        }
    }

    valuesED.listenAndDecode(messageBus) { key, value ->
        addCache(key, value)
    }
    excpetionsED.listenAndDecode(messageBus) { key, value ->
        addCacheException(
            key,
            Exception(value)
        )
    }
}
