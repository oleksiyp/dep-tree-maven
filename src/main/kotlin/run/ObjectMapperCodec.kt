package run

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.netty.buffer.ByteBuf
import io.netty.buffer.ByteBufInputStream
import io.netty.buffer.ByteBufOutputStream
import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.ByteToMessageCodec
import java.io.InputStream
import java.io.OutputStream

class ObjectMapperCodec : ByteToMessageCodec<Message>() {
    private val mapper = ObjectMapper()
        .registerKotlinModule()

    override fun encode(ctx: ChannelHandlerContext, msg: Message, out: ByteBuf) {
        ByteBufOutputStream(out).use {
            mapper.writeValue(it as OutputStream, msg)
        }
    }

    override fun decode(ctx: ChannelHandlerContext, input: ByteBuf, out: MutableList<Any>) {
        val inputStream = ByteBufInputStream(input) as InputStream
        val decodedMessage = mapper.readValue(inputStream, Message::class.java)
        out.add(decodedMessage)
    }

}