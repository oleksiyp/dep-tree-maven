/*
 * Copyright (C) 2015 Square, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package run

import java.io.IOException
import okhttp3.MediaType
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.ResponseBody
import okio.Buffer
import okio.BufferedSource
import okio.ForwardingSource
import okio.Okio
import okio.Source

class Progress(val downloadNotifier: DownloadNotifier) {

    @Throws(Exception::class)
    fun run() {
        val request = Request.Builder()
            .url("https://publicobject.com/helloworld.txt")
            .build()

        val client = OkHttpClient.Builder()
            .addNetworkInterceptor { chain ->
                val originalResponse = chain.proceed(chain.request())
                val download = downloadNotifier.startDownload(chain.request().url().toString())
                originalResponse.newBuilder()
                    .body(ProgressResponseBody(originalResponse.body()!!, download))
                    .build()
            }
            .build()

        client.newCall(request).execute().use { response ->
            if (!response.isSuccessful) throw IOException("Unexpected code $response")

            println(response.body()!!.string())
        }
    }

    private class ProgressResponseBody internal constructor(
        private val responseBody: ResponseBody,
        private val download: DownloadNotifier.Download
    ) : ResponseBody() {

        private var bufferedSource: BufferedSource? = null

        override fun contentType(): MediaType? {
            return responseBody.contentType()
        }

        override fun contentLength(): Long {
            return responseBody.contentLength()
        }

        override fun source(): BufferedSource {
            val source = bufferedSource
            return if (source == null) {
                val buffer = Okio.buffer(source(responseBody.source()))
                bufferedSource = buffer
                buffer
            } else {
                source
            }
        }

        private fun source(source: Source): Source {
            return object : ForwardingSource(source) {
                internal var totalBytesRead = 0L

                @Throws(IOException::class)
                override fun read(sink: Buffer, byteCount: Long): Long {
                    val bytesRead = super.read(sink, byteCount)
                    // read() returns the number of bytes read, or -1 if this source is exhausted.
                    totalBytesRead += if (bytesRead != -1L) bytesRead else 0
                    val len = responseBody.contentLength()
                    if (len != -1L) {
                        download.progress(totalBytesRead, len)
                    }
                    return bytesRead
                }

                override fun close() {
                    super.close()
                    download.done()
                }
            }
        }
    }


}
