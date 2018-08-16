package run

import kotlinx.coroutines.experimental.Job
import kotlinx.coroutines.experimental.delay
import kotlinx.coroutines.experimental.launch
import java.util.concurrent.atomic.AtomicLong

class SpeedMeasurement(
    val absoluteValue: AtomicLong,
    val windowSize: Int = 30,
    val measurementDelay: Long = 100,
    val parentJob: Job? = null
) {
    @Volatile
    var speed: Double = 0.0

    private val job = launch(parent = parentJob) {
        var lastValue = absoluteValue.get()
        val speedWindow = mutableListOf<Long>()

        while (isActive) {
            delay(measurementDelay)

            val newValue = absoluteValue.get()
            speedWindow.add(newValue - lastValue)
            while (speedWindow.size > windowSize) {
                speedWindow.removeAt(0)
            }

            speed = speedWindow.sum().toDouble() * 1000.0 / (speedWindow.size * measurementDelay)
            lastValue = newValue
        }
    }

    fun stop() {
        job.cancel()
    }
}