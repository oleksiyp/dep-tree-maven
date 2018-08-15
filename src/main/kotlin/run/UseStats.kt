package run

data class UseStats(
    val speed: Double, // rps in window of 3 seconds
    val utilization: Double, // % in window of 3 seconds
    val saturation: Double, // % of queue size
    val errors: Long, // absolute error count
    val errorsInWindow: Long // errors in window of 3 seconds
) {
    fun isNonZero() = speed > 0 ||
            utilization > 0 ||
            saturation > 0 ||
            errorsInWindow > 0
}
