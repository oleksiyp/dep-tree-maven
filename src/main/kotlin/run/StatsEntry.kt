package run

import com.fasterxml.jackson.annotation.JsonIgnore

sealed class StatsEntry(name: String, val type: String) {
    @JsonIgnore
    val statName = name

    @get:JsonIgnore
    abstract val isEmpty: Boolean

    data class RpsStatsEntry(val name: String, val speed: Double) : StatsEntry(name, "rps") {
        override val isEmpty: Boolean
            get() = speed <= 0.0
    }

    data class CountStatsEntry(val name: String, val count: Long) : StatsEntry(name, "count") {
        override val isEmpty: Boolean
            get() = count <= 0
    }

    data class UseStatsEntry(val name: String, val use: UseStats) : StatsEntry(name, "use") {
        override val isEmpty: Boolean
            get() = !use.isNonZero()
    }
}