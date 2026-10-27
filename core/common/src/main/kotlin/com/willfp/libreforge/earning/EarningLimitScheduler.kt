package com.willfp.libreforge.earning

import com.willfp.eco.core.Eco
import com.willfp.eco.core.scheduling.EcoTask
import com.willfp.libreforge.plugin
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import java.util.logging.Level

internal class EarningLimitScheduler {
    private data class PlayerGroup(val player: UUID, val group: EarningLimitGroup)

    private data class IntervalReset(val expiresAt: Long, val elapsedExpiresAt: Long) {
        fun remainingMillis(elapsedTime: Long): Long = elapsedExpiresAt - elapsedTime
    }

    private val cronResets = ConcurrentHashMap<EarningLimitGroup, Long>()
    private val intervalResets = ConcurrentHashMap<PlayerGroup, IntervalReset>()

    @Volatile
    private var task: EcoTask? = null

    fun start(groups: Collection<EarningLimitGroup>) {
        task?.cancel()
        task = plugin.scheduler.async().runTimer(1, 1) { tick() }
        reload(groups)
    }

    fun reload(groups: Collection<EarningLimitGroup>) {
        if (task?.isCancelled != false) return

        cronResets.keys.removeIf { group ->
            !group.active || group.policy.cron == null
        }
        intervalResets.keys.removeIf { (_, group) ->
            !group.active || group.policy.cron != null
        }

        plugin.scheduler.async().run {
            if (task == null || groups.isEmpty()) return@run

            val saved = try {
                // The zero UUID belongs to the server profile, which stores cron metadata.
                val players = Eco.get().savedProfileUUIDs - UUID(0, 0)
                Eco.get().readAllProfileValuesForKeys(players, groups.map { it.key })
            } catch (exception: Exception) {
                plugin.logger.log(Level.SEVERE, "Failed to load earning limit states", exception)
                return@run
            }

            for (group in groups) {
                if (task == null) return@run
                if (!group.active) continue

                val players = group.players.toMutableSet()
                for ((player, raw) in saved[group.key].orEmpty()) {
                    val pending = try {
                        val state = EarningState.decode(raw as String)
                        state.amount > 0 || (group.policy.cron == null && state.expiresAt != 0L)
                    } catch (_: IllegalArgumentException) {
                        true
                    }
                    if (pending) players += player
                }

                for (player in players) {
                    if (task == null) return@run
                    refresh(player, group)
                }
            }
        }
    }

    /** Remaining interval time in milliseconds, independent of server clock adjustments. */
    fun getRemainingIntervalMillis(
        player: UUID,
        group: EarningLimitGroup,
        policy: EarningPolicy,
        state: EarningState,
        elapsedTime: Long
    ): Long? {
        if (policy.cron != null || state.expiresAt == 0L) return null
        val scheduled = intervalResets[PlayerGroup(player, group)] ?: return null
        if (scheduled.expiresAt != state.expiresAt) return null
        return scheduled.remainingMillis(elapsedTime)
    }

    fun schedule(
        player: UUID,
        group: EarningLimitGroup,
        policy: EarningPolicy,
        state: EarningState,
        time: Long,
        elapsedTime: Long
    ) {
        val pending = state.amount > 0 || (policy.cron == null && state.expiresAt != 0L)
        if (pending) group.players.add(player) else group.players.remove(player)

        if (policy.cron != null) {
            cronResets.compute(group) { _, previous ->
                if (task == null || !group.active) return@compute previous
                if (group.policy !== policy) {
                    plugin.scheduler.async().run { refresh(player, group) }
                    return@compute previous
                }

                if (state.expiresAt == 0L || group.players.isEmpty()) return@compute null
                minOf(previous ?: state.expiresAt, state.expiresAt)
            }
            return
        }

        intervalResets.compute(PlayerGroup(player, group)) { _, previous ->
            if (task == null || !group.active) return@compute previous
            if (group.policy !== policy) {
                plugin.scheduler.async().run { refresh(player, group) }
                return@compute previous
            }

            if (state.expiresAt == 0L) return@compute null
            IntervalReset(state.expiresAt, elapsedTime + (state.expiresAt - time))
        }
    }

    private fun tick() {
        if (task == null) return

        val now = System.currentTimeMillis()
        for ((group, expiresAt) in cronResets) {
            if (task == null) return
            if (expiresAt > now || !cronResets.remove(group, expiresAt)) continue
            for (player in group.players) {
                if (task == null) return
                refresh(player, group)
            }
        }

        val elapsedTime = monotonicTimeMillis()
        for ((target, scheduled) in intervalResets) {
            if (task == null) return
            if (scheduled.remainingMillis(elapsedTime) > 0) continue
            refresh(target.player, target.group)
            intervalResets.remove(target, scheduled)
        }
    }

    private fun refresh(player: UUID, group: EarningLimitGroup) {
        try {
            EarningLimits.refresh(player, group)
        } catch (exception: Exception) {
            plugin.logger.log(Level.SEVERE, "Failed to reset earning limit ${group.key.key} for $player", exception)
        }
    }

    fun stop() {
        val current = task
        task = null
        current?.cancel()
        for ((player, group) in intervalResets.keys) {
            refresh(player, group)
        }
        cronResets.clear()
        intervalResets.clear()
    }
}

internal fun monotonicTimeMillis(): Long = TimeUnit.NANOSECONDS.toMillis(System.nanoTime())
