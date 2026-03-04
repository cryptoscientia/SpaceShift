#!/usr/bin/env python3
"""Event-driven SimPy models for SpaceShift balancing and stress analysis."""

from __future__ import annotations

import hashlib
import math
import random
import statistics
from typing import Any

import simpy


def _stable_hash_int(*parts: Any) -> int:
    digest = hashlib.sha256("|".join(str(part) for part in parts).encode("utf-8")).digest()
    return int.from_bytes(digest[:8], byteorder="big", signed=False)


def _percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    if len(values) == 1:
        return float(values[0])
    pos = (max(0.0, min(100.0, float(pct))) / 100.0) * (len(values) - 1)
    lo = int(math.floor(pos))
    hi = int(math.ceil(pos))
    if lo == hi:
        return float(values[lo])
    weight = pos - lo
    return float(values[lo] * (1.0 - weight) + values[hi] * weight)


def _run_queue_model(*, rng: random.Random, horizon_hours: float, players: int, config: dict[str, Any]) -> dict[str, Any]:
    env = simpy.Environment()
    compute_slots = max(1, int(config.get("simpy_compute_slots", 12)))
    fab_slots = max(1, int(config.get("simpy_fab_slots", 9)))
    compute_pool = simpy.Resource(env, capacity=compute_slots)
    fab_pool = simpy.Resource(env, capacity=fab_slots)

    research_waits: list[float] = []
    mfg_waits: list[float] = []
    research_service: list[float] = []
    mfg_service: list[float] = []
    compute_busy = [0.0]
    fab_busy = [0.0]
    created = {"research": 0, "manufacturing": 0}
    completed = {"research": 0, "manufacturing": 0}

    def research_job(workload: float) -> simpy.events.Event:
        created["research"] += 1
        queued_at = float(env.now)
        with compute_pool.request() as request:
            yield request
            start_at = float(env.now)
            wait = max(0.0, start_at - queued_at)
            throughput = 1.5 + (rng.random() * 1.8)
            service = max(0.03, workload / throughput)
            research_waits.append(wait)
            research_service.append(service)
            compute_busy[0] += service
            yield env.timeout(service)
            completed["research"] += 1

    def manufacturing_job(workload: float) -> simpy.events.Event:
        created["manufacturing"] += 1
        queued_at = float(env.now)
        with fab_pool.request() as request:
            yield request
            start_at = float(env.now)
            wait = max(0.0, start_at - queued_at)
            throughput = 1.2 + (rng.random() * 1.4)
            service = max(0.04, workload / throughput)
            mfg_waits.append(wait)
            mfg_service.append(service)
            fab_busy[0] += service
            yield env.timeout(service)
            completed["manufacturing"] += 1

    def player_generator(player_idx: int) -> simpy.events.Event:
        base_interval = 2.3 + ((player_idx % 7) * 0.18)
        while float(env.now) < horizon_hours:
            jitter = max(0.16, base_interval * rng.uniform(0.6, 1.4))
            yield env.timeout(rng.expovariate(1.0 / jitter))
            if rng.random() < 0.57:
                workload = rng.uniform(2.0, 14.0)
                env.process(research_job(workload))
            else:
                workload = rng.uniform(1.6, 11.0)
                env.process(manufacturing_job(workload))

    for idx in range(max(1, players)):
        env.process(player_generator(idx))
    env.run(until=float(horizon_hours))

    research_waits.sort()
    mfg_waits.sort()
    research_service.sort()
    mfg_service.sort()
    research_backlog = max(0, int(created["research"]) - int(completed["research"]))
    mfg_backlog = max(0, int(created["manufacturing"]) - int(completed["manufacturing"]))
    return {
        "horizon_hours": round(float(horizon_hours), 3),
        "players": int(players),
        "resources": {"compute_slots": compute_slots, "fab_slots": fab_slots},
        "research": {
            "created_jobs": int(created["research"]),
            "completed_jobs": int(completed["research"]),
            "backlog_jobs": int(research_backlog),
            "wait_mean_h": round(statistics.fmean(research_waits), 4) if research_waits else 0.0,
            "wait_p95_h": round(_percentile(research_waits, 95.0), 4),
            "service_mean_h": round(statistics.fmean(research_service), 4) if research_service else 0.0,
            "service_p95_h": round(_percentile(research_service, 95.0), 4),
        },
        "manufacturing": {
            "created_jobs": int(created["manufacturing"]),
            "completed_jobs": int(completed["manufacturing"]),
            "backlog_jobs": int(mfg_backlog),
            "wait_mean_h": round(statistics.fmean(mfg_waits), 4) if mfg_waits else 0.0,
            "wait_p95_h": round(_percentile(mfg_waits, 95.0), 4),
            "service_mean_h": round(statistics.fmean(mfg_service), 4) if mfg_service else 0.0,
            "service_p95_h": round(_percentile(mfg_service, 95.0), 4),
        },
        "utilization": {
            "compute": round(
                min(1.0, compute_busy[0] / max(0.0001, float(horizon_hours) * float(compute_slots))),
                4,
            ),
            "manufacturing": round(
                min(1.0, fab_busy[0] / max(0.0001, float(horizon_hours) * float(fab_slots))),
                4,
            ),
        },
    }


def _run_market_model(*, rng: random.Random, horizon_hours: float, config: dict[str, Any]) -> dict[str, Any]:
    env = simpy.Environment()
    anchor_price = float(config.get("simpy_market_anchor_price", 100.0))
    liquidity = max(200.0, float(config.get("simpy_market_liquidity", 1800.0)))
    inventory = [float(config.get("simpy_market_inventory_init", 12000.0))]
    price = [float(anchor_price)]
    price_history: list[dict[str, float]] = []
    returns: list[float] = []
    trades = {"buy_orders": 0, "sell_orders": 0, "shortage_events": 0}
    arbitrage_profit_credits = [0.0]

    def _apply_order(*, side: str, quantity: float, aggressiveness: float) -> None:
        qty = max(0.0, float(quantity))
        if qty <= 0.0:
            return
        imbalance = qty / liquidity
        drift = (0.0065 if side == "buy" else -0.0065) * imbalance * max(0.4, aggressiveness)
        mean_reversion = ((anchor_price - price[0]) / max(0.0001, anchor_price)) * 0.014
        noise = rng.gauss(0.0, 0.0012)
        next_price = price[0] * (1.0 + drift + mean_reversion + noise)
        bounded = max(2.0, min(anchor_price * 4.2, next_price))
        if price[0] > 0:
            returns.append(math.log(bounded / price[0]))
        price[0] = bounded
        if side == "buy":
            trades["buy_orders"] += 1
        else:
            trades["sell_orders"] += 1

    def producer_flow() -> simpy.events.Event:
        while float(env.now) < horizon_hours:
            yield env.timeout(rng.expovariate(1.0 / 0.85))
            produced = max(6.0, rng.gauss(60.0, 12.0))
            inventory[0] += produced
            _apply_order(side="sell", quantity=produced * 0.55, aggressiveness=0.9)

    def industry_flow() -> simpy.events.Event:
        while float(env.now) < horizon_hours:
            yield env.timeout(rng.expovariate(1.0 / 0.72))
            demand = max(4.0, rng.gauss(54.0, 11.5))
            fill = min(inventory[0], demand)
            inventory[0] -= fill
            unmet = max(0.0, demand - fill)
            if unmet > 0.1:
                trades["shortage_events"] += 1
            _apply_order(side="buy", quantity=demand, aggressiveness=1.1 + (unmet / max(1.0, demand)))

    def arbitrage_flow() -> simpy.events.Event:
        while float(env.now) < horizon_hours:
            yield env.timeout(0.5)
            mispricing = (price[0] - anchor_price) / max(0.0001, anchor_price)
            if abs(mispricing) < 0.018:
                continue
            notional = 75.0 + (abs(mispricing) * 1900.0)
            if mispricing > 0:
                _apply_order(side="sell", quantity=notional, aggressiveness=1.35)
                arbitrage_profit_credits[0] += abs(mispricing) * notional * 0.38
            else:
                _apply_order(side="buy", quantity=notional, aggressiveness=1.35)
                arbitrage_profit_credits[0] += abs(mispricing) * notional * 0.34

    def snapshot_flow() -> simpy.events.Event:
        while float(env.now) < horizon_hours:
            yield env.timeout(1.0)
            price_history.append(
                {
                    "hour": round(float(env.now), 3),
                    "price": round(price[0], 6),
                    "inventory": round(inventory[0], 4),
                }
            )

    env.process(producer_flow())
    env.process(industry_flow())
    env.process(arbitrage_flow())
    env.process(snapshot_flow())
    env.run(until=float(horizon_hours))

    prices = [row["price"] for row in price_history] if price_history else [price[0]]
    avg_price = statistics.fmean(prices) if prices else price[0]
    stdev_price = statistics.pstdev(prices) if len(prices) > 1 else 0.0
    volatility = statistics.pstdev(returns) * math.sqrt(24.0) if len(returns) > 1 else 0.0
    return {
        "horizon_hours": round(float(horizon_hours), 3),
        "anchor_price": round(anchor_price, 6),
        "final_price": round(price[0], 6),
        "avg_price": round(avg_price, 6),
        "stdev_price": round(stdev_price, 6),
        "annualized_like_volatility_day": round(volatility, 6),
        "min_price": round(min(prices), 6),
        "max_price": round(max(prices), 6),
        "inventory_end": round(inventory[0], 4),
        "trades": trades,
        "arbitrage_profit_credits": round(arbitrage_profit_credits[0], 4),
        "samples": {
            "points": len(price_history),
            "first5": price_history[:5],
            "last5": price_history[-5:],
        },
    }


def _simulate_body_extraction(
    *,
    rng: random.Random,
    horizon_hours: float,
    body_class: str,
    depletable: bool,
    reserve_units: float | None,
    harvest_rate_u_per_h: float,
    trip_hours: float,
    ships: int,
    ship_capacity: float,
    price_per_unit: float,
) -> dict[str, Any]:
    env = simpy.Environment()
    buffer_units = [0.0]
    mined_units = [0.0]
    delivered_units = [0.0]
    credits = [0.0]
    buffer_peak = [0.0]
    depletion_hour = [None]
    remaining = [float(reserve_units)] if reserve_units is not None else [None]

    def miner() -> simpy.events.Event:
        while float(env.now) < horizon_hours:
            yield env.timeout(rng.uniform(0.2, 1.1))
            mined = max(0.0, harvest_rate_u_per_h * rng.uniform(0.72, 1.3))
            if depletable and remaining[0] is not None:
                if remaining[0] <= 0.0:
                    if depletion_hour[0] is None:
                        depletion_hour[0] = float(env.now)
                    break
                mined = min(mined, remaining[0])
                remaining[0] -= mined
                if remaining[0] <= 0.0 and depletion_hour[0] is None:
                    depletion_hour[0] = float(env.now)
            mined_units[0] += mined
            buffer_units[0] += mined
            buffer_peak[0] = max(buffer_peak[0], buffer_units[0])

    def hauler(ship_idx: int) -> simpy.events.Event:
        _ = ship_idx
        while float(env.now) < horizon_hours:
            if buffer_units[0] <= 0.04:
                yield env.timeout(0.35)
                continue
            load = min(
                buffer_units[0],
                max(12.0, ship_capacity * rng.uniform(0.78, 1.1)),
            )
            if load <= 0:
                yield env.timeout(0.2)
                continue
            buffer_units[0] -= load
            yield env.timeout(max(0.1, trip_hours))
            recovered = load * rng.uniform(0.93, 0.995)
            delivered_units[0] += recovered
            credits[0] += recovered * price_per_unit
            yield env.timeout(max(0.1, trip_hours))

    env.process(miner())
    for ship_idx in range(max(1, ships)):
        env.process(hauler(ship_idx))
    env.run(until=float(horizon_hours))

    return {
        "body_class": body_class,
        "is_depletable": bool(depletable),
        "reserve_units_start": round(float(reserve_units), 4) if reserve_units is not None else None,
        "reserve_units_remaining": (
            round(float(remaining[0]), 4)
            if isinstance(remaining[0], (int, float))
            else None
        ),
        "depletion_hour": round(float(depletion_hour[0]), 4) if isinstance(depletion_hour[0], float) else None,
        "mined_units": round(mined_units[0], 4),
        "delivered_units": round(delivered_units[0], 4),
        "buffer_units_end": round(buffer_units[0], 4),
        "buffer_peak_units": round(buffer_peak[0], 4),
        "credits_generated": round(credits[0], 4),
    }


def _run_extraction_logistics_model(
    *,
    rng: random.Random,
    horizon_hours: float,
    market_price_anchor: float,
) -> dict[str, Any]:
    body_rows = [
        ("asteroid", True, 42_000.0, 215.0, 2.2, 3, 360.0, market_price_anchor * 0.96),
        ("comet", True, 37_500.0, 192.0, 2.8, 3, 340.0, market_price_anchor * 1.03),
        ("moon", False, None, 248.0, 3.4, 4, 420.0, market_price_anchor * 0.91),
        ("planet", False, None, 332.0, 4.2, 5, 560.0, market_price_anchor * 0.88),
        ("gas_giant", False, None, 468.0, 4.6, 5, 620.0, market_price_anchor * 0.84),
        ("star", False, None, 602.0, 5.6, 6, 720.0, market_price_anchor * 0.79),
    ]
    rows: list[dict[str, Any]] = []
    for body in body_rows:
        rows.append(
            _simulate_body_extraction(
                rng=rng,
                horizon_hours=horizon_hours,
                body_class=body[0],
                depletable=body[1],
                reserve_units=body[2],
                harvest_rate_u_per_h=body[3],
                trip_hours=body[4],
                ships=body[5],
                ship_capacity=body[6],
                price_per_unit=body[7],
            )
        )
    delivered_total = sum(float(row["delivered_units"]) for row in rows)
    credits_total = sum(float(row["credits_generated"]) for row in rows)
    craftable_module_batches = delivered_total / 420.0
    research_points_equivalent = delivered_total * 0.084
    return {
        "horizon_hours": round(float(horizon_hours), 3),
        "class_results": rows,
        "totals": {
            "delivered_units": round(delivered_total, 4),
            "credits_generated": round(credits_total, 4),
            "craftable_module_batches_eq": round(craftable_module_batches, 4),
            "research_points_equivalent": round(research_points_equivalent, 4),
        },
    }


def run_simpy_timeflow(*, seed: int, profile: str, config: dict[str, Any] | None = None) -> dict[str, Any]:
    """Run integrated SimPy scenarios used for long-horizon balancing checks."""
    config = dict(config) if isinstance(config, dict) else {}
    horizon_hours = (
        float(config.get("simpy_horizon_hours", 720.0))
        if str(profile).casefold() == "long"
        else float(config.get("simpy_horizon_hours", 240.0))
    )
    players = (
        int(config.get("simpy_players", 128))
        if str(profile).casefold() == "long"
        else int(config.get("simpy_players", 48))
    )
    master_rng = random.Random(seed ^ _stable_hash_int("simpy_timeflow", profile))
    queue_rng = random.Random(master_rng.randrange(1, 2**31 - 1))
    market_rng = random.Random(master_rng.randrange(1, 2**31 - 1))
    extraction_rng = random.Random(master_rng.randrange(1, 2**31 - 1))

    queue = _run_queue_model(
        rng=queue_rng,
        horizon_hours=horizon_hours,
        players=players,
        config=config,
    )
    market = _run_market_model(
        rng=market_rng,
        horizon_hours=horizon_hours,
        config=config,
    )
    extraction = _run_extraction_logistics_model(
        rng=extraction_rng,
        horizon_hours=horizon_hours,
        market_price_anchor=float(market.get("avg_price", 100.0)),
    )
    supply_pressure_index = float(extraction["totals"]["delivered_units"]) / max(
        1.0,
        float(queue["manufacturing"]["completed_jobs"]) * 160.0,
    )
    queue_stress_index = (
        float(queue["research"]["backlog_jobs"]) + float(queue["manufacturing"]["backlog_jobs"])
    ) / max(1.0, float(players))
    return {
        "seed": int(seed),
        "profile": str(profile),
        "queue_dynamics": queue,
        "market_dynamics": market,
        "extraction_logistics": extraction,
        "cross_effects": {
            "supply_pressure_index": round(supply_pressure_index, 6),
            "queue_stress_index_jobs_per_player": round(queue_stress_index, 6),
            "market_price_vs_anchor_ratio": round(
                float(market["final_price"]) / max(0.0001, float(market["anchor_price"])),
                6,
            ),
        },
    }

