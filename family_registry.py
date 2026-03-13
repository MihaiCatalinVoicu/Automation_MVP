from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from db import upsert_family_registry_entry


@dataclass(frozen=True)
class FamilyDefinition:
    family_id: str
    generator_type: str
    strategy_id: str
    setup_name: str | None
    data_requirements: tuple[str, ...]
    allowed_validation_levels: tuple[str, ...]
    batch_defaults: dict[str, int]
    status: str
    priority: int
    maturity: str
    notes: str = ""


_BATCH_DEFAULTS = {"cheap": 12, "medium": 6, "expensive": 1}


FAMILY_DEFINITIONS: tuple[FamilyDefinition, ...] = (
    FamilyDefinition(
        family_id="trend_volatility_expansion",
        generator_type="symbol_local",
        strategy_id="trend_volatility_expansion",
        setup_name="TrendVolExpansion",
        data_requirements=("ohlcv", "btc_regime_features"),
        allowed_validation_levels=("cheap", "medium", "expensive"),
        batch_defaults=_BATCH_DEFAULTS,
        status="active",
        priority=100,
        maturity="experimental",
        notes="Primary edge-discovery family for compression-to-breakout research.",
    ),
    FamilyDefinition(
        family_id="relative_strength_rotation",
        generator_type="cross_sectional",
        strategy_id="relative_strength_rotation",
        setup_name="RelativeStrengthRotation",
        data_requirements=("ohlcv", "btc_regime_features"),
        allowed_validation_levels=("cheap", "medium", "expensive"),
        batch_defaults=_BATCH_DEFAULTS,
        status="active",
        priority=95,
        maturity="experimental",
        notes="Cross-sectional rotation family for liquid alt leadership regimes.",
    ),
    FamilyDefinition(
        family_id="pullback_in_trend",
        generator_type="symbol_local",
        strategy_id="pullback_in_trend",
        setup_name="Pullback",
        data_requirements=("ohlcv", "btc_regime_features"),
        allowed_validation_levels=("cheap", "medium", "expensive"),
        batch_defaults=_BATCH_DEFAULTS,
        status="active",
        priority=90,
        maturity="experimental",
        notes="Trend-persistence family focused on cleaner pullback entries.",
    ),
    FamilyDefinition(
        family_id="breakout_momentum",
        generator_type="symbol_local",
        strategy_id="breakout_momentum",
        setup_name="Breakout",
        data_requirements=("ohlcv", "btc_regime_features"),
        allowed_validation_levels=("cheap", "medium", "expensive"),
        batch_defaults=_BATCH_DEFAULTS,
        status="active",
        priority=75,
        maturity="canonical",
        notes="Existing breakout research family.",
    ),
    FamilyDefinition(
        family_id="cross_sectional_momentum",
        generator_type="cross_sectional",
        strategy_id="cross_sectional_momentum",
        setup_name="CrossSectionalMomentum",
        data_requirements=("ohlcv",),
        allowed_validation_levels=("cheap", "medium", "expensive"),
        batch_defaults=_BATCH_DEFAULTS,
        status="active",
        priority=70,
        maturity="canonical",
        notes="Existing cross-sectional baseline family.",
    ),
    FamilyDefinition(
        family_id="spike_mean_reversion",
        generator_type="symbol_local",
        strategy_id="spike_mean_reversion",
        setup_name="SpikeMR",
        data_requirements=("ohlcv",),
        allowed_validation_levels=("cheap", "medium", "expensive"),
        batch_defaults=_BATCH_DEFAULTS,
        status="active",
        priority=60,
        maturity="canonical",
        notes="Existing mean-reversion family retained for comparison.",
    ),
    FamilyDefinition(
        family_id="pullback",
        generator_type="symbol_local",
        strategy_id="pullback",
        setup_name="Pullback",
        data_requirements=("ohlcv", "btc_regime_features"),
        allowed_validation_levels=("cheap", "medium", "expensive"),
        batch_defaults=_BATCH_DEFAULTS,
        status="active",
        priority=55,
        maturity="experimental",
    ),
    FamilyDefinition(
        family_id="pullback_v2",
        generator_type="symbol_local",
        strategy_id="pullback_v2",
        setup_name="PullbackV2",
        data_requirements=("ohlcv", "btc_regime_features"),
        allowed_validation_levels=("cheap", "medium", "expensive"),
        batch_defaults=_BATCH_DEFAULTS,
        status="active",
        priority=55,
        maturity="experimental",
    ),
    FamilyDefinition(
        family_id="trend_reclaim",
        generator_type="symbol_local",
        strategy_id="trend_reclaim",
        setup_name="PullbackV2",
        data_requirements=("ohlcv", "btc_regime_features"),
        allowed_validation_levels=("cheap", "medium", "expensive"),
        batch_defaults=_BATCH_DEFAULTS,
        status="active",
        priority=50,
        maturity="experimental",
    ),
    FamilyDefinition(
        family_id="oi_cascade",
        generator_type="symbol_local",
        strategy_id="oi_cascade",
        setup_name="OICascade",
        data_requirements=("ohlcv", "btc_regime_features"),
        allowed_validation_levels=("cheap", "medium"),
        batch_defaults=_BATCH_DEFAULTS,
        status="active",
        priority=45,
        maturity="experimental",
    ),
    FamilyDefinition(
        family_id="btc_structural_daily",
        generator_type="symbol_local",
        strategy_id="btc_structural_daily",
        setup_name=None,
        data_requirements=("ohlcv",),
        allowed_validation_levels=("cheap", "medium"),
        batch_defaults=_BATCH_DEFAULTS,
        status="active",
        priority=40,
        maturity="experimental",
    ),
    FamilyDefinition(
        family_id="funding_rate_mean_reversion",
        generator_type="symbol_local",
        strategy_id="funding_rate_mean_reversion",
        setup_name=None,
        data_requirements=("ohlcv", "funding_rate_history"),
        allowed_validation_levels=("cheap",),
        batch_defaults=_BATCH_DEFAULTS,
        status="deferred",
        priority=10,
        maturity="deferred",
        notes="Deferred until funding-rate history and carry accounting exist.",
    ),
)


def list_family_definitions(*, statuses: set[str] | None = None) -> list[FamilyDefinition]:
    rows = list(FAMILY_DEFINITIONS)
    if statuses is None:
        return rows
    return [item for item in rows if item.status in statuses]


def get_family_definition(family_id: str) -> FamilyDefinition | None:
    for item in FAMILY_DEFINITIONS:
        if item.family_id == family_id:
            return item
    return None


def allowed_family_ids(*, include_experimental: bool = True) -> set[str]:
    allowed_statuses = {"active"}
    if include_experimental:
        allowed_statuses.add("experimental")
    return {item.family_id for item in list_family_definitions(statuses=allowed_statuses)}


def sync_family_registry_db() -> None:
    for item in FAMILY_DEFINITIONS:
        upsert_family_registry_entry(
            family_id=item.family_id,
            generator_type=item.generator_type,
            strategy_id=item.strategy_id,
            setup_name=item.setup_name,
            data_requirements=list(item.data_requirements),
            allowed_validation_levels=list(item.allowed_validation_levels),
            batch_defaults=dict(item.batch_defaults),
            status=item.status,
            priority=item.priority,
            maturity=item.maturity,
            notes=item.notes,
        )


def family_batch_size(family_id: str, validation_level: str) -> int:
    item = get_family_definition(family_id)
    if item is None:
        return _BATCH_DEFAULTS.get(validation_level, 1)
    return int(item.batch_defaults.get(validation_level, _BATCH_DEFAULTS.get(validation_level, 1)))


def as_dict(item: FamilyDefinition) -> dict[str, Any]:
    return {
        "family_id": item.family_id,
        "generator_type": item.generator_type,
        "strategy_id": item.strategy_id,
        "setup_name": item.setup_name,
        "data_requirements": list(item.data_requirements),
        "allowed_validation_levels": list(item.allowed_validation_levels),
        "batch_defaults": dict(item.batch_defaults),
        "status": item.status,
        "priority": item.priority,
        "maturity": item.maturity,
        "notes": item.notes,
    }
