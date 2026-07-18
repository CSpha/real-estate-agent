from __future__ import annotations

import hashlib
import json
from typing import Any

from sqlalchemy import Engine, text
from sqlalchemy.exc import IntegrityError

from app.db import get_engine
from app.searches.evaluator import evaluate_listing
from app.searches.models import (
    EvaluationReason,
    ListingCandidate,
    ListingEvaluationRecord,
    SavedSearchCreate,
    SavedSearchRecord,
    SavedSearchUpdate,
    SearchCriteria,
    SearchEvaluationRun,
)


class SavedSearchNotFoundError(LookupError):
    pass


class SavedSearchNameConflictError(ValueError):
    pass


SEARCH_COLUMNS = """
    id,
    name,
    description,
    criteria_json,
    criteria_version,
    enabled,
    notification_mode,
    cooldown_minutes,
    created_at,
    updated_at
"""

CREATE_SEARCH_SQL = text(
    f"""
    INSERT INTO saved_searches (
        name,
        description,
        criteria_json,
        enabled,
        notification_mode,
        cooldown_minutes
    )
    VALUES (
        :name,
        :description,
        CAST(:criteria_json AS JSONB),
        :enabled,
        :notification_mode,
        :cooldown_minutes
    )
    RETURNING {SEARCH_COLUMNS}
    """
)

GET_SEARCH_SQL = text(
    f"""
    SELECT {SEARCH_COLUMNS}
    FROM saved_searches
    WHERE id = :saved_search_id
    """
)

GET_SEARCH_FOR_LOCK_SQL = text(
    f"""
    SELECT {SEARCH_COLUMNS}
    FROM saved_searches
    WHERE id = :saved_search_id
    FOR UPDATE
    """
)

LIST_SEARCHES_SQL = text(
    f"""
    SELECT {SEARCH_COLUMNS}
    FROM saved_searches
    ORDER BY name, id
    LIMIT :limit OFFSET :offset
    """
)

UPDATE_SEARCH_SQL = text(
    f"""
    UPDATE saved_searches
    SET
        name = :name,
        description = :description,
        criteria_version = CASE
            WHEN criteria_json IS DISTINCT FROM CAST(:criteria_json AS JSONB)
                THEN criteria_version + 1
            ELSE criteria_version
        END,
        criteria_json = CAST(:criteria_json AS JSONB),
        enabled = :enabled,
        notification_mode = :notification_mode,
        cooldown_minutes = :cooldown_minutes,
        updated_at = CURRENT_TIMESTAMP
    WHERE id = :saved_search_id
    RETURNING {SEARCH_COLUMNS}
    """
)

LISTING_CANDIDATES_SQL = text(
    """
    SELECT
        listing.source,
        listing.source_listing_id,
        listing.address,
        listing.city,
        location.county_name,
        listing.state,
        listing.zip,
        listing.list_price,
        previous.previous_price,
        listing.beds,
        listing.baths,
        listing.sqft,
        listing.lot_size_acres,
        listing.property_type,
        listing.status,
        listing.days_on_market,
        score.market_score,
        listing.updated_at AS listing_state_timestamp
    FROM listings_current listing
    LEFT JOIN city_county_lookup location
      ON LOWER(listing.city) = LOWER(location.city)
     AND UPPER(listing.state) = UPPER(location.state)
    LEFT JOIN listing_market_scores score
      ON score.source = listing.source
     AND score.source_listing_id = listing.source_listing_id
    LEFT JOIN LATERAL (
        SELECT history.list_price AS previous_price
        FROM listing_history history
        WHERE history.source = listing.source
          AND history.source_listing_id = listing.source_listing_id
          AND history.list_price IS DISTINCT FROM listing.list_price
        ORDER BY history.snapshot_timestamp DESC, history.id DESC
        LIMIT 1
    ) previous ON TRUE
    ORDER BY listing.id
    LIMIT :limit
    """
)

PREVIOUS_EVALUATIONS_SQL = text(
    """
    SELECT DISTINCT ON (source, source_listing_id)
        source,
        source_listing_id,
        is_match
    FROM listing_search_evaluations
    WHERE saved_search_id = :saved_search_id
      AND criteria_version = :criteria_version
    ORDER BY
        source,
        source_listing_id,
        evaluated_at DESC,
        id DESC
    """
)

INSERT_EVALUATION_SQL = text(
    """
    INSERT INTO listing_search_evaluations (
        saved_search_id,
        criteria_version,
        source,
        source_listing_id,
        listing_fingerprint,
        event_fingerprint,
        is_match,
        became_match,
        match_reasons,
        failed_reasons,
        listing_state_timestamp
    )
    VALUES (
        :saved_search_id,
        :criteria_version,
        :source,
        :source_listing_id,
        :listing_fingerprint,
        :event_fingerprint,
        :is_match,
        :became_match,
        CAST(:match_reasons AS JSONB),
        CAST(:failed_reasons AS JSONB),
        :listing_state_timestamp
    )
    ON CONFLICT (event_fingerprint)
    DO NOTHING
    RETURNING id, became_match, evaluated_at
    """
)

GET_EVALUATION_BY_FINGERPRINT_SQL = text(
    """
    SELECT id, became_match, evaluated_at
    FROM listing_search_evaluations
    WHERE event_fingerprint = :event_fingerprint
    """
)


def create_saved_search(
    payload: SavedSearchCreate,
    engine: Engine | None = None,
) -> SavedSearchRecord:
    engine = engine or get_engine()
    values = {
        "name": payload.name,
        "description": payload.description,
        "criteria_json": _criteria_json(payload.criteria),
        "enabled": payload.enabled,
        "notification_mode": payload.notification_mode,
        "cooldown_minutes": payload.cooldown_minutes,
    }
    try:
        with engine.begin() as conn:
            row = conn.execute(CREATE_SEARCH_SQL, values).mappings().one()
    except IntegrityError as exc:
        raise SavedSearchNameConflictError(
            f"A saved search named {payload.name!r} already exists"
        ) from exc
    return _search_record(row)


def get_saved_search(
    saved_search_id: int,
    engine: Engine | None = None,
) -> SavedSearchRecord:
    engine = engine or get_engine()
    with engine.connect() as conn:
        row = (
            conn.execute(
                GET_SEARCH_SQL,
                {"saved_search_id": saved_search_id},
            )
            .mappings()
            .first()
        )
    if row is None:
        raise SavedSearchNotFoundError(saved_search_id)
    return _search_record(row)


def list_saved_searches(
    *,
    limit: int = 100,
    offset: int = 0,
    engine: Engine | None = None,
) -> list[SavedSearchRecord]:
    engine = engine or get_engine()
    with engine.connect() as conn:
        rows = (
            conn.execute(
                LIST_SEARCHES_SQL,
                {"limit": limit, "offset": offset},
            )
            .mappings()
            .all()
        )
    return [_search_record(row) for row in rows]


def update_saved_search(
    saved_search_id: int,
    payload: SavedSearchUpdate,
    engine: Engine | None = None,
) -> SavedSearchRecord:
    engine = engine or get_engine()
    try:
        with engine.begin() as conn:
            current_row = (
                conn.execute(
                    GET_SEARCH_FOR_LOCK_SQL,
                    {"saved_search_id": saved_search_id},
                )
                .mappings()
                .first()
            )
            if current_row is None:
                raise SavedSearchNotFoundError(saved_search_id)
            current = _search_record(current_row)
            fields = payload.model_fields_set
            criteria = payload.criteria if "criteria" in fields else current.criteria
            values = {
                "saved_search_id": saved_search_id,
                "name": payload.name if "name" in fields else current.name,
                "description": (
                    payload.description
                    if "description" in fields
                    else current.description
                ),
                "criteria_json": _criteria_json(criteria),
                "enabled": (
                    payload.enabled if "enabled" in fields else current.enabled
                ),
                "notification_mode": (
                    payload.notification_mode
                    if "notification_mode" in fields
                    else current.notification_mode
                ),
                "cooldown_minutes": (
                    payload.cooldown_minutes
                    if "cooldown_minutes" in fields
                    else current.cooldown_minutes
                ),
            }
            row = conn.execute(UPDATE_SEARCH_SQL, values).mappings().first()
    except IntegrityError as exc:
        raise SavedSearchNameConflictError(
            f"A saved search named {values['name']!r} already exists"
        ) from exc
    if row is None:
        raise SavedSearchNotFoundError(saved_search_id)
    return _search_record(row)


def delete_saved_search(
    saved_search_id: int,
    engine: Engine | None = None,
) -> None:
    engine = engine or get_engine()
    with engine.begin() as conn:
        result = conn.execute(
            text("DELETE FROM saved_searches WHERE id = :saved_search_id"),
            {"saved_search_id": saved_search_id},
        )
    if result.rowcount != 1:
        raise SavedSearchNotFoundError(saved_search_id)


def evaluate_saved_search(
    saved_search_id: int,
    *,
    limit: int = 1000,
    engine: Engine | None = None,
) -> SearchEvaluationRun:
    engine = engine or get_engine()

    results: list[ListingEvaluationRecord] = []
    matched = 0
    new_matches = 0
    recorded_count = 0

    with engine.begin() as conn:
        search_row = (
            conn.execute(
                GET_SEARCH_FOR_LOCK_SQL,
                {"saved_search_id": saved_search_id},
            )
            .mappings()
            .first()
        )
        if search_row is None:
            raise SavedSearchNotFoundError(saved_search_id)
        search = _search_record(search_row)
        candidate_rows = (
            conn.execute(
                LISTING_CANDIDATES_SQL,
                {"limit": limit},
            )
            .mappings()
            .all()
        )
        previous_rows = (
            conn.execute(
                PREVIOUS_EVALUATIONS_SQL,
                {
                    "saved_search_id": saved_search_id,
                    "criteria_version": search.criteria_version,
                },
            )
            .mappings()
            .all()
        )
        previous_matches = {
            (row["source"], row["source_listing_id"]): row["is_match"]
            for row in previous_rows
        }

        for candidate_row in candidate_rows:
            candidate = ListingCandidate.model_validate(candidate_row)
            evaluation = evaluate_listing(search.criteria, candidate)
            listing_fingerprint = _fingerprint(candidate.model_dump(mode="json"))
            event_fingerprint = _fingerprint(
                {
                    "event_type": "saved_search_evaluation",
                    "saved_search_id": saved_search_id,
                    "criteria_version": search.criteria_version,
                    "source": candidate.source,
                    "source_listing_id": candidate.source_listing_id,
                    "listing_fingerprint": listing_fingerprint,
                }
            )
            key = (candidate.source, candidate.source_listing_id)
            became_match = evaluation.is_match and not previous_matches.get(key, False)
            values = {
                "saved_search_id": saved_search_id,
                "criteria_version": search.criteria_version,
                "source": candidate.source,
                "source_listing_id": candidate.source_listing_id,
                "listing_fingerprint": listing_fingerprint,
                "event_fingerprint": event_fingerprint,
                "is_match": evaluation.is_match,
                "became_match": became_match,
                "match_reasons": _json_dump(evaluation.match_reasons),
                "failed_reasons": _json_dump(evaluation.failed_reasons),
                "listing_state_timestamp": candidate.listing_state_timestamp,
            }
            inserted = (
                conn.execute(
                    INSERT_EVALUATION_SQL,
                    values,
                )
                .mappings()
                .first()
            )
            recorded = inserted is not None
            persisted = inserted
            if persisted is None:
                persisted = (
                    conn.execute(
                        GET_EVALUATION_BY_FINGERPRINT_SQL,
                        {"event_fingerprint": event_fingerprint},
                    )
                    .mappings()
                    .one()
                )

            persisted_became_match = persisted["became_match"]
            if evaluation.is_match:
                matched += 1
            if recorded:
                recorded_count += 1
                previous_matches[key] = evaluation.is_match
                if persisted_became_match:
                    new_matches += 1

            results.append(
                ListingEvaluationRecord(
                    id=persisted["id"],
                    source=candidate.source,
                    source_listing_id=candidate.source_listing_id,
                    address=candidate.address,
                    criteria_version=search.criteria_version,
                    listing_fingerprint=listing_fingerprint,
                    event_fingerprint=event_fingerprint,
                    is_match=evaluation.is_match,
                    became_match=persisted_became_match,
                    match_reasons=evaluation.match_reasons,
                    failed_reasons=evaluation.failed_reasons,
                    listing_state_timestamp=candidate.listing_state_timestamp,
                    evaluated_at=persisted["evaluated_at"],
                    recorded=recorded,
                )
            )

    return SearchEvaluationRun(
        saved_search_id=saved_search_id,
        criteria_version=search.criteria_version,
        evaluated=len(results),
        matched=matched,
        new_matches=new_matches,
        recorded=recorded_count,
        results=results,
    )


def evaluate_enabled_searches(
    *,
    limit_per_search: int = 1000,
    engine: Engine | None = None,
) -> list[SearchEvaluationRun]:
    engine = engine or get_engine()
    with engine.connect() as conn:
        saved_search_ids = conn.scalars(
            text(
                """
                SELECT id
                FROM saved_searches
                WHERE enabled = TRUE
                ORDER BY id
                """
            )
        ).all()

    return [
        evaluate_saved_search(
            saved_search_id,
            limit=limit_per_search,
            engine=engine,
        )
        for saved_search_id in saved_search_ids
    ]


def list_search_evaluations(
    saved_search_id: int,
    *,
    limit: int = 100,
    offset: int = 0,
    engine: Engine | None = None,
) -> list[ListingEvaluationRecord]:
    engine = engine or get_engine()
    get_saved_search(saved_search_id, engine)
    query = text(
        """
        SELECT
            evaluation.id,
            evaluation.source,
            evaluation.source_listing_id,
            listing.address,
            evaluation.criteria_version,
            evaluation.listing_fingerprint,
            evaluation.event_fingerprint,
            evaluation.is_match,
            evaluation.became_match,
            evaluation.match_reasons,
            evaluation.failed_reasons,
            evaluation.listing_state_timestamp,
            evaluation.evaluated_at
        FROM listing_search_evaluations evaluation
        LEFT JOIN listings_current listing
          ON listing.source = evaluation.source
         AND listing.source_listing_id = evaluation.source_listing_id
        WHERE evaluation.saved_search_id = :saved_search_id
        ORDER BY evaluation.evaluated_at DESC, evaluation.id DESC
        LIMIT :limit OFFSET :offset
        """
    )
    with engine.connect() as conn:
        rows = (
            conn.execute(
                query,
                {
                    "saved_search_id": saved_search_id,
                    "limit": limit,
                    "offset": offset,
                },
            )
            .mappings()
            .all()
        )

    return [
        ListingEvaluationRecord(
            id=row["id"],
            source=row["source"],
            source_listing_id=row["source_listing_id"],
            address=row["address"],
            criteria_version=row["criteria_version"],
            listing_fingerprint=row["listing_fingerprint"],
            event_fingerprint=row["event_fingerprint"],
            is_match=row["is_match"],
            became_match=row["became_match"],
            match_reasons=[
                EvaluationReason.model_validate(reason)
                for reason in row["match_reasons"]
            ],
            failed_reasons=[
                EvaluationReason.model_validate(reason)
                for reason in row["failed_reasons"]
            ],
            listing_state_timestamp=row["listing_state_timestamp"],
            evaluated_at=row["evaluated_at"],
            recorded=True,
        )
        for row in rows
    ]


def _search_record(row: Any) -> SavedSearchRecord:
    criteria_raw = row["criteria_json"]
    if isinstance(criteria_raw, str):
        criteria_raw = json.loads(criteria_raw)
    return SavedSearchRecord(
        id=row["id"],
        name=row["name"],
        description=row["description"],
        criteria=SearchCriteria.model_validate(criteria_raw),
        criteria_version=row["criteria_version"],
        enabled=row["enabled"],
        notification_mode=row["notification_mode"],
        cooldown_minutes=row["cooldown_minutes"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def _criteria_json(criteria: SearchCriteria | None) -> str:
    if criteria is None:
        raise ValueError("criteria cannot be null")
    return json.dumps(
        criteria.model_dump(mode="json", exclude_none=True),
        sort_keys=True,
        separators=(",", ":"),
    )


def _json_dump(values: Any) -> str:
    if isinstance(values, list):
        values = [
            value.model_dump(mode="json") if hasattr(value, "model_dump") else value
            for value in values
        ]
    return json.dumps(values, sort_keys=True, separators=(",", ":"))


def _fingerprint(value: Any) -> str:
    canonical = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()
