from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query, Response, status

from app.api_limits import (
    get_api_engine,
    page_limit,
    page_offset,
    paginate,
    set_page_headers,
)

from app.searches.models import (
    ListingEvaluationRecord,
    SavedSearchCreate,
    SavedSearchRecord,
    SavedSearchUpdate,
    SearchEvaluationRun,
)
from app.searches.service import (
    SavedSearchNameConflictError,
    SavedSearchNotFoundError,
    create_saved_search,
    delete_saved_search,
    evaluate_saved_search,
    get_saved_search,
    list_saved_searches,
    list_search_evaluations,
    update_saved_search,
)


router = APIRouter(prefix="/saved-searches", tags=["saved searches"])


@router.post(
    "",
    response_model=SavedSearchRecord,
    status_code=status.HTTP_201_CREATED,
)
def create_search(payload: SavedSearchCreate) -> SavedSearchRecord:
    try:
        return create_saved_search(payload, get_api_engine())
    except SavedSearchNameConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.get("", response_model=list[SavedSearchRecord])
def list_searches(
    response: Response,
    limit: int = page_limit(),
    offset: int = page_offset(),
) -> list[SavedSearchRecord]:
    rows = list_saved_searches(limit=limit + 1, offset=offset, engine=get_api_engine())
    rows, has_more = paginate(rows, limit)
    set_page_headers(response, limit=limit, offset=offset, has_more=has_more)
    return rows


@router.get("/{saved_search_id}", response_model=SavedSearchRecord)
def get_search(saved_search_id: int) -> SavedSearchRecord:
    try:
        return get_saved_search(saved_search_id, get_api_engine())
    except SavedSearchNotFoundError as exc:
        raise HTTPException(status_code=404, detail="Saved search not found") from exc


@router.patch("/{saved_search_id}", response_model=SavedSearchRecord)
def update_search(
    saved_search_id: int,
    payload: SavedSearchUpdate,
) -> SavedSearchRecord:
    try:
        return update_saved_search(saved_search_id, payload, get_api_engine())
    except SavedSearchNotFoundError as exc:
        raise HTTPException(status_code=404, detail="Saved search not found") from exc
    except SavedSearchNameConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.delete("/{saved_search_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_search(saved_search_id: int) -> Response:
    try:
        delete_saved_search(saved_search_id, get_api_engine())
    except SavedSearchNotFoundError as exc:
        raise HTTPException(status_code=404, detail="Saved search not found") from exc
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.post(
    "/{saved_search_id}/evaluate",
    response_model=SearchEvaluationRun,
)
def evaluate_search(
    saved_search_id: int,
    limit: int = Query(1000, ge=1, le=10000),
) -> SearchEvaluationRun:
    try:
        return evaluate_saved_search(
            saved_search_id, limit=limit, engine=get_api_engine()
        )
    except SavedSearchNotFoundError as exc:
        raise HTTPException(status_code=404, detail="Saved search not found") from exc


@router.get(
    "/{saved_search_id}/evaluations",
    response_model=list[ListingEvaluationRecord],
)
def list_evaluations(
    saved_search_id: int,
    response: Response,
    limit: int = page_limit(),
    offset: int = page_offset(),
) -> list[ListingEvaluationRecord]:
    try:
        rows = list_search_evaluations(
            saved_search_id,
            limit=limit + 1,
            offset=offset,
            engine=get_api_engine(),
        )
        rows, has_more = paginate(rows, limit)
        set_page_headers(response, limit=limit, offset=offset, has_more=has_more)
        return rows
    except SavedSearchNotFoundError as exc:
        raise HTTPException(status_code=404, detail="Saved search not found") from exc
