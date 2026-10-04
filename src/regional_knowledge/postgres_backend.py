from __future__ import annotations

import asyncio
import json
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any, AsyncIterator
from urllib.parse import urlparse
from uuid import UUID

from psycopg import sql
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from psycopg_pool import AsyncConnectionPool

from .contracts import Principal
from .object_store import ObjectStore
from .supabase_backend import (
    Embedder,
    SupabaseConfig,
    SupabaseRestBackend,
)


@dataclass(slots=True)
class _DbResponse:
    payload: Any = None

    @property
    def content(self) -> bytes:
        if self.payload in (None, [], {}):
            return b""
        return b"1"

    def json(self) -> Any:
        return self.payload

    def raise_for_status(self) -> None:
        return None


_JSON_COLUMNS = {
    "authors",
    "bbox",
    "metadata",
    "rights_evidence",
    "warnings",
    "payload",
    "profile_evidence",
    "evidence",
}
_UUID_ARRAY_COLUMNS = {
    "region_ids",
    "page_ids",
    "illustration_ids",
    "footnote_region_ids",
    "caption_region_ids",
    "nearby_region_ids",
}
_UUID_COLUMNS = {
    "id",
    "owner_user_id",
    "user_id",
    "workspace_id",
    "document_id",
    "grantee_user_id",
    "page_id",
    "source_region_id",
    "target_region_id",
    "page_object_id",
    "crop_object_id",
    "text_object_id",
    "source_object_id",
    "staged_graph_object_id",
    "verified_by",
    "author_id",
    "event_id",
}
_ALLOWED_TABLES = {
    "rkb_documents",
    "rkb_document_grants",
    "rkb_ingestion_jobs",
    "rkb_objects",
    "rkb_pages",
    "rkb_regions",
    "rkb_region_relations",
    "rkb_illustrations",
    "rkb_chunks",
}


def _coerce_value(column: str, value: Any) -> Any:
    if value is None:
        return None
    if column in _JSON_COLUMNS:
        return Jsonb(value)
    if column in _UUID_ARRAY_COLUMNS:
        return [UUID(str(item)) for item in value]
    if column in _UUID_COLUMNS:
        return UUID(str(value))
    return value


def _filter_value(column: str, raw: str) -> Any:
    if not raw.startswith("eq."):
        raise ValueError("direct database adapter supports only eq filters")
    value = raw[3:]
    return _coerce_value(column, value)


def _split_select(value: str | None) -> list[str]:
    if not value:
        return ["*"]
    columns = [item.strip() for item in value.split(",") if item.strip()]
    if not columns:
        raise ValueError("empty select")
    for column in columns:
        if not column.replace("_", "").isalnum():
            raise ValueError("unsupported select expression")
    return columns


class PostgresDataClient:
    """Narrow internal data-plane adapter used by the existing business logic.

    It intentionally resembles the tiny subset of httpx/PostgREST calls already
    used by the service. No HTTP request is emitted: every call is translated to
    parameterized SQL over the Session Pooler.
    """

    def __init__(
        self,
        dsn: str,
        *,
        min_size: int = 1,
        max_size: int = 6,
    ) -> None:
        if not dsn:
            raise ValueError("Postgres DSN is required")
        self.pool = AsyncConnectionPool(
            conninfo=dsn,
            min_size=max(1, min_size),
            max_size=max(max(1, min_size), max_size),
            open=False,
            kwargs={"row_factory": dict_row},
        )
        self._open_lock = asyncio.Lock()

    async def _ensure_open(self) -> None:
        if not self.pool.closed:
            return
        async with self._open_lock:
            if self.pool.closed:
                await self.pool.open(wait=True)

    @staticmethod
    def _actor(headers: dict[str, str] | None) -> UUID | None:
        headers = headers or {}
        actor = headers.get("x-rkb-actor")
        if actor:
            return UUID(actor)
        if headers.get("x-rkb-service") == "1":
            return None
        raise RuntimeError("database actor context is required")

    @asynccontextmanager
    async def _connection(
        self,
        headers: dict[str, str] | None,
    ) -> AsyncIterator[Any]:
        await self._ensure_open()
        actor = self._actor(headers)
        async with self.pool.connection() as connection:
            async with connection.transaction():
                if actor is not None:
                    # Registry admission happens under the pool login role, before
                    # reducing privileges. A disabled UUID remains rejected.
                    row = await (
                        await connection.execute(
                            "select status from public.rkb_users where id = %s",
                            (actor,),
                        )
                    ).fetchone()
                    if row is None:
                        await connection.execute(
                            "insert into public.rkb_users(id) values (%s) "
                            "on conflict (id) do nothing",
                            (actor,),
                        )
                    elif row["status"] != "active":
                        raise PermissionError("regional knowledge account is disabled")

                    await connection.execute("set local role rkb_app")
                    await connection.execute(
                        "select set_config('rkb.actor_id', %s, true)",
                        (str(actor),),
                    )
                yield connection

    @staticmethod
    def _route(url: str) -> tuple[str, bool]:
        path = urlparse(url).path
        marker = "/rest/v1/"
        if marker not in path:
            raise ValueError("unsupported direct data-plane URL")
        tail = path.split(marker, 1)[1].strip("/")
        if tail.startswith("rpc/"):
            return tail[4:], True
        if tail not in _ALLOWED_TABLES:
            raise ValueError(f"table is not allowed: {tail}")
        return tail, False

    async def get(
        self,
        url: str,
        *,
        headers: dict[str, str] | None = None,
        params: dict[str, str] | None = None,
    ) -> _DbResponse:
        table, is_rpc = self._route(url)
        if is_rpc:
            raise ValueError("GET RPC is not supported")
        params = dict(params or {})
        columns = _split_select(params.pop("select", None))
        order = params.pop("order", None)
        limit_raw = params.pop("limit", None)
        limit = int(limit_raw) if limit_raw else None

        where_parts: list[Any] = []
        values: list[Any] = []
        for column, raw in params.items():
            if not column.replace("_", "").isalnum():
                raise ValueError("invalid filter column")
            where_parts.append(
                sql.SQL("{} = %s").format(sql.Identifier(column))
            )
            values.append(_filter_value(column, raw))

        if columns == ["*"]:
            query = sql.SQL("select * from public.{}").format(sql.Identifier(table))
        else:
            query = sql.SQL("select {} from public.{}").format(
                sql.SQL(", ").join(sql.Identifier(column) for column in columns),
                sql.Identifier(table),
            )
        if where_parts:
            query += sql.SQL(" where ") + sql.SQL(" and ").join(where_parts)
        if order:
            name, _, direction = order.partition(".")
            if not name.replace("_", "").isalnum() or direction not in {"asc", "desc"}:
                raise ValueError("invalid order")
            query += sql.SQL(" order by {} {}").format(
                sql.Identifier(name),
                sql.SQL(direction),
            )
        if limit is not None:
            if limit < 1 or limit > 1000:
                raise ValueError("invalid limit")
            query += sql.SQL(" limit %s")
            values.append(limit)

        async with self._connection(headers) as connection:
            cursor = await connection.execute(query, tuple(values))
            rows = await cursor.fetchall()
        return _DbResponse([dict(row) for row in rows])

    async def post(
        self,
        url: str,
        *,
        headers: dict[str, str] | None = None,
        json: Any = None,
    ) -> _DbResponse:
        target, is_rpc = self._route(url)
        if is_rpc:
            return await self._rpc(target, json or {}, headers)

        rows = json if isinstance(json, list) else [json]
        if any(not isinstance(row, dict) for row in rows):
            raise ValueError("insert payload must be an object or object list")
        if not rows:
            return _DbResponse([])

        prefer = (headers or {}).get("Prefer", "")
        upsert_grant = (
            target == "rkb_document_grants"
            and "resolution=merge-duplicates" in prefer
        )

        async with self._connection(headers) as connection:
            for row in rows:
                assert isinstance(row, dict)
                columns = list(row)
                if not columns:
                    continue
                values = [_coerce_value(column, row[column]) for column in columns]
                query = sql.SQL("insert into public.{} ({}) values ({})").format(
                    sql.Identifier(target),
                    sql.SQL(", ").join(sql.Identifier(column) for column in columns),
                    sql.SQL(", ").join(sql.Placeholder() for _ in columns),
                )
                if upsert_grant:
                    query += sql.SQL(
                        " on conflict (document_id, grantee_user_id) "
                        "do update set role = excluded.role"
                    )
                elif target == 'rkb_objects' and 'resolution=ignore-duplicates' in prefer:
                    query += sql.SQL(' on conflict (document_id,object_key) do nothing')
                await connection.execute(query, tuple(values))
        return _DbResponse([])

    async def patch(
        self,
        url: str,
        *,
        headers: dict[str, str] | None = None,
        params: dict[str, str] | None = None,
        json: dict[str, Any] | None = None,
    ) -> _DbResponse:
        table, is_rpc = self._route(url)
        if is_rpc:
            raise ValueError("PATCH RPC is not supported")
        values_map = dict(json or {})
        if not values_map:
            return _DbResponse([])

        assignments = [
            sql.SQL("{} = %s").format(sql.Identifier(column))
            for column in values_map
        ]
        values = [_coerce_value(column, value) for column, value in values_map.items()]
        where_parts: list[Any] = []
        for column, raw in (params or {}).items():
            where_parts.append(sql.SQL("{} = %s").format(sql.Identifier(column)))
            values.append(_filter_value(column, raw))
        if not where_parts:
            raise ValueError("direct update requires a filter")

        returning = "return=representation" in (headers or {}).get("Prefer", "")
        query = sql.SQL("update public.{} set {} where {}").format(
            sql.Identifier(table),
            sql.SQL(", ").join(assignments),
            sql.SQL(" and ").join(where_parts),
        )
        if returning:
            query += sql.SQL(" returning *")

        async with self._connection(headers) as connection:
            cursor = await connection.execute(query, tuple(values))
            rows = await cursor.fetchall() if returning else []
        return _DbResponse([dict(row) for row in rows])

    async def delete(
        self,
        url: str,
        *,
        headers: dict[str, str] | None = None,
        params: dict[str, str] | None = None,
    ) -> _DbResponse:
        table, is_rpc = self._route(url)
        if is_rpc:
            raise ValueError("DELETE RPC is not supported")
        where_parts: list[Any] = []
        values: list[Any] = []
        for column, raw in (params or {}).items():
            where_parts.append(sql.SQL("{} = %s").format(sql.Identifier(column)))
            values.append(_filter_value(column, raw))
        if not where_parts:
            raise ValueError("direct delete requires a filter")
        query = sql.SQL("delete from public.{} where {}").format(
            sql.Identifier(table),
            sql.SQL(" and ").join(where_parts),
        )
        async with self._connection(headers) as connection:
            await connection.execute(query, tuple(values))
        return _DbResponse([])

    async def _rpc(
        self,
        name: str,
        payload: dict[str, Any],
        headers: dict[str, str] | None,
    ) -> _DbResponse:
        if name in {"rkb_hybrid_search", "rkb_fast_e5_search"}:
            statement = f"select * from public.{name}(%s,%s,%s,%s)"
            values = (
                payload.get("query_text"),
                payload.get("query_embedding"),
                payload.get("query_embedding_space"),
                payload.get("match_count", 8),
            )
        elif name == 'rkb_multilingual_rankings':
            statement='select * from public.rkb_multilingual_rankings(%s,%s,%s,%s,%s,%s::jsonb,%s)'
            values=(payload.get('query_text'),payload.get('bge_vector'),payload.get('bge_space'),payload.get('e5_vector'),payload.get('e5_space'),json.dumps(payload.get('aliases') or []),payload.get('depth',100))
        elif name == "rkb_start_ingestion":
            statement = (
                "select * from public.rkb_start_ingestion("
                "%s,%s,%s,%s::jsonb,%s,%s,%s,%s,%s,%s)"
            )
            values = (
                UUID(str(payload["p_ingestion_id"])),
                UUID(str(payload["p_document_id"])),
                payload["p_title"],
                json.dumps(payload.get("p_authors") or [], ensure_ascii=False),
                payload.get("p_publication_year"),
                payload.get("p_language"),
                payload["p_source_sha256"],
                payload["p_source_file_id"],
                payload["p_page_count"],
                payload.get('p_duplicate_policy', 'reuse'),
            )
        elif name == "rkb_author_authority_for_names":
            statement = (
                "select * from public.rkb_author_authority_for_names(%s::text[],%s,%s)"
            )
            values = (
                payload.get("p_names") or [],
                payload.get("p_subject"),
                payload.get("p_geography"),
            )
        elif name == "rkb_insert_chunks":
            statement = (
                "select public.rkb_insert_chunks(%s,%s,%s,%s,%s::jsonb) "
                "as inserted_count"
            )
            values = (
                UUID(str(payload["p_document_id"])),
                UUID(str(payload["p_ingestion_id"])),
                int(payload["p_revision"]),
                UUID(str(payload["p_text_object_id"])),
                json.dumps(payload.get("p_chunks") or [], ensure_ascii=False),
            )
        elif name == "rkb_activate_revision":
            statement = (
                "select * from public.rkb_activate_revision(%s,%s,%s,%s::jsonb)"
            )
            values = (
                UUID(str(payload["p_document_id"])),
                UUID(str(payload["p_ingestion_id"])),
                int(payload["p_revision"]),
                json.dumps(payload.get("p_poi_events") or [], ensure_ascii=False),
            )
        else:
            raise ValueError(f"RPC is not allowed: {name}")

        async with self._connection(headers) as connection:
            cursor = await connection.execute(statement, values)
            rows = await cursor.fetchall()
        return _DbResponse([dict(row) for row in rows])

    async def aclose(self) -> None:
        if not self.pool.closed:
            await self.pool.close()


class PostgresBackend(SupabaseRestBackend):
    """Production backend: existing RKB semantics over direct Postgres + RLS."""

    def __init__(
        self,
        dsn: str,
        *,
        embedder: Embedder,
        object_store: ObjectStore,
        pool_min_size: int = 1,
        pool_max_size: int = 6,
        public_base_url: str | None = None,
    ) -> None:
        self.data_client = PostgresDataClient(
            dsn,
            min_size=pool_min_size,
            max_size=pool_max_size,
        )
        super().__init__(
            SupabaseConfig(
                url="postgres://regional-knowledge.internal",
                anon_key="direct-postgres",
                public_base_url=public_base_url,
                service_role_key="direct-postgres",
            ),
            embedder=embedder,
            client=self.data_client,  # type: ignore[arg-type]
            object_store=object_store,
        )

    def _headers(self, principal: Principal) -> dict[str, str]:
        # UUID validation here prevents arbitrary custom-GUC contents before a
        # connection is checked out. The OAuth bearer is deliberately discarded.
        actor = str(UUID(principal.subject))
        return {"x-rkb-actor": actor}

    def _service_headers(self) -> dict[str, str]:
        return {"x-rkb-service": "1"}

    async def _mark_ingestion_failed(
        self,
        ingestion_id: str,
        principal: Principal,
        error_code: str,
    ) -> None:
        try:
            await self._patch_ingestion(
                ingestion_id,
                principal,
                {"state": "failed", "error_code": error_code[:120]},
            )
        except Exception:
            return

    async def aclose(self) -> None:
        await super().aclose()
        await self.data_client.aclose()
