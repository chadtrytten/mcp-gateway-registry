"""Mongo-style filter dict -> Postgres SQL WHERE-clause translator.

Reference implementation for POSTGRES-E (Phase Alpha, batch-NEXT-27).
Translates the 8-operator subset of MongoDB find() filters that upstream
mcp-gateway-registry actually passes to BaseRepository.find_with_filter():

  - {"field": value}                      equality (top-level / dot-nested)
  - {"field": {"$exists": True/False}}    key presence
  - {"field": {"$ne": value}}             NULL-safe inequality
  - {"field": {"$in": [...]}}             membership
  - {"field": {"$regex": pattern}}        Postgres-regex match
  - {"$or":  [filter_dict, ...]}          recursive disjunction
  - {"$and": [filter_dict, ...]}          recursive conjunction
  - implicit AND for sibling keys/operators

Anything else raises NotImplementedError.

Output param style: asyncpg-numbered ($1, $2, ...). Conversion to
psycopg-style %s placeholders is a trivial post-step if needed.

Field paths use dot-notation (Mongo convention). The leading "_id"
component is mapped to the configured id column (default "path"),
matching the existing repos that store the Mongo _id as the row
primary key.
"""

from __future__ import annotations

import re
from typing import Any, Iterable

__all__ = [
    "translate",
    "MongoToPostgresTranslator",
    "TranslationError",
]


class TranslationError(NotImplementedError):
    """Raised when a filter contains an operator outside the 8-op subset
    or a value the translator cannot safely coerce."""


# Field-name parts must look like simple identifiers. Field names are
# never user-supplied at runtime in the upstream repo (they are baked
# into call sites), but the JSONB path literal #>> '{a,b}' is unsafe
# against commas/braces, so we still validate.
_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

# The 8 supported field-level operators. $or/$and are handled at the
# dict level, not the field level, so they aren't in this set.
_FIELD_OPS = frozenset({"$exists", "$ne", "$in", "$regex"})


def translate(
    filter_dict: dict[str, Any],
    *,
    id_column: str = "path",
    data_column: str = "data",
    start_param: int = 1,
) -> tuple[str, list[Any]]:
    """Translate a Mongo-style filter into a Postgres WHERE clause.

    Args:
        filter_dict: Mongo-style filter. Empty dict yields ``("TRUE", [])``.
        id_column:   Postgres column the Mongo ``_id`` field maps to.
        data_column: JSONB column holding the document payload.
        start_param: Numbering offset for ``$n`` placeholders. Use this
                     when composing the fragment with another already-
                     parameterized SQL string.

    Returns:
        ``(sql_fragment, params)``. The SQL fragment is parenthesized
        when it contains multiple clauses, otherwise bare. It can be
        appended directly after ``WHERE``.

    Raises:
        TranslationError: filter contains an unsupported operator or
                          an unsafe field name.
    """
    t = MongoToPostgresTranslator(
        id_column=id_column, data_column=data_column, start_param=start_param
    )
    sql = t.translate(filter_dict)
    return sql, t.params


class MongoToPostgresTranslator:
    """Stateful translator. One instance per filter — reuse is unsafe
    because params accumulate."""

    def __init__(
        self,
        *,
        id_column: str = "path",
        data_column: str = "data",
        start_param: int = 1,
    ) -> None:
        if not _IDENT_RE.match(id_column):
            raise TranslationError(f"unsafe id_column identifier: {id_column!r}")
        if not _IDENT_RE.match(data_column):
            raise TranslationError(f"unsafe data_column identifier: {data_column!r}")
        if start_param < 1:
            raise TranslationError("start_param must be >= 1")
        self.id_column = id_column
        self.data_column = data_column
        self._start_param = start_param
        self.params: list[Any] = []

    # -- entry point ---------------------------------------------------

    def translate(self, filter_dict: dict[str, Any]) -> str:
        if not isinstance(filter_dict, dict):
            raise TranslationError("filter must be a dict")
        if not filter_dict:
            return "TRUE"
        return self._translate_dict(filter_dict)

    # -- helpers -------------------------------------------------------

    def _placeholder(self, value: Any) -> str:
        self.params.append(value)
        return f"${self._start_param + len(self.params) - 1}"

    def _validate_path(self, parts: Iterable[str]) -> None:
        for p in parts:
            if not _IDENT_RE.match(p):
                raise TranslationError(f"unsafe field-name component: {p!r}")

    def _is_id_field(self, field: str) -> bool:
        return field == "_id" or field.startswith("_id.")

    def _field_text(self, field: str) -> str:
        """Emit a SQL fragment yielding the field's *text* value
        (NULL when key is absent or the JSON value is null)."""
        if field == "_id":
            return self.id_column
        if field.startswith("_id."):
            raise TranslationError(
                "nested paths under _id are not supported (id is a scalar column)"
            )
        parts = field.split(".")
        self._validate_path(parts)
        if len(parts) == 1:
            return f"{self.data_column}->>'{parts[0]}'"
        path = "{" + ",".join(parts) + "}"
        return f"{self.data_column} #>> '{path}'"

    def _field_has_key(self, field: str) -> str:
        """Emit a SQL fragment that's true iff the key is present."""
        if field == "_id":
            # The id column is NOT NULL by schema; treat $exists:true as a no-op.
            return "TRUE"
        parts = field.split(".")
        self._validate_path(parts)
        if len(parts) == 1:
            return f"{self.data_column} ? '{parts[0]}'"
        # For nested keys, "presence" means the path resolves to any
        # JSON value (including JSON null). #> returns the JSON node;
        # IS NOT NULL distinguishes "key absent / parent missing"
        # (SQL NULL) from "key present" (any JSON value, even null).
        path = "{" + ",".join(parts) + "}"
        return f"{self.data_column} #> '{path}' IS NOT NULL"

    @staticmethod
    def _coerce_text(value: Any) -> str:
        """Coerce a Python scalar into the text representation that
        Postgres' ``->>`` extraction would yield for the same JSON
        value, so equality holds."""
        if value is None:
            # Caller must special-case None — coercion would be ambiguous.
            raise TranslationError("None comparison must be handled by caller")
        if isinstance(value, bool):
            return "true" if value else "false"
        if isinstance(value, (int, float)):
            return repr(value) if isinstance(value, float) else str(value)
        if isinstance(value, str):
            return value
        raise TranslationError(
            f"unsupported value type for Postgres backend: {type(value).__name__}"
        )

    # -- dispatch ------------------------------------------------------

    def _translate_dict(self, d: dict[str, Any]) -> str:
        clauses: list[str] = []
        for key, val in d.items():
            if key == "$or":
                clauses.append(self._translate_logical(val, "OR"))
            elif key == "$and":
                clauses.append(self._translate_logical(val, "AND"))
            elif key.startswith("$"):
                raise TranslationError(
                    f"Postgres backend does not support filter op: {key}"
                )
            else:
                clauses.append(self._translate_field(key, val))
        if len(clauses) == 1:
            return clauses[0]
        return "(" + " AND ".join(clauses) + ")"

    def _translate_logical(self, value: Any, joiner: str) -> str:
        if not isinstance(value, list) or not value:
            raise TranslationError(
                f"${joiner.lower()} requires a non-empty list of filters"
            )
        parts = [self._translate_dict(d) for d in value]
        if len(parts) == 1:
            return parts[0]
        return "(" + f" {joiner} ".join(parts) + ")"

    def _translate_field(self, field: str, condition: Any) -> str:
        # An operator-dict has only $-prefixed keys. Mixing operator and
        # non-operator keys is a Mongo error and we reject it too.
        if isinstance(condition, dict) and condition:
            keys = list(condition.keys())
            op_keys = [k for k in keys if k.startswith("$")]
            if op_keys and len(op_keys) != len(keys):
                raise TranslationError(
                    f"field {field!r} mixes operator and literal keys"
                )
            if op_keys:
                op_clauses = [
                    self._translate_field_op(field, op, condition[op])
                    for op in op_keys
                ]
                if len(op_clauses) == 1:
                    return op_clauses[0]
                return "(" + " AND ".join(op_clauses) + ")"
            # Non-operator dict => literal JSON-equality on the embedded
            # object. Not in the 8-op set; refuse rather than silently
            # producing wrong results.
            raise TranslationError(
                "literal-object equality is not supported "
                "(use explicit field paths)"
            )
        # Bare value => equality. None is a Mongo special case
        # ("matches docs where field is null OR missing"), expressed in
        # Postgres as IS NULL on the text extraction.
        if condition is None:
            return f"{self._field_text(field)} IS NULL"
        return f"{self._field_text(field)} = {self._placeholder(self._coerce_text(condition))}"

    def _translate_field_op(self, field: str, op: str, val: Any) -> str:
        if op not in _FIELD_OPS:
            raise TranslationError(
                f"Postgres backend does not support filter op: {op}"
            )
        if op == "$exists":
            if val is True:
                return self._field_has_key(field)
            if val is False:
                return f"NOT ({self._field_has_key(field)})"
            raise TranslationError("$exists requires a boolean")
        if op == "$ne":
            if val is None:
                # In Mongo, $ne:null matches docs where the field is
                # present AND not JSON-null. Postgres ->> returns SQL
                # NULL for both "absent" and "json null", so a single
                # IS NOT NULL suffices.
                return f"{self._field_text(field)} IS NOT NULL"
            return (
                f"{self._field_text(field)} IS DISTINCT FROM "
                f"{self._placeholder(self._coerce_text(val))}"
            )
        if op == "$in":
            if not isinstance(val, list):
                raise TranslationError("$in requires a list")
            if not val:
                # Mongo: $in:[] matches nothing.
                return "FALSE"
            coerced = [
                None if v is None else self._coerce_text(v) for v in val
            ]
            if any(v is None for v in coerced):
                # Postgres ANY() with a NULL element doesn't match
                # missing-key rows; emit an explicit OR for the null
                # branch.
                non_null = [v for v in coerced if v is not None]
                if non_null:
                    return (
                        f"({self._field_text(field)} IS NULL OR "
                        f"{self._field_text(field)} = ANY("
                        f"{self._placeholder(non_null)}::text[]))"
                    )
                return f"{self._field_text(field)} IS NULL"
            return (
                f"{self._field_text(field)} = ANY("
                f"{self._placeholder(coerced)}::text[])"
            )
        if op == "$regex":
            if not isinstance(val, str):
                raise TranslationError("$regex requires a string pattern")
            return f"{self._field_text(field)} ~ {self._placeholder(val)}"
        # Unreachable: guarded above.
        raise TranslationError(f"unhandled op: {op}")


# ----------------------------------------------------------------------
# Tests — run with:  python postgres-E-mongo-filter-py.py
# Pytest-compatible: each test_* function takes no fixtures.
# ----------------------------------------------------------------------

def _eq(actual, expected):
    assert actual == expected, f"\n  got:  {actual!r}\n  want: {expected!r}"


# -- per-operator coverage ---------------------------------------------

def test_top_level_equality():
    sql, params = translate({"tags": "agentcore"})
    _eq(sql, "data->>'tags' = $1")
    _eq(params, ["agentcore"])


def test_nested_equality_dot_notation():
    sql, params = translate({"metadata.agentcore_registry_id": "reg-123"})
    _eq(sql, "data #>> '{metadata,agentcore_registry_id}' = $1")
    _eq(params, ["reg-123"])


def test_exists_true_top():
    sql, params = translate({"ans_metadata": {"$exists": True}})
    _eq(sql, "data ? 'ans_metadata'")
    _eq(params, [])


def test_exists_true_nested():
    sql, params = translate({"meta.x": {"$exists": True}})
    _eq(sql, "data #> '{meta,x}' IS NOT NULL")


def test_exists_false():
    sql, params = translate({"is_active": {"$exists": False}})
    _eq(sql, "NOT (data ? 'is_active')")


def test_ne_value():
    sql, params = translate({"status": {"$ne": "deleted"}})
    _eq(sql, "data->>'status' IS DISTINCT FROM $1")
    _eq(params, ["deleted"])


def test_ne_null():
    sql, params = translate({"ans_metadata": {"$ne": None}})
    _eq(sql, "data->>'ans_metadata' IS NOT NULL")
    _eq(params, [])


def test_in_list():
    sql, params = translate({"status": {"$in": ["active", "pending"]}})
    _eq(sql, "data->>'status' = ANY($1::text[])")
    _eq(params, [["active", "pending"]])


def test_in_empty_list():
    sql, _ = translate({"status": {"$in": []}})
    _eq(sql, "FALSE")


def test_regex():
    sql, params = translate({"_id": {"$regex": "^/agents/agentcore-"}})
    # _id maps to the configured id column (default "path").
    _eq(sql, "path ~ $1")
    _eq(params, ["^/agents/agentcore-"])


def test_or():
    sql, params = translate(
        {"$or": [{"status": "active"}, {"status": {"$exists": False}}]}
    )
    _eq(sql, "(data->>'status' = $1 OR NOT (data ? 'status'))")
    _eq(params, ["active"])


def test_and():
    sql, params = translate(
        {"$and": [{"is_active": True}, {"tags": "mcp"}]}
    )
    _eq(sql, "(data->>'is_active' = $1 AND data->>'tags' = $2)")
    _eq(params, ["true", "mcp"])


# -- combined-operator coverage (real upstream callsites) --------------

def test_callsite_federation_metadata():
    """federation_routes.py:799 — agent_repo.find_with_filter(
       {"metadata.agentcore_registry_id": registry_id})"""
    sql, params = translate({"metadata.agentcore_registry_id": "REG_ABC"})
    _eq(sql, "data #>> '{metadata,agentcore_registry_id}' = $1")
    _eq(params, ["REG_ABC"])


def test_callsite_federation_tags_plus_id_regex():
    """federation_routes.py:803 — implicit AND of equality + $regex on _id."""
    sql, params = translate(
        {"tags": "agentcore", "_id": {"$regex": "^/agents/agentcore-"}}
    )
    _eq(sql, "(data->>'tags' = $1 AND path ~ $2)")
    _eq(params, ["agentcore", "^/agents/agentcore-"])


def test_callsite_ans_service_exists_and_ne_null():
    """ans_service.py:65 — combined operators in a single op-dict."""
    sql, params = translate(
        {"ans_metadata": {"$exists": True, "$ne": None}}
    )
    _eq(sql, "(data ? 'ans_metadata' AND data->>'ans_metadata' IS NOT NULL)")
    _eq(params, [])


def test_combined_and_or_nested():
    sql, params = translate(
        {
            "$or": [
                {"status": "active"},
                {"$and": [{"status": {"$exists": False}}, {"is_legacy": True}]},
            ]
        }
    )
    _eq(
        sql,
        "(data->>'status' = $1 OR (NOT (data ? 'status') AND data->>'is_legacy' = $2))",
    )
    _eq(params, ["active", "true"])


def test_param_offset_composition():
    sql, params = translate({"name": "alice"}, start_param=5)
    _eq(sql, "data->>'name' = $5")
    _eq(params, ["alice"])


def test_empty_filter_returns_true():
    _eq(translate({}), ("TRUE", []))


def test_equality_to_null_uses_is_null():
    sql, _ = translate({"deleted_at": None})
    _eq(sql, "data->>'deleted_at' IS NULL")


def test_in_with_null_member():
    sql, params = translate({"status": {"$in": [None, "active"]}})
    _eq(
        sql,
        "(data->>'status' IS NULL OR data->>'status' = ANY($1::text[]))",
    )
    _eq(params, [["active"]])


def test_numeric_value_is_text_encoded():
    sql, params = translate({"count": 42})
    _eq(sql, "data->>'count' = $1")
    _eq(params, ["42"])  # Postgres ->> returns text "42" for JSON 42


# -- whitelist / safety -----------------------------------------------

def test_unsupported_operator_raises():
    try:
        translate({"x": {"$gt": 1}})
    except TranslationError as e:
        assert "$gt" in str(e), e
    else:
        raise AssertionError("expected TranslationError for $gt")


def test_top_level_unknown_dollar_raises():
    try:
        translate({"$where": "this.a == 1"})
    except TranslationError as e:
        assert "$where" in str(e), e
    else:
        raise AssertionError("expected TranslationError for $where")


def test_unsafe_field_name_raises():
    try:
        translate({"a.b'); DROP TABLE": "x"})
    except TranslationError:
        pass
    else:
        raise AssertionError("expected TranslationError for unsafe field name")


def test_mixed_op_and_literal_keys_raises():
    try:
        translate({"f": {"$ne": "x", "literal": 1}})
    except TranslationError:
        pass
    else:
        raise AssertionError("expected TranslationError for mixed keys")


def test_unsupported_value_type_raises():
    try:
        translate({"f": {"a": 1}})  # bare nested dict on a field
    except TranslationError:
        pass
    else:
        raise AssertionError("expected TranslationError for nested dict")


def test_custom_columns():
    sql, params = translate(
        {"_id": "abc", "x": 1},
        id_column="server_path",
        data_column="payload",
    )
    _eq(sql, "(server_path = $1 AND payload->>'x' = $2)")
    _eq(params, ["abc", "1"])


# -- runner ------------------------------------------------------------

if __name__ == "__main__":
    import sys

    tests = [
        v for k, v in sorted(globals().items())
        if k.startswith("test_") and callable(v)
    ]
    failed = 0
    for t in tests:
        try:
            t()
        except Exception as e:
            failed += 1
            print(f"FAIL  {t.__name__}: {e}")
        else:
            print(f"ok    {t.__name__}")
    print(f"\n{len(tests) - failed}/{len(tests)} passed")
    sys.exit(1 if failed else 0)
