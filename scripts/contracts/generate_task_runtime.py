#!/usr/bin/env python3
"""Generate TaskRuntime V4 Rust/TypeScript contracts from one JSON authority."""

from __future__ import annotations

import argparse
import copy
import difflib
import json
import re
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[2]
SOURCE = ROOT / "contracts" / "task-runtime-v4.json"
OUTPUTS = {
    ROOT / "crates" / "zk-db" / "src" / "generated" / "task_runtime_v4.rs": "db",
    ROOT / "crates" / "zk-tools" / "src" / "generated" / "task_runtime_v4.rs": "tools",
    ROOT / "frontend" / "src" / "types" / "generated" / "taskRuntimeV4.ts": "typescript",
}
EXPECTED_STATUS_GROUPS = {
    "taskStatus",
    "runStatus",
    "resultStatus",
    "cleanupStatus",
    "verificationStatus",
    "exitReason",
}
EXPECTED_TOOLS = {
    "Agent",
    "TaskCreate",
    "TaskGet",
    "TaskList",
    "TaskOutput",
    "TaskStop",
    "TaskUpdate",
    "SendMessage",
}
EXPECTED_SUCCESS_FIELDS = {
    "taskId",
    "runId",
    "parentTaskId",
    "status",
    "reason",
    "resultVersion",
    "partial",
    "resultRef",
    "usageSummary",
    "cleanupStatus",
    "waitExpired",
}
EXPECTED_ERROR_FIELDS = {"code", "message", "retryable", "details"}
CAMEL_CASE = re.compile(r"^[a-z][A-Za-z0-9]*$")
PASCAL_CASE = re.compile(r"^[A-Z][A-Za-z0-9]*$")
ERROR_CODE = re.compile(r"^[A-Z][A-Z0-9_]*$")
LEGACY_FIELD = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class ContractError(ValueError):
    """The canonical contract is internally inconsistent."""


def fail(message: str) -> None:
    raise ContractError(message)


def load_contract() -> dict[str, Any]:
    try:
        loaded = json.loads(SOURCE.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        fail(f"cannot read {SOURCE.relative_to(ROOT)}: {error}")
    if not isinstance(loaded, dict):
        fail("contract root must be an object")
    return loaded


def json_pointer(contract: dict[str, Any], pointer: str) -> Any:
    if not pointer.startswith("#/"):
        fail(f"only local JSON pointers are supported: {pointer}")
    current: Any = contract
    for raw_part in pointer[2:].split("/"):
        part = raw_part.replace("~1", "/").replace("~0", "~")
        if not isinstance(current, dict) or part not in current:
            fail(f"unresolved JSON pointer: {pointer}")
        current = current[part]
    return current


def public_schema_for_ref(contract: dict[str, Any], pointer: str) -> dict[str, Any]:
    target = json_pointer(contract, pointer)
    if not isinstance(target, dict):
        fail(f"JSON pointer must resolve to an object schema: {pointer}")
    if pointer.startswith("#/statuses/"):
        enum = target.get("enum")
        if not isinstance(enum, list):
            fail(f"status pointer has no enum: {pointer}")
        return {
            "type": "string",
            "enum": copy.deepcopy(enum),
        }
    return copy.deepcopy(target)


def expand_refs(contract: dict[str, Any], value: Any) -> Any:
    if isinstance(value, list):
        return [expand_refs(contract, item) for item in value]
    if not isinstance(value, dict):
        return value
    if "$ref" in value:
        if set(value) != {"$ref"} or not isinstance(value["$ref"], str):
            fail("$ref objects cannot contain sibling fields")
        return expand_refs(contract, public_schema_for_ref(contract, value["$ref"]))
    return {key: expand_refs(contract, item) for key, item in value.items()}


def validate_object_schema(
    contract: dict[str, Any], schema: Any, location: str
) -> None:
    if not isinstance(schema, dict) or schema.get("type") != "object":
        fail(f"{location} must be an object schema")
    properties = schema.get("properties")
    if not isinstance(properties, dict):
        fail(f"{location}.properties must be an object")
    for field, field_schema in properties.items():
        if not isinstance(field, str) or CAMEL_CASE.fullmatch(field) is None:
            fail(f"{location} has non-lowerCamelCase field: {field!r}")
        validate_refs(contract, field_schema, f"{location}.properties.{field}")
    required = schema.get("required", [])
    if not isinstance(required, list) or len(required) != len(set(required)):
        fail(f"{location}.required must contain unique field names")
    missing = [field for field in required if field not in properties]
    if missing:
        fail(f"{location}.required references missing properties: {missing}")


def validate_refs(contract: dict[str, Any], value: Any, location: str) -> None:
    if isinstance(value, list):
        for index, item in enumerate(value):
            validate_refs(contract, item, f"{location}[{index}]")
        return
    if not isinstance(value, dict):
        return
    enum = value.get("enum")
    if enum is not None:
        if not isinstance(enum, list) or not enum:
            fail(f"{location}.enum must be a non-empty list")
        encoded = [compact_json(item) for item in enum]
        if len(encoded) != len(set(encoded)):
            fail(f"{location}.enum contains duplicate values")
    if "$ref" in value:
        if set(value) != {"$ref"} or not isinstance(value["$ref"], str):
            fail(f"{location}: $ref cannot have siblings")
        public_schema_for_ref(contract, value["$ref"])
        return
    for key, item in value.items():
        validate_refs(contract, item, f"{location}.{key}")


def validate_contract(contract: dict[str, Any]) -> None:
    if contract.get("contract") != "zkcode.taskRuntime":
        fail("contract identity must be zkcode.taskRuntime")
    if contract.get("protocolVersion") != 4:
        fail("protocolVersion must be 4")
    if contract.get("fieldNaming") != "lowerCamelCase":
        fail("fieldNaming must be lowerCamelCase")

    statuses = contract.get("statuses")
    if not isinstance(statuses, dict) or set(statuses) != EXPECTED_STATUS_GROUPS:
        fail(
            "statuses must be exactly: "
            + ", ".join(sorted(EXPECTED_STATUS_GROUPS))
        )
    rust_types: set[str] = set()
    typescript_types: set[str] = set()
    for group_name, group in statuses.items():
        if not isinstance(group, dict) or group.get("type") != "string":
            fail(f"statuses.{group_name} must have type=string")
        rust_type = group.get("rustType")
        typescript_type = group.get("typescriptType")
        invalid_code = group.get("invalidCode")
        if not isinstance(rust_type, str) or PASCAL_CASE.fullmatch(rust_type) is None:
            fail(f"statuses.{group_name}.rustType must be PascalCase")
        if (
            not isinstance(typescript_type, str)
            or PASCAL_CASE.fullmatch(typescript_type) is None
        ):
            fail(f"statuses.{group_name}.typescriptType must be PascalCase")
        if not isinstance(invalid_code, str) or ERROR_CODE.fullmatch(invalid_code) is None:
            fail(f"statuses.{group_name}.invalidCode must be UPPER_SNAKE_CASE")
        if rust_type in rust_types or typescript_type in typescript_types:
            fail(f"duplicate generated type name in statuses.{group_name}")
        rust_types.add(rust_type)
        typescript_types.add(typescript_type)

        values = group.get("values")
        if not isinstance(values, list) or not values:
            fail(f"statuses.{group_name}.values must be non-empty")
        wires: set[str] = set()
        variants: set[str] = set()
        for index, value in enumerate(values):
            if not isinstance(value, dict):
                fail(f"statuses.{group_name}.values[{index}] must be an object")
            wire = value.get("wire")
            variant = value.get("rust")
            if not isinstance(wire, str) or CAMEL_CASE.fullmatch(wire) is None:
                fail(f"statuses.{group_name} has non-lowerCamelCase wire value: {wire!r}")
            if not isinstance(variant, str) or PASCAL_CASE.fullmatch(variant) is None:
                fail(f"statuses.{group_name} has invalid Rust variant: {variant!r}")
            if wire in wires:
                fail(f"statuses.{group_name} has duplicate wire value: {wire}")
            if variant in variants:
                fail(f"statuses.{group_name} has duplicate Rust variant: {variant}")
            if "terminal" in value and not isinstance(value["terminal"], bool):
                fail(f"statuses.{group_name}.{wire}.terminal must be boolean")
            wires.add(wire)
            variants.add(variant)
        enum = group.get("enum")
        if not isinstance(enum, list) or enum != [value["wire"] for value in values]:
            fail(f"statuses.{group_name}.enum must exactly match values[].wire")

    definitions = contract.get("$defs")
    if not isinstance(definitions, dict) or not definitions:
        fail("$defs must be a non-empty object")
    for name, schema in definitions.items():
        if CAMEL_CASE.fullmatch(name) is None:
            fail(f"definition name must be lowerCamelCase: {name}")
        validate_refs(contract, schema, f"$defs.{name}")

    responses = contract.get("responses")
    if not isinstance(responses, dict) or set(responses) != {"success", "error"}:
        fail("responses must contain exactly success and error")
    for response_name, schema in responses.items():
        validate_object_schema(contract, schema, f"responses.{response_name}")
    success_required = set(responses["success"].get("required", []))
    error_required = set(responses["error"].get("required", []))
    if success_required != EXPECTED_SUCCESS_FIELDS:
        fail("responses.success.required does not match the V4 common success contract")
    if error_required != EXPECTED_ERROR_FIELDS:
        fail("responses.error.required does not match the V4 error contract")

    tools = contract.get("tools")
    if not isinstance(tools, dict) or set(tools) != EXPECTED_TOOLS:
        fail("tools must be exactly: " + ", ".join(sorted(EXPECTED_TOOLS)))
    for tool_name, tool in tools.items():
        if not isinstance(tool, dict):
            fail(f"tools.{tool_name} must be an object")
        schema = tool.get("inputSchema")
        validate_object_schema(contract, schema, f"tools.{tool_name}.inputSchema")
        if schema.get("additionalProperties") is not False:
            fail(f"tools.{tool_name}.inputSchema must reject additional properties")
        legacy = tool.get("legacyFields")
        if not isinstance(legacy, list) or len(legacy) != len(set(legacy)):
            fail(f"tools.{tool_name}.legacyFields must contain unique names")
        for field in legacy:
            if not isinstance(field, str) or LEGACY_FIELD.fullmatch(field) is None:
                fail(f"tools.{tool_name} has invalid legacy field: {field!r}")
        overlap = set(legacy) & set(schema["properties"])
        if overlap:
            fail(f"tools.{tool_name} legacy/properties overlap: {sorted(overlap)}")


def rust_enum(group: dict[str, Any]) -> str:
    name = group["rustType"]
    values = group["values"]
    lines = [
        "#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]",
        f"pub enum {name} {{",
    ]
    for value in values:
        lines.extend(
            [
                f'    #[serde(rename = "{value["wire"]}")]',
                f'    {value["rust"]},',
            ]
        )
    lines.extend(["}", "", f"impl {name} {{"])
    lines.extend(
        [
            "    #[must_use]",
            "    pub const fn as_db(self) -> &'static str {",
            "        match self {",
        ]
    )
    for value in values:
        lines.append(f'            Self::{value["rust"]} => "{value["wire"]}",')
    lines.extend(["        }", "    }"])
    terminals = [value["rust"] for value in values if value.get("terminal", False)]
    if terminals:
        lines.extend(
            [
                "",
                "    #[must_use]",
                "    pub const fn is_terminal(self) -> bool {",
                "        matches!(",
                "            self,",
                "            " + " | ".join(f"Self::{variant}" for variant in terminals),
                "        )",
                "    }",
            ]
        )
    lines.extend(["", "    pub(crate) fn parse(value: &str) -> Result<Self, DbError> {", "        match value {"])
    for value in values:
        lines.append(f'            "{value["wire"]}" => Ok(Self::{value["rust"]}),')
    invalid_line = (
        f'            other => Err(DbError::Invalid(format!("{group["invalidCode"]}:{{other}}"))),'
    )
    # rustfmt's default small-heuristics width wraps this nested call above 90
    # even though the repository max_width is 100.
    if len(invalid_line) <= 90:
        lines.append(invalid_line)
    else:
        lines.extend(
            [
                "            other => Err(DbError::Invalid(format!(",
                f'                "{group["invalidCode"]}:{{other}}"',
                "            ))),",
            ]
        )
    lines.extend(["        }", "    }", "}"])
    return "\n".join(lines)


def render_db(contract: dict[str, Any]) -> str:
    header = [
        "// @generated by scripts/contracts/generate_task_runtime.py.",
        "// Source: contracts/task-runtime-v4.json. DO NOT EDIT.",
        "#![allow(missing_docs)]",
        "",
        "use serde::{Deserialize, Serialize};",
        "",
        "use crate::error::DbError;",
        "",
    ]
    body = "\n\n".join(rust_enum(group) for group in contract["statuses"].values())
    return "\n".join(header) + body + "\n"


def rust_const_name(tool_name: str) -> str:
    return re.sub(r"(?<!^)(?=[A-Z])", "_", tool_name).upper()


def rust_function_name(tool_name: str) -> str:
    return re.sub(r"(?<!^)(?=[A-Z])", "_", tool_name).lower() + "_input_schema"


def screaming_snake(name: str) -> str:
    return re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", name).upper()


def render_rust_string_list(name: str, values: list[str]) -> list[str]:
    lines = ["#[rustfmt::skip]", f"pub(crate) const {name}: &[&str] = &["]
    lines.extend(f'    "{value}",' for value in values)
    lines.append("];")
    return lines


def compact_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def render_runtime_constraints(prefix: str, schema: dict[str, Any]) -> list[str]:
    lines: list[str] = []
    for field, field_schema in schema["properties"].items():
        field_prefix = f"{prefix}_{screaming_snake(field)}"
        enum = field_schema.get("enum")
        if isinstance(enum, list) and all(isinstance(value, str) for value in enum):
            lines.extend(
                render_rust_string_list(f"{field_prefix}_VALUES", list(enum))
            )
        for keyword, suffix in (
            ("default", "DEFAULT"),
            ("minimum", "MINIMUM"),
            ("maximum", "MAXIMUM"),
            ("maxLength", "MAX_LENGTH"),
        ):
            if keyword not in field_schema:
                continue
            value = field_schema[keyword]
            name = f"{field_prefix}_{suffix}"
            if isinstance(value, str):
                lines.append(f'pub(crate) const {name}: &str = "{value}";')
            elif keyword == "maxLength":
                lines.append(f"pub(crate) const {name}: usize = {value};")
            elif field_schema.get("type") == "number":
                lines.append(f"pub(crate) const {name}: f64 = {float(value):.1f};")
            else:
                lines.append(f"pub(crate) const {name}: i64 = {value};")
    return lines


def render_tools(contract: dict[str, Any]) -> str:
    success = expand_refs(contract, contract["responses"]["success"])
    error = expand_refs(contract, contract["responses"]["error"])
    lines = [
        "// @generated by scripts/contracts/generate_task_runtime.py.",
        "// Source: contracts/task-runtime-v4.json. DO NOT EDIT.",
        "#![allow(dead_code, missing_docs)]",
        "",
        "use serde_json::{Value, json};",
        "",
        f'pub(crate) const TASK_RUNTIME_SUCCESS_SCHEMA_JSON: &str = r#"{compact_json(success)}"#;',
        f'pub(crate) const TASK_RUNTIME_ERROR_SCHEMA_JSON: &str = r#"{compact_json(error)}"#;',
        "",
        "pub(crate) fn task_runtime_success_schema() -> Value {",
        "    serde_json::from_str(TASK_RUNTIME_SUCCESS_SCHEMA_JSON)",
        '        .expect("generated TaskRuntime success schema must be valid JSON")',
        "}",
        "",
        "pub(crate) fn task_runtime_error_schema() -> Value {",
        "    serde_json::from_str(TASK_RUNTIME_ERROR_SCHEMA_JSON)",
        '        .expect("generated TaskRuntime error schema must be valid JSON")',
        "}",
        "",
        "pub(crate) fn task_runtime_error_response(",
        "    code: impl Into<String>,",
        "    message: impl Into<String>,",
        "    retryable: bool,",
        ") -> Value {",
        "    json!({",
        '        "code": code.into(),',
        '        "message": message.into(),',
        '        "retryable": retryable,',
        '        "details": {},',
        "    })",
        "}",
        "",
    ]
    for tool_name, tool in contract["tools"].items():
        prefix = rust_const_name(tool_name)
        schema = expand_refs(contract, tool["inputSchema"])
        allowed = list(tool["inputSchema"]["properties"])
        legacy = tool["legacyFields"]
        lines.append(
            f'const {prefix}_INPUT_SCHEMA_JSON: &str = r#"{compact_json(schema)}"#;'
        )
        lines.extend(render_rust_string_list(f"{prefix}_ALLOWED_FIELDS", allowed))
        lines.extend(render_rust_string_list(f"{prefix}_LEGACY_FIELDS", legacy))
        # Derive runtime guard constants from the public, fully-expanded schema so
        # referenced status enums remain part of the executable admission checks.
        lines.extend(render_runtime_constraints(prefix, schema))
        lines.extend(
            [
                "",
                f"pub(crate) fn {rust_function_name(tool_name)}() -> Value {{",
                f"    serde_json::from_str({prefix}_INPUT_SCHEMA_JSON)",
                f'        .expect("generated {tool_name} input schema must be valid JSON")',
                "}",
                "",
            ]
        )
    return "\n".join(lines).rstrip() + "\n"


def schema_to_typescript(contract: dict[str, Any], schema: dict[str, Any]) -> str:
    reference = schema.get("$ref")
    if isinstance(reference, str):
        if reference.startswith("#/statuses/"):
            group_name = reference.rsplit("/", 1)[1]
            return contract["statuses"][group_name]["typescriptType"]
        return schema_to_typescript(contract, public_schema_for_ref(contract, reference))
    schema_type = schema.get("type")
    if isinstance(schema_type, list):
        mapped = []
        for item in schema_type:
            mapped.append(
                "null"
                if item == "null"
                else schema_to_typescript(contract, {**schema, "type": item})
            )
        return " | ".join(dict.fromkeys(mapped))
    enum = schema.get("enum")
    if isinstance(enum, list):
        return " | ".join(json.dumps(item, ensure_ascii=False) for item in enum)
    if schema_type == "string":
        return "string"
    if schema_type in {"integer", "number"}:
        return "number"
    if schema_type == "boolean":
        return "boolean"
    if schema_type == "array":
        items = schema.get("items", {})
        return f"Array<{schema_to_typescript(contract, items)}>"
    if schema_type == "object":
        return "Record<string, unknown>"
    return "unknown"


def render_typescript_interface(
    contract: dict[str, Any], name: str, schema: dict[str, Any]
) -> list[str]:
    required = set(schema.get("required", []))
    lines = [f"export interface {name} {{"]
    if schema.get("additionalProperties") is True:
        lines.append("  [key: string]: unknown;")
    for field, field_schema in schema["properties"].items():
        optional = "" if field in required else "?"
        lines.append(
            f"  {field}{optional}: {schema_to_typescript(contract, field_schema)};"
        )
    lines.append("}")
    return lines


def render_typescript(contract: dict[str, Any]) -> str:
    lines = [
        "// @generated by scripts/contracts/generate_task_runtime.py.",
        "// Source: contracts/task-runtime-v4.json. DO NOT EDIT.",
        "",
        f"export const TASK_RUNTIME_V4_PROTOCOL_VERSION = {contract['protocolVersion']} as const;",
        "",
    ]
    for group in contract["statuses"].values():
        type_name = group["typescriptType"]
        const_name = re.sub(r"(?<!^)(?=[A-Z])", "_", type_name).upper() + "_VALUES"
        lines.append(f"export const {const_name} = [")
        lines.extend(f'  "{value["wire"]}",' for value in group["values"])
        lines.extend(["] as const;", f"export type {type_name} = typeof {const_name}[number];", ""])

    lines.extend(
        render_typescript_interface(
            contract,
            "TaskRuntimeSuccessResponse",
            contract["responses"]["success"],
        )
    )
    lines.append("")
    lines.extend(
        render_typescript_interface(
            contract,
            "TaskRuntimeErrorResponse",
            contract["responses"]["error"],
        )
    )
    lines.extend(["", "export const TASK_RUNTIME_V4_TOOL_INPUT_SCHEMAS = "])
    tool_schemas = {
        name: expand_refs(contract, tool["inputSchema"])
        for name, tool in contract["tools"].items()
    }
    rendered_tools = json.dumps(tool_schemas, ensure_ascii=False, indent=2, sort_keys=True)
    lines[-1] += rendered_tools + " as const;"
    lines.extend(
        [
            "",
            "export type TaskRuntimeV4ToolName =",
            "  keyof typeof TASK_RUNTIME_V4_TOOL_INPUT_SCHEMAS;",
        ]
    )
    return "\n".join(lines) + "\n"


def render_outputs(contract: dict[str, Any]) -> dict[Path, str]:
    renderers = {
        "db": render_db,
        "tools": render_tools,
        "typescript": render_typescript,
    }
    return {path: renderers[kind](contract) for path, kind in OUTPUTS.items()}


def check_or_write(outputs: dict[Path, str], check: bool) -> bool:
    clean = True
    for path, expected in outputs.items():
        try:
            actual = path.read_text(encoding="utf-8")
        except FileNotFoundError:
            actual = ""
        if actual == expected:
            continue
        clean = False
        relative = path.relative_to(ROOT)
        if check:
            print(f"task-runtime-contract: generated file differs: {relative}", file=sys.stderr)
            diff = difflib.unified_diff(
                actual.splitlines(),
                expected.splitlines(),
                fromfile=str(relative),
                tofile=f"{relative} (expected)",
                lineterm="",
            )
            for line in diff:
                print(line, file=sys.stderr)
        else:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(expected, encoding="utf-8")
            print(f"task-runtime-contract: wrote {relative}")
    return clean


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check",
        action="store_true",
        help="verify generated outputs without writing any files",
    )
    args = parser.parse_args()
    try:
        contract = load_contract()
        validate_contract(contract)
        outputs = render_outputs(contract)
    except ContractError as error:
        print(f"task-runtime-contract: invalid contract: {error}", file=sys.stderr)
        return 1
    clean = check_or_write(outputs, args.check)
    if args.check and not clean:
        print(
            "task-runtime-contract: run scripts/contracts/generate_task_runtime.py",
            file=sys.stderr,
        )
        return 1
    print("task-runtime-contract: ok")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
