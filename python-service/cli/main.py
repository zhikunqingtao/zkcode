"""
zkcode CLI 入口模块 — §4.21

Typer CLI 框架 + httpx HTTP 客户端 + Rich 终端美化。
命令名称: zkcode (zkcode 缩写，4 字符，易输入)

退出码:
  0 — 成功完成
  1 — 通用错误
  2 — 参数错误
  3 — 连接错误
  4 — 认证错误
  130 — SIGINT 中断
"""

import json
import ipaddress
import os
import signal
import sys
import uuid
from pathlib import Path
from typing import Optional
from enum import Enum
from importlib.metadata import version as pkg_version, PackageNotFoundError
from urllib.parse import urlsplit

import typer
import httpx
from rich.console import Console
from rich.markdown import Markdown

from .client import ZkcodeClient, StreamProtocolError
from .session import SessionCache

app = typer.Typer(
    name="zkcode",
    help="zkcode CLI — 通过管道和脚本调用 AI 编程助手",
    no_args_is_help=False,
    add_completion=True,
)
console = Console(stderr=True)   # 元信息输出到 stderr
stdout_console = Console()       # LLM 内容输出到 stdout
_active_query: Optional[tuple[ZkcodeClient, str]] = None


def _version_callback(value: bool):
    if value:
        try:
            v = pkg_version("zkcode-python-service")
        except PackageNotFoundError:
            v = "1.0.0"
        print(f"zkcode {v}")
        raise typer.Exit()


class OutputFormat(str, Enum):
    text = "text"
    json = "json"
    stream_json = "stream-json"


class PermissionMode(str, Enum):
    dont_ask = "dont_ask"
    default = "default"
    plan = "plan"
    accept_edits = "accept_edits"
    auto_approve = "auto_approve"


class EffortLevel(str, Enum):
    low = "low"
    medium = "medium"
    high = "high"
    xhigh = "xhigh"
    max = "max"


class ThinkingMode(str, Enum):
    adaptive = "adaptive"
    enabled = "enabled"
    disabled = "disabled"


def _is_loopback_server(server: str) -> bool:
    """Return whether the configured server can use this process' paths."""
    parsed = urlsplit(server if "://" in server else f"//{server}")
    hostname = parsed.hostname
    if not hostname:
        return False
    if hostname.lower() == "localhost":
        return True
    try:
        return ipaddress.ip_address(hostname).is_loopback
    except ValueError:
        # Deliberately do not resolve DNS names. A private-looking hostname does
        # not prove that the CLI and backend share a filesystem.
        return False


def _find_project_id(
    projects: list[dict],
    workspace_root: str,
) -> Optional[str]:
    """Find a Project whose persisted workspace path exactly matches cwd."""
    for project in projects:
        if (
            isinstance(project, dict)
            and project.get("workspaceRoot") == workspace_root
            and project.get("id")
        ):
            return str(project["id"])
    return None


def _resolve_local_project(
    client: ZkcodeClient,
    working_dir: str,
) -> Optional[str]:
    """Reuse or create the Project representing a local CLI workspace."""
    workspace_path = Path(working_dir).expanduser().resolve()
    workspace_root = str(workspace_path)

    try:
        projects = client.list_projects()
        project_id = _find_project_id(projects, workspace_root)
        if project_id:
            return project_id

        try:
            created = client.create_project(
                workspace_path.name or "root",
                workspace_root,
            )
            created_id = created.get("id")
            if not created_id:
                console.print(
                    "[red]Error: Project API returned no Project ID[/red]"
                )
                raise typer.Exit(code=1)
            return str(created_id)
        except httpx.HTTPStatusError as error:
            if error.response.status_code != 409:
                raise

            # Another CLI may have registered the same canonical path between
            # our list and create calls. Re-list once and reuse that Project.
            projects = client.list_projects()
            project_id = _find_project_id(projects, workspace_root)
            if project_id:
                return project_id
            raise
    except httpx.HTTPStatusError as error:
        status = error.response.status_code
        if status in (400, 403, 404):
            console.print(
                "[red]Error: the selected working directory cannot be "
                f"authorized (HTTP {status})[/red]"
            )
            raise typer.Exit(code=1)
        raise


def _handle_sigint(signum, frame):
    """Stop the owned request, not a newer query in the same session."""
    if _active_query is not None:
        client, request_id = _active_query
        try:
            client.cancel_query(request_id)
        except (httpx.HTTPError, ValueError):
            console.print("[yellow]Stop request could not be confirmed; check the task in zkcode.[/yellow]")
    console.print("\n[dim]Interrupted[/dim]")
    sys.exit(130)


@app.command()
def main(
    prompt: Optional[str] = typer.Argument(None, help="查询内容"),
    version: bool = typer.Option(
        False, "--version", "-V", callback=_version_callback,
        is_eager=True, help="显示版本号"),
    # 输出控制
    output_format: OutputFormat = typer.Option(
        OutputFormat.text, "--output-format", "-f", help="输出格式"),
    input_format: str = typer.Option(
        "text", "--input-format", help="输入格式: text | stream-json"),
    include_partial_messages: bool = typer.Option(
        False, "--include-partial-messages", help="包含部分消息块(仅stream-json)"),
    verbose: bool = typer.Option(False, "--verbose", help="详细输出"),
    quiet: bool = typer.Option(False, "--quiet", "-q", help="静默模式"),
    # 模型与行为
    model: Optional[str] = typer.Option(None, "--model", "-m", envvar=["ZK_MODEL", "AICA_MODEL"], help="指定模型"),
    thinking: Optional[ThinkingMode] = typer.Option(None, "--thinking", help="思考模式；effort 单独控制强度"),
    effort: Optional[EffortLevel] = typer.Option(
        None, "--effort", help="推理努力等级"),
    fallback_model: Optional[str] = typer.Option(
        None, "--fallback-model", help="主模型过载时降级模型"),
    system_prompt: Optional[str] = typer.Option(
        None, "--system-prompt", help="替换系统提示"),
    system_prompt_file: Optional[Path] = typer.Option(
        None, "--system-prompt-file", help="从文件读取系统提示"),
    append_system_prompt: Optional[str] = typer.Option(
        None, "--append-system-prompt", help="追加系统提示"),
    max_turns: Optional[int] = typer.Option(
        None, "--max-turns", min=1, help="最大轮次"),
    max_budget: Optional[float] = typer.Option(
        None, "--max-budget", min=0.000000001, help="预算上限 USD"),
    json_schema: Optional[str] = typer.Option(
        None, "--json-schema", help="JSON Schema 约束输出结构"),
    # 权限
    permission_mode: Optional[PermissionMode] = typer.Option(
        None, "--permission-mode", help="权限模式；未指定时新建默认 DONT_ASK，续接沿用会话权限"),
    # 工具
    allowed_tools: Optional[str] = typer.Option(
        None, "--allowed-tools", help="工具白名单(逗号分隔)"),
    disallowed_tools: Optional[str] = typer.Option(
        None, "--disallowed-tools", help="工具黑名单(逗号分隔)"),
    tools: Optional[str] = typer.Option(
        None, "--tools", help="指定可用工具集(逗号分隔)"),
    # 会话
    continue_session: bool = typer.Option(
        False, "--continue", "-c", help="继续上次会话"),
    resume: Optional[str] = typer.Option(
        None, "--resume", "-r", help="恢复指定会话"),
    session_id: Optional[str] = typer.Option(
        None, "--session-id", help="使用指定会话 ID"),
    project_id: Optional[str] = typer.Option(
        None, "--project-id",
        help="新建会话时使用的服务端 Project ID"),
    fork_session: bool = typer.Option(
        False, "--fork-session", help="恢复时创建新会话 ID"),
    name: Optional[str] = typer.Option(
        None, "--name", "-n", help="会话显示名称"),
    no_session: bool = typer.Option(
        False, "--no-session", help="不持久化会话"),
    # 连接
    server: str = typer.Option(
        "http://127.0.0.1:8082", "--server", "-s", envvar=["ZK_SERVER", "AICA_SERVER"], help="后端地址"),
    token: Optional[str] = typer.Option(
        None, "--token", help="认证 Token"),
    timeout: int = typer.Option(300, "--timeout", min=1, help="任务总期限秒数"),
    # MCP
    mcp_config: Optional[str] = typer.Option(
        None, "--mcp-config", help="MCP 配置文件"),
    # 工作目录
    working_dir: Optional[str] = typer.Option(
        None, "--working-dir", "-w",
        help="本地后端的新会话 Project 目录；远程后端请使用 --project-id"),
):
    """
    zkcode — 命令行查询接口。

    支持管道输入:  cat file.py | zkcode "review this"
    支持结构化输出: zkcode -f json "query" | jq '.result'
    """
    signal.signal(signal.SIGINT, _handle_sigint)

    if input_format not in ("text", "stream-json"):
        raise typer.BadParameter("input-format must be text or stream-json")
    if include_partial_messages and output_format != OutputFormat.stream_json:
        raise typer.BadParameter("--include-partial-messages requires --output-format stream-json")
    if no_session and (session_id or continue_session or resume or fork_session):
        raise typer.BadParameter("--no-session cannot resume, continue, or fork a saved session")
    if thinking == ThinkingMode.disabled and effort is not None:
        raise typer.BadParameter("--effort cannot be used with --thinking disabled")
    if sum(bool(value) for value in (continue_session, resume, session_id)) > 1:
        raise typer.BadParameter("Choose only one of --continue, --resume, or --session-id")

    # 1. 读取 stdin（如果是管道）
    stdin_content = None
    if not sys.stdin.isatty():
        stdin_content = sys.stdin.read(1024 * 1024 + 1)
        if len(stdin_content.encode("utf-8")) > 1024 * 1024:
            raise typer.BadParameter("stdin exceeds the 1 MiB limit; input was not submitted")
    input_messages = _parse_jsonl_users(stdin_content or "") if input_format == "stream-json" else None
    if input_messages is not None:
        stdin_content = None
    schema = _parse_json_option(json_schema, "json-schema") if json_schema is not None else None
    run_mcp_config = _parse_mcp_config(mcp_config) if mcp_config is not None else None
    if schema is not None and not isinstance(schema, (dict, bool)):
        raise typer.BadParameter("json-schema must be an object or boolean schema")

    # 2. 从文件读取系统提示
    effective_system_prompt = system_prompt
    if system_prompt_file and not system_prompt:
        effective_system_prompt = system_prompt_file.read_text(encoding="utf-8")

    # 3. 验证输入
    if not prompt and not stdin_content and not input_messages:
        console.print("[red]Error: No prompt or stdin input[/red]")
        raise typer.Exit(code=2)

    # 4. 解析权限模式
    # 权限模式只控制授权决策；任何模式都不能绕过系统安全不变量。
    perm = permission_mode.value.upper() if permission_mode is not None else None

    if working_dir is not None and not _is_loopback_server(server):
        console.print(
            "[red]Error: --working-dir can only authorize a directory "
            "when the backend is on localhost; use --project-id for a "
            "remote backend[/red]"
        )
        raise typer.Exit(code=2)

    if working_dir is not None and project_id is not None:
        console.print(
            "[red]Error: --working-dir cannot be combined "
            "with --project-id[/red]"
        )
        raise typer.Exit(code=2)

    # 5. 解析会话
    cache = SessionCache()
    wd = str(Path(working_dir or os.getcwd()).expanduser().resolve())
    resolved_sid = session_id
    if continue_session and not resolved_sid:
        resolved_sid = cache.get_last_session(wd)
    elif resume and not resolved_sid:
        resolved_sid = resume
    if continue_session and not resolved_sid:
        raise typer.BadParameter("No previous session exists for this directory")
    if fork_session and not resolved_sid:
        raise typer.BadParameter("--fork-session requires an existing session")
    if resolved_sid and project_id:
        console.print(
            "[red]Error: --project-id cannot be combined "
            "with an existing session[/red]")
        raise typer.Exit(code=2)

    # 6. 创建客户端，按需解析本地 Project，然后执行查询
    client = ZkcodeClient(server=server, token=token, timeout=timeout)

    response_sid = None
    request_id = str(uuid.uuid4())
    global _active_query
    try:
        effective_project_id = project_id
        if (
            not resolved_sid
            and not effective_project_id
            and _is_loopback_server(server)
        ):
            effective_project_id = _resolve_local_project(client, wd)
        if not resolved_sid and not effective_project_id:
            raise typer.BadParameter("A remote backend requires --project-id for a new session")

        request_body: dict = {
            "prompt": prompt or "",
            "requestId": request_id,
            "model": model,
            "effort": effort.value if effort else None,
            "thinking": thinking.value if thinking else None,
            "fallbackModel": fallback_model,
            "systemPrompt": effective_system_prompt,
            "appendSystemPrompt": append_system_prompt,
            "permissionMode": perm,
            "maxTurns": max_turns if max_turns is not None else 99,
            "maxBudgetUsd": max_budget,
            "allowedTools": _tool_names(allowed_tools) if allowed_tools is not None else None,
            "disallowedTools": _tool_names(disallowed_tools) if disallowed_tools is not None else None,
            "tools": _tool_names(tools) if tools is not None else None,
            "projectId": effective_project_id,
            "sessionId": resolved_sid,
            "forkSession": fork_session or None,
            "name": name,
            "timeoutSeconds": timeout,
            "jsonSchema": schema,
            "mcpConfig": run_mcp_config,
            "messages": input_messages,
            "includePartialMessages": include_partial_messages or None,
            "context": {"stdin": stdin_content} if stdin_content else None,
            "noSession": no_session or None,
        }
        request_body = {
            key: value
            for key, value in request_body.items()
            if value is not None
        }

        _active_query = (client, request_id)

        if output_format == OutputFormat.stream_json:
            response_sid = _stream_query(client, request_body, verbose)
        elif output_format == OutputFormat.json:
            response_sid = _sync_query_json(client, request_body)
        else:
            response_sid = _sync_query_text(client, request_body, verbose, quiet)
    except httpx.ConnectError:
        console.print(f"[red]Error: Backend not reachable at {server}[/red]")
        raise typer.Exit(code=3)
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 503:
            try:
                error = e.response.json()
            except ValueError:
                error = None
            if isinstance(error, dict) and error.get("code") == "CANCELLATION_PERSISTENCE_PENDING":
                console.print("[yellow]HTTP 等待已结束；原执行仍在保存取消状态或清理资源，请勿重发请求。[/yellow]")
                raise typer.Exit(code=1)
        if e.response.status_code == 409:
            try:
                body = e.response.json()
            except ValueError:
                body = {}
            error = body.get("error", body) if isinstance(body, dict) else None
            if isinstance(error, dict) and error.get("code") in ("PERMISSION_MODE_MISMATCH", "PERMISSION_MODE_CONFLICT"):
                console.print("[red]Error: 指定权限与会话权限不一致；请先在页面修改会话权限，或省略 --permission-mode 沿用当前设置。[/red]")
                raise typer.Exit(code=1)
        if e.response.status_code in (401, 403):
            console.print("[red]Error: Authentication failed[/red]")
            raise typer.Exit(code=4)
        console.print(f"[red]Error: HTTP {e.response.status_code}[/red]")
        raise typer.Exit(code=1)
    except (StreamProtocolError, httpx.TimeoutException, httpx.RemoteProtocolError) as error:
        try:
            client.cancel_query(request_id)
        except (httpx.HTTPError, ValueError):
            pass
        console.print(f"[red]Error: {error}[/red]")
        raise typer.Exit(code=1)
    finally:
        _active_query = None

    # 7. 更新本地会话缓存（优先使用后端响应中的 sessionId）
    final_sid = response_sid or resolved_sid or ""
    if not no_session:
        cache.save_last_session(wd, final_sid, model or "")


def _stream_query(client: ZkcodeClient, body: dict, verbose: bool) -> Optional[str]:
    """SSE 流式查询 — POST /api/query/stream"""
    response_session_id = None
    terminal_error = None
    completed = False
    for event in client.stream_query(body):
        event_name = event.get("event", event.get("type"))
        if event_name in ("text", "thinking", "stream_delta", "thinking_delta", "tool_input_delta") and not body.get("includePartialMessages"):
            continue
        print(json.dumps(event, ensure_ascii=False), flush=True)
        # 从 message_complete 事件中提取 sessionId
        if isinstance(event, dict) and event.get("sessionId"):
            response_session_id = event["sessionId"]
        if event_name == "error":
            terminal_error = event.get("code", "QUERY_FAILED")
        elif event_name in ("result", "message_complete"):
            terminal_error = event.get("error")
            completed = True
        elif event_name == "complete":
            completed = True
            if event.get("success") is False:
                terminal_error = terminal_error or "QUERY_FAILED"
    if not completed:
        raise StreamProtocolError("No terminal query result was received")
    if terminal_error:
        raise typer.Exit(code=1)
    return response_session_id


def _sync_query_json(client: ZkcodeClient, body: dict) -> Optional[str]:
    """同步查询 JSON 输出"""
    data = client.sync_query(body)
    print(json.dumps(data, ensure_ascii=False, indent=2))
    if data.get("error"):
        raise typer.Exit(code=1)
    return data.get("sessionId")


def _sync_query_text(client: ZkcodeClient, body: dict,
                     verbose: bool, quiet: bool) -> Optional[str]:
    """同步查询文本输出"""
    if not quiet:
        console.print("[dim]Thinking...[/dim]")
    data = client.sync_query(body)
    if data.get("error"):
        console.print(f"[red]Error: {data['error']}[/red]")
        raise typer.Exit(code=1)
    result = data.get("result", "")

    if sys.stdout.isatty():
        stdout_console.print(Markdown(result))
    else:
        print(result)

    if verbose and not quiet:
        usage = data.get("usage", {})
        cost = data.get("costUsd", 0)
        console.print(
            f"[dim]Tokens: {usage.get('inputTokens', 0)}in "
            f"+ {usage.get('outputTokens', 0)}out | "
            f"Cost: ${cost:.4f}[/dim]"
        )

    return data.get("sessionId")


def _tool_names(value: str) -> list[str]:
    return list(dict.fromkeys(name.strip() for name in value.split(",") if name.strip()))


def _parse_json_option(value: str, name: str):
    def invalid_constant(_value: str):
        raise ValueError("non-finite JSON number")
    try:
        return json.loads(value, parse_constant=invalid_constant)
    except (ValueError, RecursionError) as error:
        # Do not echo source text: MCP configuration and prompts may contain secrets.
        raise typer.BadParameter(f"{name} is not valid JSON") from error


def _parse_mcp_config(value: str) -> dict:
    if value.lstrip().startswith("{"):
        raw = value.encode("utf-8")
    else:
        try:
            with Path(value).expanduser().open("rb") as source:
                raw = source.read(256 * 1024 + 1)
        except OSError as error:
            raise typer.BadParameter("mcp-config file cannot be read") from error
    if len(raw) > 256 * 1024:
        raise typer.BadParameter("mcp-config exceeds the 256 KiB limit")
    try:
        text = raw.decode("utf-8-sig")
    except UnicodeError as error:
        raise typer.BadParameter("mcp-config must use UTF-8") from error
    config = _parse_json_option(text, "mcp-config")
    if not isinstance(config, dict) or not config:
        raise typer.BadParameter("mcp-config must be a non-empty object")
    return config


def _parse_jsonl_users(value: str) -> list[dict]:
    messages = []
    for number, line in enumerate(value.splitlines(), start=1):
        if not line.strip():
            continue
        item = _parse_json_option(line, f"JSONL line {number}")
        if isinstance(item, dict) and item.get("type") == "user":
            if set(item) != {"type", "message"}:
                raise typer.BadParameter(f"JSONL line {number} contains unsupported control fields")
            item = item["message"]
        if not isinstance(item, dict) or set(item) != {"role", "content"} or item["role"] != "user":
            raise typer.BadParameter(f"JSONL line {number} must be a user message")
        content = item["content"]
        if isinstance(content, list):
            if not content or any(not isinstance(block, dict) or set(block) != {"type", "text"}
                                  or block["type"] != "text" or not isinstance(block["text"], str) for block in content):
                raise typer.BadParameter(f"JSONL line {number} supports text content only")
            content = "\n".join(block["text"] for block in content)
        if not isinstance(content, str) or not content.strip():
            raise typer.BadParameter(f"JSONL line {number} requires non-empty user text")
        messages.append({"role": "user", "content": content})
        if len(messages) > 256:
            raise typer.BadParameter("JSONL exceeds the 256-message limit")
    return messages


if __name__ == "__main__":
    app()
