"""
Complexity Analyzer — F3 代码复杂度分析服务

基于 radon (Python) 和 tree-sitter (Java/TS) 的启发式圈复杂度分析，
构建项目级树形 ComplexityNode 结构，用于前端 Treemap 可视化。
"""

import asyncio
import logging
import hashlib
from dataclasses import dataclass
from pathlib import Path
import os
import time
from typing import Optional

from pydantic import BaseModel, Field
from typing import List, Literal

logger = logging.getLogger(__name__)

# ── 数据模型 ──


class ComplexityNode(BaseModel):
    name: str
    type: Literal["project", "directory", "file", "class", "method"] = "file"
    loc: int = 0  # 代码行数（Treemap 面积）
    cc: float = 0.0  # 圈复杂度
    mi: float = 100.0  # 维护指数 (0-100)
    risk_level: Literal["A", "B", "C", "D", "E"] = "A"
    children: Optional[List["ComplexityNode"]] = None
    file_path: Optional[str] = None
    language: Optional[str] = None


class ComplexityStats(BaseModel):
    total_files: int = 0
    avg_cc: float = 0.0
    high_risk_count: int = 0  # C/D/E 级别的文件数
    analysis_time_ms: int = 0


class ComplexityResult(BaseModel):
    root: ComplexityNode
    stats: ComplexityStats
    cached: bool = False
    truncated: bool = False


# ── 风险等级映射 (radon 标准) ──


def cc_to_risk(cc: float) -> Literal["A", "B", "C", "D", "E"]:
    """圈复杂度 → 风险等级"""
    if cc <= 5:
        return "A"
    if cc <= 10:
        return "B"
    if cc <= 20:
        return "C"
    if cc <= 30:
        return "D"
    return "E"


# ── 文件行数统计 ──

_PYGOUNT_AVAILABLE = False
try:
    from pygount import SourceAnalysis

    _PYGOUNT_AVAILABLE = True
except ImportError:
    logger.info("pygount not available, falling back to simple line counting")


def count_loc(file_path: str) -> int:
    """统计代码行数（不含空行和注释行）。优先使用 pygount，降级为简单统计。"""
    if _PYGOUNT_AVAILABLE:
        try:
            analysis = SourceAnalysis.from_file(file_path, group="pygount", encoding="utf-8")
            return analysis.code_count
        except Exception:
            pass
    # 降级：简单行数统计（排除空行）
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            return sum(1 for line in f if line.strip())
    except Exception:
        return 0


# ── 文件级缓存 ──

_file_cache: dict[str, tuple[float, float, ComplexityNode]] = {}
_CACHE_TTL = 300  # 5 分钟


def _cache_key(file_path: str) -> str:
    try:
        mtime = os.path.getmtime(file_path)
    except OSError:
        mtime = 0.0
    return f"{file_path}:{mtime}"


def _get_cached(file_path: str) -> Optional[ComplexityNode]:
    key = _cache_key(file_path)
    entry = _file_cache.get(key)
    if entry is None:
        return None
    ts, _, node = entry
    if time.time() - ts > _CACHE_TTL:
        _file_cache.pop(key, None)
        return None
    return node


def _set_cached(file_path: str, node: ComplexityNode) -> None:
    key = _cache_key(file_path)
    _file_cache[key] = (time.time(), 0.0, node)


# ── Python 文件分析 (radon) ──

_RADON_AVAILABLE = False
try:
    from radon.complexity import cc_visit
    from radon.metrics import mi_visit

    _RADON_AVAILABLE = True
except ImportError:
    logger.warning("radon not available, Python complexity analysis disabled")


def analyze_python_file(file_path: str) -> ComplexityNode:
    """使用 radon 分析 Python 文件复杂度"""
    with open(file_path, "r", encoding="utf-8") as f:
        source = f.read(1024 * 1024 + 1)
        if len(source.encode("utf-8")) > 1024 * 1024:
            raise ValueError("Complexity source exceeds one MiB limit")

    loc = count_loc(file_path)
    method_children: list[ComplexityNode] = []
    file_cc = 1.0

    if _RADON_AVAILABLE and source.strip():
        try:
            cc_results = cc_visit(source)
            if cc_results:
                # 每个结果是 Function/Class 对象
                for block in cc_results:
                    child = ComplexityNode(
                        name=block.name,
                        type="class" if hasattr(block, "methods") else "method",
                        loc=block.endline - block.lineno + 1 if block.endline else 1,
                        cc=float(block.complexity),
                        mi=0.0,
                        risk_level=cc_to_risk(block.complexity),
                    )
                    method_children.append(child)
                file_cc = sum(b.complexity for b in cc_results) / len(cc_results)
        except Exception as e:
            raise ValueError("Python complexity parser rejected source") from e

        try:
            mi_score = mi_visit(source, multi=True)
        except Exception as e:
            raise ValueError("Python maintainability parser rejected source") from e
    elif source.strip():
        raise RuntimeError("Python complexity analyzer is unavailable")
    else:
        mi_score = 100.0

    risk = cc_to_risk(file_cc)
    node = ComplexityNode(
        name=os.path.basename(file_path),
        type="file",
        loc=loc,
        cc=round(file_cc, 2),
        mi=round(mi_score, 2) if isinstance(mi_score, (int, float)) else 100.0,
        risk_level=risk,
        children=method_children if method_children else None,
        file_path=file_path,
        language="python",
    )
    return node


# ── Java/TS 文件分析 (tree-sitter 启发式) ──

# 分支节点类型 — 每个都贡献 +1 CC
_BRANCH_NODE_TYPES = {
    "if_statement", "if_expression",
    "for_statement", "for_in_statement", "enhanced_for_statement",
    "while_statement", "do_statement",
    "switch_expression", "switch_statement",
    "catch_clause",
    "ternary_expression", "conditional_expression",
    "case_statement", "switch_case",
    "logical_and", "logical_or",  # && || 也贡献分支
}

# tree-sitter 延迟引用
_ts_service = None


def _get_ts_service():
    global _ts_service
    if _ts_service is None:
        from services.tree_sitter_service import TreeSitterService
        _ts_service = TreeSitterService()
    return _ts_service


def _count_branches(node) -> int:
    """递归计数 AST 中的分支节点数量"""
    count = 1 if node.type in _BRANCH_NODE_TYPES else 0
    for child in node.children:
        count += _count_branches(child)
    return count


def _extract_methods_ts(root_node, source: str, language: str) -> list[ComplexityNode]:
    """从 tree-sitter AST 提取方法级复杂度"""
    methods: list[ComplexityNode] = []
    ts_svc = _get_ts_service()

    func_types = ts_svc.FUNCTION_TYPES
    class_types = ts_svc.CLASS_TYPES

    def walk(node, parent_name: Optional[str] = None):
        if node.type in class_types:
            name_node = node.child_by_field_name("name")
            cls_name = source.encode("utf-8")[name_node.start_byte:name_node.end_byte].decode("utf-8") if name_node else "<anonymous>"
            for child in node.children:
                walk(child, parent_name=cls_name)
            return
        if node.type in func_types:
            name_node = node.child_by_field_name("name")
            func_name = source.encode("utf-8")[name_node.start_byte:name_node.end_byte].decode("utf-8") if name_node else "<anonymous>"
            display = f"{parent_name}.{func_name}" if parent_name else func_name
            branches = _count_branches(node)
            cc_val = branches + 1
            line_count = node.end_point[0] - node.start_point[0] + 1
            methods.append(ComplexityNode(
                name=display,
                type="method",
                loc=line_count,
                cc=float(cc_val),
                mi=0.0,
                risk_level=cc_to_risk(cc_val),
            ))
            return
        for child in node.children:
            walk(child, parent_name)

    walk(root_node)
    return methods


def analyze_ts_java_file(file_path: str, language: str) -> ComplexityNode:
    """使用 tree-sitter 启发式分析 Java/TS 文件复杂度"""
    ts_svc = _get_ts_service()
    if not ts_svc.is_available():
        raise RuntimeError("tree-sitter not available")

    with open(file_path, "r", encoding="utf-8") as f:
        source = f.read(1024 * 1024 + 1)
        if len(source.encode("utf-8")) > 1024 * 1024:
            raise ValueError("Complexity source exceeds one MiB limit")

    loc = count_loc(file_path)

    # 确定 tree-sitter 语言名
    ext = os.path.splitext(file_path)[1]
    ts_lang = ts_svc.language_for_extension(ext) or language
    root_node = ts_svc.parse(source, ts_lang)

    # 提取方法级复杂度
    methods = _extract_methods_ts(root_node, source, ts_lang)

    # 文件级 CC = 方法 CC 均值，无方法则为 1
    if methods:
        file_cc = sum(m.cc for m in methods) / len(methods)
    else:
        file_cc = 1.0

    # 启发式 MI: 基于 LOC 和 CC 简单估算
    # Halstead 卷不可得，用简化公式: MI ≈ max(0, 171 - 5.2*ln(LOC) - 0.23*CC - 16.2*ln(LOC))
    import math
    if loc > 0:
        ln_loc = math.log(max(loc, 1))
        mi_raw = 171.0 - 5.2 * ln_loc - 0.23 * file_cc - 16.2 * ln_loc
        mi_score = max(0.0, min(100.0, mi_raw))
    elif source.strip():
        raise RuntimeError("Python complexity analyzer is unavailable")
    else:
        mi_score = 100.0

    risk = cc_to_risk(file_cc)
    return ComplexityNode(
        name=os.path.basename(file_path),
        type="file",
        loc=loc,
        cc=round(file_cc, 2),
        mi=round(mi_score, 2),
        risk_level=risk,
        children=methods if methods else None,
        file_path=file_path,
        language=language,
    )


# ── 支持的语言扩展名 ──

LANG_EXTENSIONS: dict[str, list[str]] = {
    "python": [".py"],
    "java": [".java"],
    "typescript": [".ts", ".tsx"],
    "javascript": [".js", ".jsx"],
}


def _detect_language(ext: str) -> Optional[str]:
    for lang, exts in LANG_EXTENSIONS.items():
        if ext in exts:
            return lang
    return None


# ── 忽略目录 ──

_IGNORE_DIRS = {
    "node_modules", ".git", "__pycache__", ".venv", "venv",
    "dist", "build", "target", ".idea", ".vscode", ".next",
    ".claude", ".zhikun", ".scratchpad", ".ai-code-assistant",
    "egg-info", ".pytest_cache", ".mypy_cache",
}

_MAX_FILES = 500


@dataclass(frozen=True)
class ComplexityScan:
    root: str
    files: tuple[str, ...]
    truncated: bool


def complexity_files(project_root: str, target_path: Optional[str] = None,
                     languages: Optional[list[str]] = None) -> ComplexityScan:
    """The analyzer and its worker cache must select exactly the same files."""
    selected = languages or list(LANG_EXTENSIONS)
    if any(language not in LANG_EXTENSIONS for language in selected):
        raise ValueError("Unsupported complexity language")
    extensions = {ext for language in selected for ext in LANG_EXTENSIONS[language]}
    root = target_path if target_path else project_root
    if not os.path.exists(root):
        raise FileNotFoundError("Complexity target not found")
    # Preserve explicit-file behavior; language filtering applies to directory scans.
    if os.path.isfile(root):
        return ComplexityScan(root, (root,), False)
    files: list[str] = []

    def walk(path: str):
        for entry in sorted(os.listdir(path)):
            if entry in _IGNORE_DIRS or entry.startswith("."):
                continue
            full = os.path.join(path, entry)
            if os.path.islink(full):
                continue
            if os.path.isdir(full):
                yield from walk(full)
            elif os.path.isfile(full) and os.path.splitext(entry)[1] in extensions:
                yield full

    for path in walk(root):
        if len(files) >= _MAX_FILES:
            return ComplexityScan(root, tuple(files), True)
        files.append(path)
    return ComplexityScan(root, tuple(files), False)


def complexity_fingerprint(project_root: str, target_path: Optional[str] = None,
                           languages: Optional[list[str]] = None) -> str:
    """Hash selected contents, not mtime/size, within the existing killable worker."""
    scan = complexity_files(project_root, target_path, languages)
    digest = hashlib.sha256()
    digest.update(b"truncated\0" if scan.truncated else b"complete\0")
    for path in scan.files:
        digest.update(os.fsencode(os.path.relpath(path, project_root)))
        digest.update(b"\0")
        with open(path, "rb") as source:
            digest.update(hashlib.file_digest(source, "sha256").digest())
    return digest.hexdigest()


# ── 核心分析器 ──


class ComplexityAnalyzer:
    """项目级代码复杂度分析器"""

    def __init__(self, languages: Optional[list[str]] = None):
        self.languages = languages or ["python", "java", "typescript", "javascript"]
        if any(language not in LANG_EXTENSIONS for language in self.languages):
            raise ValueError("Unsupported complexity language")
        self._files_analyzed = 0
        self._truncated = False

    async def analyze(
        self,
        project_root: str,
        target_path: Optional[str] = None,
        timeout: float = 60.0,
    ) -> ComplexityResult:
        """异步分析项目复杂度，在线程池中执行以不阻塞事件循环"""
        start = time.time()
        try:
            result = await asyncio.wait_for(
                asyncio.to_thread(self._analyze_sync, project_root, target_path),
                timeout=timeout,
            )
        except asyncio.TimeoutError:
            # A timed-out scanner is not a successful empty project. Public routes
            # use a killable worker; direct callers also receive a truthful failure.
            raise TimeoutError("Complexity analysis exceeded its deadline") from None
        elapsed = int((time.time() - start) * 1000)
        result.stats.analysis_time_ms = elapsed
        return result

    def _analyze_sync(
        self,
        project_root: str,
        target_path: Optional[str] = None,
    ) -> ComplexityResult:
        """同步分析入口"""
        scan = complexity_files(project_root, target_path, self.languages)
        self._files_analyzed = len(scan.files)
        self._truncated = scan.truncated
        root_node = self._build_tree(scan.root, project_root, scan.files)
        stats = self._compute_stats(root_node)
        return ComplexityResult(root=root_node, stats=stats, truncated=self._truncated)

    def _build_tree(self, path: str, project_root: str,
                    files: tuple[str, ...]) -> ComplexityNode:
        """Build only from the shared selection, without a second directory walk."""
        name = os.path.basename(path) or os.path.basename(project_root)
        if files == (path,):
            return self._analyze_file(path)
        groups: dict[str, list[str]] = {}
        for file_path in files:
            first = Path(os.path.relpath(file_path, path)).parts[0]
            groups.setdefault(first, []).append(file_path)
        children: list[ComplexityNode] = []
        for entry, selected in sorted(groups.items()):
            full_path = os.path.join(path, entry)
            child = self._build_tree(full_path, project_root, tuple(selected))
            if child.type == "file" or child.loc > 0:
                children.append(child)
        return self._aggregate_directory(name, children, path, project_root)

    def _analyze_file(self, file_path: str) -> ComplexityNode:
        """分析单个文件，带缓存"""
        cached = _get_cached(file_path)
        if cached is not None:
            return cached

        ext = os.path.splitext(file_path)[1]
        language = _detect_language(ext)
        if not language:
            return ComplexityNode(
                name=os.path.basename(file_path), type="file",
                file_path=file_path,
            )

        try:
            if language == "python":
                node = analyze_python_file(file_path)
            else:
                node = analyze_ts_java_file(file_path, language)
            _set_cached(file_path, node)
            return node
        except Exception as error:
            raise ValueError("Complexity file analysis failed") from error

    @staticmethod
    def _count_files(node: ComplexityNode) -> int:
        if node.type == "file":
            return 1
        if node.children:
            return sum(ComplexityAnalyzer._count_files(c) for c in node.children)
        return 0

    @staticmethod
    def _aggregate_directory(
        name: str,
        children: list[ComplexityNode],
        path: str,
        project_root: str,
    ) -> ComplexityNode:
        """将子节点指标聚合到目录节点"""
        if not children:
            return ComplexityNode(name=name, type="directory", loc=0, cc=0, mi=100)

        total_loc = sum(c.loc for c in children)
        # CC = 子节点加权平均 (按 LOC 加权)
        if total_loc > 0:
            avg_cc = sum(c.cc * c.loc for c in children) / total_loc
            avg_mi = sum(c.mi * c.loc for c in children) / total_loc
        else:
            avg_cc = sum(c.cc for c in children) / len(children)
            avg_mi = sum(c.mi for c in children) / len(children)

        node_type = "project" if path == project_root else "directory"

        return ComplexityNode(
            name=name,
            type=node_type,
            loc=total_loc,
            cc=round(avg_cc, 2),
            mi=round(avg_mi, 2),
            risk_level=cc_to_risk(avg_cc),
            children=children if children else None,
            file_path=path,
        )

    @staticmethod
    def _compute_stats(root: ComplexityNode) -> ComplexityStats:
        """从树形结构计算统计信息"""
        files: list[ComplexityNode] = []
        ComplexityAnalyzer._collect_files(root, files)

        total = len(files)
        avg_cc = sum(f.cc for f in files) / total if total else 0.0
        high_risk = sum(1 for f in files if f.risk_level in ("C", "D", "E"))

        return ComplexityStats(
            total_files=total,
            avg_cc=round(avg_cc, 2),
            high_risk_count=high_risk,
        )

    @staticmethod
    def _collect_files(node: ComplexityNode, result: list[ComplexityNode]) -> None:
        if node.type == "file":
            result.append(node)
        if node.children:
            for child in node.children:
                ComplexityAnalyzer._collect_files(child, result)
