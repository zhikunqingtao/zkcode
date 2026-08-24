"""Real filesystem/parser coverage for the production analysis capability domains.

These tests intentionally exercise the actual parsers and analyzers against a
temporary multi-language project.  They do not replace services or accept an
"unavailable" response as success.
"""

from __future__ import annotations

import networkx as nx
import pytest

from analyzers.code_path_tracer import CodePathTracer
from analyzers.flow_chart_generator import FlowChartGenerator
from analyzers.sequence_diagram_generator import SequenceDiagramGenerator
from routers.code_intel import (
    CodeMapRequest,
    DependenciesRequest,
    ParseRequest,
    SymbolsRequest,
    analyze_dependencies,
    build_code_map,
    extract_symbols,
    parse_code,
)
from routers.code_quality import ComplexityRequest, analyze_complexity
from services.complexity_analyzer import ComplexityAnalyzer, cc_to_risk, count_loc


@pytest.fixture
def real_project(tmp_path):
    (tmp_path / "api.py").write_text(
        '''
from fastapi import APIRouter
from service import UserService

router = APIRouter()

@router.get("/users/{user_id}")
def get_user(user_id: int):
    service = UserService()
    if user_id <= 0:
        return {"error": "invalid"}
    return service.fetch(user_id)
''',
        encoding="utf-8",
    )
    (tmp_path / "service.py").write_text(
        '''
from repository import UserRepository

class UserService:
    def fetch(self, user_id: int):
        repo = UserRepository()
        try:
            user = repo.find(user_id)
            if user is None:
                return {"missing": user_id}
            for key in ("id", "name"):
                if key not in user:
                    user[key] = None
            return user
        except ValueError:
            return {"error": "bad value"}
''',
        encoding="utf-8",
    )
    (tmp_path / "repository.py").write_text(
        '''
class UserRepository:
    def find(self, user_id: int):
        return {"id": user_id, "name": "Ada"}
''',
        encoding="utf-8",
    )
    (tmp_path / "DemoController.java").write_text(
        '''
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
class DemoController {
    @GetMapping("/demo")
    public String getDemo(int count) {
        if (count <= 0) {
            return "empty";
        }
        for (int i = 0; i < count; i++) {
            helper(i);
        }
        return "ok";
    }

    private void helper(int value) {
        switch (value) {
            case 1: break;
            default: break;
        }
    }
}
''',
        encoding="utf-8",
    )
    (tmp_path / "client.ts").write_text(
        '''
export function formatUser(name: string): string {
  if (!name) return "anonymous";
  return name.toUpperCase();
}

export const loadUser = async (id: number) => formatUser(String(id));
''',
        encoding="utf-8",
    )
    ignored = tmp_path / "node_modules"
    ignored.mkdir()
    (ignored / "ignored.py").write_text("raise RuntimeError('must not scan')\n")
    return tmp_path


def test_real_flowcharts_cover_python_java_and_missing(real_project):
    generator = FlowChartGenerator(str(real_project))

    python = generator.generate("UserService.fetch", depth=5)
    assert python.metadata.nodes_count >= 5
    assert python.metadata.edges_count >= 4
    assert python.metadata.languages_analyzed == ["python"]
    assert "flowchart TD" in python.mermaid_syntax
    assert "try 块" in python.mermaid_syntax

    java = generator.generate("DemoController.getDemo", depth=5)
    assert java.metadata.nodes_count >= 3
    assert java.metadata.languages_analyzed == ["java"]
    assert "flowchart TD" in java.mermaid_syntax

    missing = generator.generate('Missing.method["unsafe"]')
    assert missing.confidence_score == 0
    assert missing.metadata.nodes_count == 0
    assert "未找到" in missing.mermaid_syntax


def test_real_sequence_and_code_path_analysis(real_project):
    sequence = SequenceDiagramGenerator(str(real_project))
    result = sequence.generate("get_user", depth=5)
    assert result.diagram_type == "sequence"
    assert result.metadata.nodes_count >= 2
    assert "sequenceDiagram" in result.mermaid_syntax
    assert "participant" in result.mermaid_syntax

    missing = sequence.generate("not_a_real_entry")
    assert missing.confidence_score == 0
    assert missing.metadata.nodes_count == 0

    tracer = CodePathTracer(str(real_project))
    endpoints = tracer.scan_api_endpoints(["python"])
    assert any(endpoint.handler_function == "get_user" for endpoint in endpoints)
    endpoint = next(endpoint for endpoint in endpoints if endpoint.handler_function == "get_user")
    assert endpoint.language == "python"
    assert endpoint.line_number >= 1

    traced = tracer.trace_code_path(str(real_project / "api.py"), "get_user", max_depth=20)
    assert traced.entry_node
    assert any(node.name == "get_user" for node in traced.nodes)
    assert traced.total_depth >= 0
    assert traced.analysis_time_ms >= 0

    absent = tracer.trace_code_path(str(real_project / "api.py"), "absent")
    assert absent.entry_node is None
    assert absent.warnings


def test_sequence_and_path_helpers_cover_cycles_roles_and_metadata(real_project):
    graph = nx.DiGraph()
    graph.add_node(
        "api.UserController.get_user",
        name="get_user",
        type="api",
        file_path="api/controller.py",
        language="python",
        line_range=[3, 8],
        parameters=[{"name": "user_id", "type": "int"}],
        annotations=["GetMapping", "/users/{id}"],
        http_method="GET",
        path="/users/{id}",
    )
    graph.add_node(
        "service.UserService.fetch",
        name="fetch",
        type="service",
        file_path="service/user.py",
        language="python",
        line_range=(10, 20),
    )
    graph.add_node(
        "repository.UserRepository.find",
        name="find",
        type="repository",
        file_path="repository/user.py",
        language="python",
        line_range=(5, 7),
    )
    graph.add_node(
        "external.PaymentClient.call",
        name="call",
        type="function",
        file_path="client/external.py",
        language="python",
        line_range=(1, 2),
    )
    graph.add_edge("api.UserController.get_user", "service.UserService.fetch", type="call")
    graph.add_edge("service.UserService.fetch", "repository.UserRepository.find", type="dependency")
    graph.add_edge("repository.UserRepository.find", "api.UserController.get_user", type="call")
    graph.add_edge("service.UserService.fetch", "external.PaymentClient.call", type="unknown")

    sequence = SequenceDiagramGenerator(str(real_project))
    sequence._graph = graph
    entries = sequence._find_entry_nodes("/users/get_user")
    assert entries[0] == "api.UserController.get_user"
    chains, warnings = sequence._extract_call_chains(entries, 5)
    assert chains and warnings
    participants = sequence._identify_participants(chains)
    assert participants["api.UserController.get_user"] == "Controller"
    assert participants["service.UserService.fetch"] == "Service"
    assert participants["repository.UserRepository.find"] == "Repository"
    assert participants["external.PaymentClient.call"] == "External"
    rendered = sequence._render_mermaid(participants, chains)
    assert "-->>+" in rendered
    assert sequence._compute_confidence(chains, participants, warnings) < 1.0
    assert sequence._short_name("one.two.three") == "two.three"

    tracer = CodePathTracer(str(real_project))
    tracer._graph = graph
    nodes, edges, depth = tracer._forward_bfs(entries[0], 5)
    assert len(nodes) == 4
    assert len(edges) >= 3
    assert depth >= 2
    assert tracer._classify_layer("x", {"type": "api"}) == "controller"
    assert tracer._classify_layer("x", {"type": "service"}) == "service"
    assert tracer._classify_layer("x", {"type": "repository"}) == "repository"
    assert tracer._classify_layer("client.gateway.call", {}) == "external"
    assert tracer._extract_http_info(entries[0], graph.nodes[entries[0]]) == ("GET", "/users/{id}")
    assert tracer._extract_class_name(entries[0]) == "UserController"
    assert tracer._compute_layer_stats(nodes)


@pytest.mark.asyncio
async def test_real_complexity_service_and_router(real_project):
    assert [cc_to_risk(value) for value in (1, 6, 11, 21, 41)] == ["A", "B", "C", "D", "E"]
    assert count_loc(str(real_project / "service.py")) > 10

    analyzer = ComplexityAnalyzer()
    result = await analyzer.analyze(str(real_project), timeout=20)
    assert result.stats.total_files >= 5
    assert result.root.loc > 0
    assert result.root.children

    file_result = await analyzer.analyze(
        str(real_project), target_path=str(real_project / "service.py"), timeout=20
    )
    assert file_result.root.type == "file"
    assert file_result.root.language == "python"
    # A second analysis traverses the real cache path.
    cached = await analyzer.analyze(str(real_project), timeout=20)
    assert cached.stats.total_files == result.stats.total_files

    response = await analyze_complexity(
        ComplexityRequest(project_root=str(real_project), languages=["python", "java", "typescript"])
    )
    assert response["success"] is True
    assert response["data"]["stats"]["total_files"] >= 5


@pytest.mark.asyncio
async def test_real_code_intel_router_uses_tree_sitter():
    code = '''
import os
from pathlib import Path

class Greeter:
    """A real parsed class."""
    def hello(self, name: str) -> str:
        return f"Hello {name}"
'''
    parsed = await parse_code(ParseRequest(file_path="greeter.py", content=code))
    assert parsed.language == "python"
    assert any(symbol.name == "Greeter" for symbol in parsed.symbols)
    assert parsed.imports
    assert "Greeter" in parsed.code_map

    symbols = await extract_symbols(SymbolsRequest(code=code, language="python"))
    assert symbols.total >= 2
    dependencies = await analyze_dependencies(
        DependenciesRequest(content=code, language="python")
    )
    assert dependencies.total >= 2
    code_map = await build_code_map(CodeMapRequest(content=code, language="python"))
    assert code_map.symbol_count >= 2
