"""Tests for JSON Schema $ref resolution with uri_replace_pattern.

Verifies that uri_replace_pattern is applied to nested $ref resolution,
not just top-level references. See: https://github.com/datahub-project/datahub/issues/16238
"""

import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Dict, List, Optional

import datahub.metadata.schema_classes as models
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.schema.json_schema import JsonSchemaSource


def _write_schema(tmp_path: Path, filename: str, schema: Dict) -> Path:
    filepath = tmp_path / filename
    filepath.write_text(json.dumps(schema))
    return filepath


def _get_schema_metadata(
    workunits: List[MetadataWorkUnit],
) -> Optional[models.SchemaMetadataClass]:
    for wu in workunits:
        if isinstance(wu.metadata, MetadataChangeProposalWrapper):
            if isinstance(wu.metadata.aspect, models.SchemaMetadataClass):
                return wu.metadata.aspect
    return None


class TestNestedRefResolution:
    """Test that $ref resolution works correctly with local file references."""

    def test_nested_ref_resolves_locally(self, tmp_path: Path) -> None:
        """A schema referencing another schema in the same directory should resolve."""
        _write_schema(
            tmp_path,
            "status.json",
            {
                "$id": "https://example.com/enums/status/1.0",
                "type": "string",
                "enum": ["active", "inactive"],
            },
        )
        _write_schema(
            tmp_path,
            "person.json",
            {
                "$id": "https://example.com/types/person/1.0",
                "type": "object",
                "properties": {
                    "name": {"type": "string"},
                    "status": {"$ref": "status.json"},
                },
            },
        )

        config = {
            "path": str(tmp_path / "person.json"),
            "platform": "test_platform",
        }
        ctx = PipelineContext(run_id="test")
        source = JsonSchemaSource.create(config, ctx)
        workunits = list(source.get_workunits())

        schema = _get_schema_metadata(workunits)
        assert schema is not None
        field_paths = [f.fieldPath for f in schema.fields]
        # Should have resolved the $ref and found the status field
        assert any("status" in fp for fp in field_paths)


class TestUriReplacePatternOnRefs:
    """Test that uri_replace_pattern is applied during $ref resolution."""

    def test_uri_replace_applied_to_local_refs(self, tmp_path: Path) -> None:
        """uri_replace_pattern should transform URIs during ref resolution."""
        # Create schemas in a subdirectory
        schemas_dir = tmp_path / "schemas"
        schemas_dir.mkdir()

        _write_schema(
            schemas_dir,
            "status.json",
            {
                "$id": "https://example.com/enums/status/1.0",
                "type": "string",
                "enum": ["active", "inactive"],
            },
        )

        # The main schema references status via a URI that needs replacement
        _write_schema(
            schemas_dir,
            "person.json",
            {
                "$id": "https://example.com/types/person/1.0",
                "type": "object",
                "properties": {
                    "name": {"type": "string"},
                    "status": {"$ref": "https://example.com/enums/status/1.0"},
                },
            },
        )

        # Use uri_replace_pattern to redirect the HTTPS ref to local file
        config = {
            "path": str(schemas_dir / "person.json"),
            "platform": "test_platform",
            "uri_replace_pattern": {
                "match": "https://example.com/enums/status/1.0",
                "replace": f"file://{schemas_dir}/status.json",
            },
        }
        ctx = PipelineContext(run_id="test")
        source = JsonSchemaSource.create(config, ctx)
        workunits = list(source.get_workunits())

        schema = _get_schema_metadata(workunits)
        assert schema is not None
        field_paths = [f.fieldPath for f in schema.fields]
        assert any("status" in fp for fp in field_paths)

    def test_uri_replace_on_nested_ref_in_properties(self, tmp_path: Path) -> None:
        """Nested $ref inside properties should also use uri_replace_pattern.

        This tests the scenario from issue #16238 where a schema has a property
        that references another schema via HTTPS, and uri_replace_pattern should
        redirect that to a local file.
        """
        schemas_dir = tmp_path / "schemas"
        schemas_dir.mkdir()

        # Referenced enum schema — filename matches the last part of the URI
        _write_schema(
            schemas_dir,
            "status.json",
            {
                "$id": "https://registry.example.com/status.json",
                "type": "string",
                "enum": ["active", "inactive"],
                "description": "Status enum",
            },
        )

        # Top-level schema referencing status via HTTPS URI
        _write_schema(
            schemas_dir,
            "person.json",
            {
                "$id": "https://registry.example.com/person.json",
                "type": "object",
                "properties": {
                    "name": {"type": "string"},
                    "status": {"$ref": "https://registry.example.com/status.json"},
                },
            },
        )

        # uri_replace_pattern redirects HTTPS refs to local files
        config = {
            "path": str(schemas_dir / "person.json"),
            "platform": "test_platform",
            "uri_replace_pattern": {
                "match": "https://registry.example.com/",
                "replace": f"file://{schemas_dir}/",
            },
        }
        ctx = PipelineContext(run_id="test")
        source = JsonSchemaSource.create(config, ctx)
        workunits = list(source.get_workunits())

        schema = _get_schema_metadata(workunits)
        assert schema is not None
        field_paths = [f.fieldPath for f in schema.fields]
        # Should have resolved the $ref via uri_replace_pattern
        assert any("status" in fp for fp in field_paths), (
            f"Expected 'status' in field paths but got: {field_paths}"
        )


class TestUriReplaceWithHttpProxy:
    """Simulate the exact scenario from issue #16238.

    Schemas reference each other via HTTPS URLs that require auth.
    uri_replace_pattern redirects to a local HTTP proxy that serves
    the schemas without auth.
    """

    def test_https_refs_redirected_to_local_proxy(self, tmp_path: Path) -> None:
        """$ref to HTTPS URL should be redirected to local HTTP server via uri_replace_pattern."""

        # The referenced schema — served by our mock HTTP server
        status_schema = {
            "$id": "https://schemas.example.com/enums/status/1.0",
            "type": "string",
            "enum": ["active", "inactive"],
        }

        # Simple HTTP handler that serves the status schema
        class SchemaHandler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps(status_schema).encode())

            def log_message(self, format: str, *args: object) -> None:
                pass  # Suppress request logging

        # Start a local HTTP server
        server = HTTPServer(("127.0.0.1", 0), SchemaHandler)
        port = server.server_address[1]
        thread = threading.Thread(target=server.serve_forever)
        thread.daemon = True
        thread.start()

        try:
            # Main schema with $ref pointing to HTTPS (which would fail without redirect)
            _write_schema(
                tmp_path,
                "person.json",
                {
                    "$id": "https://schemas.example.com/types/person/1.0",
                    "type": "object",
                    "properties": {
                        "name": {"type": "string"},
                        "status": {
                            "$ref": "https://schemas.example.com/enums/status/1.0"
                        },
                    },
                },
            )

            # uri_replace_pattern redirects HTTPS to our local HTTP server
            config = {
                "path": str(tmp_path / "person.json"),
                "platform": "test_platform",
                "uri_replace_pattern": {
                    "match": "https://schemas.example.com/",
                    "replace": f"http://127.0.0.1:{port}/",
                },
            }
            ctx = PipelineContext(run_id="test")
            source = JsonSchemaSource.create(config, ctx)
            workunits = list(source.get_workunits())

            schema = _get_schema_metadata(workunits)
            assert schema is not None, "Schema should have been ingested successfully"
            field_paths = [f.fieldPath for f in schema.fields]
            assert any("status" in fp for fp in field_paths), (
                f"Expected 'status' field from resolved $ref, got: {field_paths}"
            )
        finally:
            server.shutdown()

    def test_nested_https_refs_both_redirected(self, tmp_path: Path) -> None:
        """Two levels of $ref via HTTPS should both be redirected."""

        schemas = {
            "/enums/status/1.0": {
                "$id": "https://schemas.example.com/enums/status/1.0",
                "type": "string",
                "enum": ["active", "inactive"],
            },
            "/types/metadata/1.0": {
                "$id": "https://schemas.example.com/types/metadata/1.0",
                "type": "object",
                "properties": {
                    "status": {"$ref": "https://schemas.example.com/enums/status/1.0"},
                    "version": {"type": "integer"},
                },
            },
        }

        class SchemaHandler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:
                schema = schemas.get(self.path)
                if schema:
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.end_headers()
                    self.wfile.write(json.dumps(schema).encode())
                else:
                    self.send_response(404)
                    self.end_headers()

            def log_message(self, format: str, *args: object) -> None:
                pass

        server = HTTPServer(("127.0.0.1", 0), SchemaHandler)
        port = server.server_address[1]
        thread = threading.Thread(target=server.serve_forever)
        thread.daemon = True
        thread.start()

        try:
            # Top-level schema references metadata, which references status
            _write_schema(
                tmp_path,
                "event.json",
                {
                    "$id": "https://schemas.example.com/events/user-created/1.0",
                    "type": "object",
                    "properties": {
                        "userId": {"type": "string"},
                        "metadata": {
                            "$ref": "https://schemas.example.com/types/metadata/1.0"
                        },
                    },
                },
            )

            config = {
                "path": str(tmp_path / "event.json"),
                "platform": "test_platform",
                "uri_replace_pattern": {
                    "match": "https://schemas.example.com",
                    "replace": f"http://127.0.0.1:{port}",
                },
            }
            ctx = PipelineContext(run_id="test")
            source = JsonSchemaSource.create(config, ctx)
            workunits = list(source.get_workunits())

            schema = _get_schema_metadata(workunits)
            assert schema is not None, (
                "Schema should have been ingested — both levels of $ref should resolve via proxy"
            )
            field_paths = [f.fieldPath for f in schema.fields]
            assert any("userId" in fp for fp in field_paths)
            assert any("metadata" in fp for fp in field_paths)
        finally:
            server.shutdown()

    def test_uri_replace_with_query_param_proxy(self, tmp_path: Path) -> None:
        """Simulate the exact user scenario: redirect to a proxy that takes the
        original URL as a query parameter.

        match: "https://schemas.example.com/"
        replace: "http://proxy/schema?uri=https://schemas.example.com/"

        So https://schemas.example.com/enums/status/1.0 becomes
        http://proxy/schema?uri=https://schemas.example.com/enums/status/1.0
        """

        status_schema = {
            "$id": "https://schemas.example.com/enums/status/1.0",
            "type": "string",
            "enum": ["active", "inactive"],
        }

        class ProxyHandler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:
                # The proxy receives the original URL as a query param
                from urllib.parse import parse_qs, urlparse

                parsed = urlparse(self.path)
                query = parse_qs(parsed.query)
                original_uri = query.get("uri", [None])[0]

                if original_uri == "https://schemas.example.com/enums/status/1.0":
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.end_headers()
                    self.wfile.write(json.dumps(status_schema).encode())
                else:
                    self.send_response(404)
                    self.end_headers()
                    self.wfile.write(f"Unknown uri: {original_uri}".encode())

            def log_message(self, format: str, *args: object) -> None:
                pass

        server = HTTPServer(("127.0.0.1", 0), ProxyHandler)
        port = server.server_address[1]
        thread = threading.Thread(target=server.serve_forever)
        thread.daemon = True
        thread.start()

        try:
            _write_schema(
                tmp_path,
                "person.json",
                {
                    "$id": "https://schemas.example.com/types/person/1.0",
                    "type": "object",
                    "properties": {
                        "name": {"type": "string"},
                        "status": {
                            "$ref": "https://schemas.example.com/enums/status/1.0"
                        },
                    },
                },
            )

            config = {
                "path": str(tmp_path / "person.json"),
                "platform": "test_platform",
                "uri_replace_pattern": {
                    "match": "https://schemas.example.com/",
                    "replace": f"http://127.0.0.1:{port}/schema?uri=https://schemas.example.com/",
                },
            }
            ctx = PipelineContext(run_id="test")
            source = JsonSchemaSource.create(config, ctx)
            workunits = list(source.get_workunits())

            schema = _get_schema_metadata(workunits)
            assert schema is not None, (
                "Schema should resolve $ref via proxy with query param pattern"
            )
            field_paths = [f.fieldPath for f in schema.fields]
            assert any("status" in fp for fp in field_paths), (
                f"Expected 'status' field, got: {field_paths}"
            )
        finally:
            server.shutdown()
