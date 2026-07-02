"""Core migration engine.

This package holds the importable migration engine: source introspection,
compatibility assessment, schema/query conversion, data migration, validation,
and the application anti-pattern linter. It has no dependency on the NiceGUI
UI so it can be embedded in other programs, tests, or a headless API.

Concrete engine components are implemented in subsequent tasks.
"""

__all__: list[str] = []
