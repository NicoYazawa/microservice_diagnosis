---
name: english-only
description: All files in this repository MUST NOT contain Chinese (CJK) characters. Use when writing, editing, or reviewing any source code, comments, configuration, SQL, scripts, documentation, or commit messages in this project.
---

# English Only

This project requires all human-readable content to be written in **English**. Chinese (CJK) characters are not allowed.

## Scope

Applies to every file in the repository, with one exception (`PLAN.md`):

- Source code and comments
- Log messages, error strings, and user-facing text
- Configuration files (YAML, JSON, TOML)
- Proto files, and the generated code derived from them (regenerate after editing protos)
- SQL init scripts and Prometheus configs
- PowerShell / shell scripts
- Documentation (README, docs/, design docs)
- Commit messages

## Why

- Windows PowerShell 5.1 misreads UTF-8 files without BOM as ANSI/GBK, which breaks `.ps1` scripts that contain non-ASCII text (this project already hit that failure).
- Tooling and CI stay consistent across locales.
- The codebase stays accessible to international contributors.

## Exceptions

- `PLAN.md` may keep its Chinese content (project decision).
- Test fixture data that legitimately contains CJK characters (e.g., real-world log patterns) is allowed.

## Checklist

When adding or modifying files:

1. No CJK characters (U+4E00–U+9FFF) in code, comments, configs, scripts, or docs.
2. After editing `.proto` files, regenerate `api/gen/` with `scripts/gen-proto.ps1` and commit the output.
3. Before finishing, scan the changed files for CJK characters and confirm none remain.
