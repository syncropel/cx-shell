## [0.1.0] - 2025-10-04

This is the inaugural public release of the `cx-shell` under the **Syncropel** brand. This version establishes the foundation of the Universal Computational Fabric and introduces a comprehensive suite of tools for interactive, declarative, and agentic workflows.

### ✨ Key Features & Capabilities

#### **The Intelligent Shell & Workspace IDE**

- **Stateful Interactive REPL (`cx>`):** A robust interactive shell that maintains session state, including active connections and variables.
- **Session Variables (`=`):** Assign the output of any command to a variable for use in subsequent steps (e.g., `my_data = query run ...`).
- **Pipelining (`|`):** Compose complex, in-terminal workflows by piping the output of one command to the input of the next.
- **Universal Output Formatters (`--cx-*`):** Control the presentation of any command with flags like `--cx-output table` and `--cx-query <jmespath>`.
- **Session Persistence (`session` commands):** Save, load, list, and manage entire workspace sessions for perfect reproducibility.
- **Rich Object Inspection (`inspect`):** A dedicated command to view a detailed summary of any session variable.
- **Upgraded Parsing Engine:** The shell is powered by a robust **Lark** parser, ensuring a consistent and unambiguous command language.

#### **The Agentic Co-pilot (CARE Engine)**

- **Collaborative Agent (`agent <goal>`):** A multi-step AI reasoning engine that can understand high-level goals, formulate a plan, and execute `cx` commands to achieve them.
- **Translate Feature (`// <intent>`):** An instant, "fast path" co-pilot for translating natural language intent into a single, suggested `cx` command.
- **Human-in-the-Loop Workflow:** The agent presents its plan and requires user confirmation before execution for maximum safety and transparency.
- **On-Demand Onboarding:** The agent automatically guides users through setting up required LLM connections on first use.

#### **Computational Documents (`.cx.md` & Flows)**

- **Contextual Block Protocol (`.cx.md`):** A next-generation computational document format that seamlessly blends Markdown narrative with executable code blocks (`sql`, `python`, etc.) in a single, version-controllable file.
- **Polymorphic `ScriptEngine`:** The core engine can execute both traditional `.flow.yaml` meta-flows and the new `.cx.md` computational pages.
- **The `Publisher` (`cx publish`):** A powerful subsystem for rendering the results of a `.cx.md` execution into static, shareable artifacts like standalone HTML, PDF, and archival Markdown.

#### **The Data Fabric & Self-Organizing Workspace**

- **Multi-Rooted Workspace (`cx workspace`):** A namespaced workspace system that allows users to register multiple project directories. `cx` can discover and run assets from any registered location, from any working directory.
- **Immutable Run Manifests:** Every execution produces an auditable `RunManifest`, creating a decentralized, Git-friendly source of truth for all computational history.
- **Content-Addressable Caching:** All artifacts are stored in a content-addressable cache, enabling perfect, automatic **incremental execution**.
- **VFS Search Index (`cx find`):** A `LanceDB` vector index provides semantic search over the entire workspace and run history.

#### **The Integration & Application Ecosystem**

- **Blueprint Compiler (`cx compile`):** A powerful "Compile-Ahead" tool that ingests API specifications (OpenAPI, Google Discovery) and generates durable, type-safe Blueprint packages (`blueprint.cx.yaml`, `schemas.py`).
- **Application Lifecycle Management (`cx app`):** A full suite of commands (`install`, `list`, `package`, `search`) for managing self-contained, distributable `cx` applications.
- **Declarative Web Browser Provider:** A stateful provider that enables browser automation directly within flows using actions like `browser_navigate` and `browser_click`.

#### **Build, Distribution & Quality of Life**

- **Robust CI/CD Pipeline:** Fully automated, cross-platform builds for Linux, macOS, and Windows.
- **Linux Portability:** The Linux binary is built in an older Docker environment to ensure broad `glibc` compatibility.
- **Self-Upgrade Capability (`cx upgrade`):** The `cx` shell can check for, download, and install the latest version of itself.
- **Stateless `RunContext` Architecture:** The entire execution engine was refactored to operate on a stateless `RunContext` object, dramatically improving stability and testability.
- **Robust Path Resolution:** A centralized, workspace-aware path resolver (`utils.resolve_path`) eliminates `FileNotFoundError` issues for all asset types.
