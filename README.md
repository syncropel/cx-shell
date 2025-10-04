# The Syncropel Shell (`cx`)

Welcome to the `cx-shell`, the core engine and command-line interface for the **Syncropel** platform. This is not just a shell; it's a powerful, stateful **"Workspace IDE"** for modern data and operations teams.

`cx` is the primary tool for interacting with the **Universal Computational Fabric**—a cohesive environment where your workflows, data, and context are unified, making them auditable, reproducible, and intelligent.

---

## 🚀 Quick Start: The 5-Minute Tutorial

Get your first "win" in under five minutes. This tutorial guides you through installing `cx` with a one-liner, initializing your workspace, and running your first command.

### 1. Installation

Our intelligent installer script will automatically detect your OS and architecture and install the latest stable version of `cx`.

**For Linux & macOS (including Apple Silicon via Rosetta 2):**

Open your terminal and run the following command:

```bash
curl -sL https://install.syncropel.com | bash
```

**For Windows (PowerShell):**

Open PowerShell as an Administrator and run the following command:

```powershell
irm https://install.syncropel.com/win | iex
```

After installation, open a **new terminal** and verify it's working:

```bash
cx --version
```

### 2. Initialize Your Environment

The `cx init` command creates the necessary configuration files and a sample "GitHub API" blueprint inside your home directory (`~/.cx/`). This is a one-time setup.

```bash
cx init
```

### 3. Run Your First Command

Now, start the interactive shell.

```bash
cx
```

Inside the shell, a world of possibilities opens up. Let's run your first commands:

```sh
# 1. Connect to the public GitHub API, giving it a temporary name 'gh'
cx> connect user:github --as gh

# 2. Execute a blueprint-driven API call to get Linus Torvalds' user profile
cx> gh.getUser(username="torvalds")
```

**Congratulations!** You've just used a pre-compiled Blueprint to execute a dynamic, validated API call. You are now ready to explore the full power of the Syncropel platform.

## 📚 Documentation

For full documentation, tutorials, and architectural deep dives, please visit our **[official documentation site](https://syncropel.github.io/docs/)**.

## 🤝 Contributing

We welcome contributions of all kinds! The most valuable way to contribute is by adding new integrations to our public **[Blueprint Registry](https://github.com/syncropel/blueprints)**.

### Setting up a Development Environment

If you'd like to contribute to the core `cx-shell` application, you'll need the following:

- Python 3.12+
- `uv` (installed via `pip install uv`)

The setup is streamlined and handles all dependencies automatically.

```bash
# 1. Clone the repository
git clone https://github.com/syncropel/cx-shell.git
cd cx-shell

# 2. Create the virtual environment
uv venv

# 3. Install all dependencies, including dev tools
# This automatically fetches cx-core-schemas from its GitHub repository.
uv pip install -e .[all]

# 4. Verify the installation
source .venv/bin/activate
cx --help
```

---

_Licensed under the MIT License._
_Copyright (c) 2025 Syncropel_
