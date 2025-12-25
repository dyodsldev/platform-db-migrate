# Platform DB Migration dbt Project
# Run 'just' or 'just --list' to see all available commands

# Set shell to zsh
set shell := ["zsh", "-uc"]

# Variables
dbt_dir := "dbt"
python_version := "3.11"

# Default recipe
default:
    @just --list

# Setup and Installation
# =====================

# Install all dependencies and setup dbt
setup:
    @echo "🔧 Setting up dbt project..."
    uv sync --all-groups
    cd {{dbt_dir}} && uv run dbt deps
    cd {{dbt_dir}} && uv run dbt debug
    @echo "✅ Setup complete!"

# Install dependencies only
install:
    @echo "📦 Installing dependencies..."
    uv sync
    @echo "✅ Dependencies installed!"

# Install with dev dependencies
install-dev:
    @echo "📦 Installing with dev dependencies..."
    uv sync --group dev
    @echo "✅ Dev dependencies installed!"

# Install all dependency groups
install-all:
    @echo "📦 Installing all dependency groups..."
    uv sync --all-groups
    @echo "✅ All dependencies installed!"

# Install dbt packages
deps:
    @echo "📦 Installing dbt packages..."
    cd {{dbt_dir}} && uv run dbt deps
    @echo "✅ dbt packages installed!"

# Development Commands
# ===================

# Run all dbt models
run target="dev":
    @echo "🚀 Running dbt models on {{target}}..."
    cd {{dbt_dir}} && uv run dbt run --target {{target}}
    @echo "✅ Models run complete!"

# Run specific model
run-model model target="dev":
    @echo "🚀 Running model: {{model}} on {{target}}..."
    cd {{dbt_dir}} && uv run dbt run --select {{model}} --target {{target}}
    @echo "✅ Model {{model}} run complete!"

# Run model and downstream dependencies
run-downstream model target="dev":
    @echo "🚀 Running {{model}} and downstream models on {{target}}..."
    cd {{dbt_dir}} && uv run dbt run --select {{model}}+ --target {{target}}
    @echo "✅ Complete!"

# Run model and upstream dependencies
run-upstream model target="dev":
    @echo "🚀 Running {{model}} and upstream models on {{target}}..."
    cd {{dbt_dir}} && uv run dbt run --select +{{model}} --target {{target}}
    @echo "✅ Complete!"

# Run full lineage of a model
run-full model target="dev":
    @echo "🚀 Running full lineage of {{model}} on {{target}}..."
    cd {{dbt_dir}} && uv run dbt run --select +{{model}}+ --target {{target}}
    @echo "✅ Complete!"

# Testing
# =======

# Run all tests
test target="dev":
    @echo "🧪 Running all tests on {{target}}..."
    cd {{dbt_dir}} && uv run dbt test --target {{target}}
    @echo "✅ All tests passed!"

# Test specific model
test-model model target="dev":
    @echo "🧪 Testing model: {{model}} on {{target}}..."
    cd {{dbt_dir}} && uv run dbt test --select {{model}} --target {{target}}
    @echo "✅ Tests for {{model}} passed!"

# Run only schema tests
test-schema target="dev":
    @echo "🧪 Running schema tests on {{target}}..."
    cd {{dbt_dir}} && uv run dbt test --select test_type:schema --target {{target}}
    @echo "✅ Schema tests passed!"

# Run only data tests
test-data target="dev":
    @echo "🧪 Running data tests on {{target}}..."
    cd {{dbt_dir}} && uv run dbt test --select test_type:data --target {{target}}
    @echo "✅ Data tests passed!"

# Build (run + test)
build target="dev":
    @echo "🏗️  Building (run + test) on {{target}}..."
    cd {{dbt_dir}} && uv run dbt build --target {{target}}
    @echo "✅ Build complete!"

# Build specific model
build-model model target="dev":
    @echo "🏗️  Building model: {{model}} on {{target}}..."
    cd {{dbt_dir}} && uv run dbt build --select {{model}} --target {{target}}
    @echo "✅ Build for {{model}} complete!"

# Documentation
# =============

# Generate and serve documentation
docs:
    @echo "📚 Generating documentation..."
    cd {{dbt_dir}} && uv run dbt docs generate
    @echo "🌐 Serving documentation at http://localhost:8080"
    cd {{dbt_dir}} && uv run dbt docs serve

# Generate documentation only
docs-generate:
    @echo "📚 Generating documentation..."
    cd {{dbt_dir}} && uv run dbt docs generate
    @echo "✅ Documentation generated!"

# Deployment
# ==========

# Deploy to development
deploy-dev:
    @echo "🚀 Deploying to development..."
    cd {{dbt_dir}} && uv run dbt run --target dev
    cd {{dbt_dir}} && uv run dbt test --target dev
    @echo "✅ Development deployment complete!"

# Deploy to staging
deploy-staging:
    @echo "🚀 Deploying to staging..."
    cd {{dbt_dir}} && uv run dbt run --target staging
    cd {{dbt_dir}} && uv run dbt test --target staging
    @echo "✅ Staging deployment complete!"

# Deploy to production (with confirmation)
deploy-prod:
    #!/usr/bin/env zsh
    set -e
    echo "⚠️  PRODUCTION DEPLOYMENT"
    echo "This will deploy to the production database."
    read "REPLY?Are you sure you want to continue? [y/N] "
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        echo "🚀 Deploying to production..."
        cd {{dbt_dir}} && uv run dbt run --target prod
        cd {{dbt_dir}} && uv run dbt test --target prod
        echo "✅ Production deployment complete!"
    else
        echo "❌ Production deployment cancelled"
        exit 1
    fi

# Full deployment with deps and build
deploy-full target:
    @echo "🚀 Full deployment to {{target}}..."
    uv sync --frozen
    cd {{dbt_dir}} && uv run dbt deps
    cd {{dbt_dir}} && uv run dbt build --target {{target}}
    @echo "✅ Full deployment to {{target}} complete!"

# Code Quality
# ============

# Lint SQL files
lint:
    @echo "🔍 Linting SQL files..."
    @if ! uv run sqlfluff --version &>/dev/null; then \
        echo "❌ sqlfluff not installed. Run 'just install-dev' first."; \
        exit 1; \
    fi
    uv run sqlfluff lint {{dbt_dir}}/models
    @echo "✅ Linting complete!"

# Lint specific path
lint-path path:
    @echo "🔍 Linting {{path}}..."
    uv run sqlfluff lint {{path}}
    @echo "✅ Linting complete!"

# Fix SQL formatting issues
fix:
    @echo "🔧 Fixing SQL formatting..."
    @if ! uv run sqlfluff --version &>/dev/null; then \
        echo "❌ sqlfluff not installed. Run 'just install-dev' first."; \
        exit 1; \
    fi
    uv run sqlfluff fix {{dbt_dir}}/models
    @echo "✅ Formatting fixed!"

# Fix specific path
fix-path path:
    @echo "🔧 Fixing {{path}}..."
    uv run sqlfluff fix {{path}}
    @echo "✅ Formatting fixed!"

# Format SQL files (alias for fix)
format: fix

# Run pre-commit hooks
pre-commit:
    @echo "🔍 Running pre-commit hooks..."
    @if ! uv run pre-commit --version &>/dev/null; then \
        echo "❌ pre-commit not installed. Run 'just hooks-install' first."; \
        exit 1; \
    fi
    uv run pre-commit run --all-files
    @echo "✅ Pre-commit checks complete!"

# Utility Commands
# ===============

# Show dbt debug information
debug target="dev":
    cd {{dbt_dir}} && uv run dbt debug --target {{target}}

# Compile dbt models without running
compile target="dev":
    @echo "🔨 Compiling dbt models..."
    cd {{dbt_dir}} && uv run dbt compile --target {{target}}
    @echo "✅ Compilation complete!"

# Parse dbt project
parse:
    @echo "📖 Parsing dbt project..."
    cd {{dbt_dir}} && uv run dbt parse
    @echo "✅ Parse complete!"

# Show dbt version
version:
    uv run dbt --version

# List all models
list-models:
    @echo "📋 Listing all models..."
    cd {{dbt_dir}} && uv run dbt list --resource-type model

# List all tests
list-tests:
    @echo "📋 Listing all tests..."
    cd {{dbt_dir}} && uv run dbt list --resource-type test

# List all sources
list-sources:
    @echo "📋 Listing all sources..."
    cd {{dbt_dir}} && uv run dbt list --resource-type source

# Show model lineage
lineage model:
    @echo "🔗 Showing lineage for {{model}}..."
    cd {{dbt_dir}} && uv run dbt list --select +{{model}}+

# Snapshot
# ========

# Run all snapshots
snapshot target="dev":
    @echo "📸 Running snapshots on {{target}}..."
    cd {{dbt_dir}} && uv run dbt snapshot --target {{target}}
    @echo "✅ Snapshots complete!"

# Seeds
# =====

# Load seed files
seed target="dev":
    @echo "🌱 Loading seed files on {{target}}..."
    cd {{dbt_dir}} && uv run dbt seed --target {{target}}
    @echo "✅ Seeds loaded!"

# Reload specific seed
seed-file file target="dev":
    @echo "🌱 Loading seed: {{file}} on {{target}}..."
    cd {{dbt_dir}} && uv run dbt seed --select {{file}} --target {{target}}
    @echo "✅ Seed {{file}} loaded!"

# Maintenance
# ===========

# Clean dbt artifacts
clean:
    @echo "🧹 Cleaning dbt artifacts..."
    cd {{dbt_dir}} && uv run dbt clean
    rm -rf {{dbt_dir}}/logs/
    @echo "✅ Clean complete!"

# Deep clean (includes venv and lock file)
clean-all: clean
    @echo "🧹 Deep cleaning project..."
    rm -rf .venv/
    rm -f uv.lock
    find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
    find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
    @echo "✅ Deep clean complete!"

# Update dependencies
update:
    @echo "⬆️  Updating dependencies..."
    uv lock --upgrade
    uv sync --all-groups
    cd {{dbt_dir}} && uv run dbt deps
    @echo "✅ Dependencies updated!"

# Update specific package
update-package package:
    @echo "⬆️  Updating {{package}}..."
    uv lock --upgrade-package {{package}}
    uv sync --all-groups
    @echo "✅ {{package}} updated!"

# CI/CD Helpers
# =============

# Run CI pipeline
ci target="staging":
    @echo "🤖 Running CI pipeline for {{target}}..."
    uv sync --frozen
    cd {{dbt_dir}} && uv run dbt deps
    cd {{dbt_dir}} && uv run dbt compile --target {{target}}
    cd {{dbt_dir}} && uv run dbt run --target {{target}}
    cd {{dbt_dir}} && uv run dbt test --target {{target}}
    @echo "✅ CI pipeline complete!"

# Validate project without running
validate:
    @echo "✓ Validating dbt project..."
    cd {{dbt_dir}} && uv run dbt parse
    cd {{dbt_dir}} && uv run dbt compile
    @echo "✅ Validation complete!"

# Development Workflows
# =====================

# Fresh start (clean + setup)
fresh: clean-all setup
    @echo "✨ Fresh start complete!"

# Quick iteration on a model
quick model:
    @echo "⚡ Quick iteration on {{model}}..."
    cd {{dbt_dir}} && uv run dbt run --select {{model}} --target dev
    cd {{dbt_dir}} && uv run dbt test --select {{model}} --target dev
    @echo "✅ Done!"

# Development full cycle
dev-cycle target="dev":
    @echo "🔄 Running development cycle on {{target}}..."
    just run {{target}}
    just test {{target}}
    just docs-generate
    @echo "✅ Development cycle complete!"

# Compare environments
compare-envs model:
    @echo "🔍 Comparing {{model}} across environments..."
    @echo "\n--- DEV ---"
    cd {{dbt_dir}} && uv run dbt run --select {{model}} --target dev
    @echo "\n--- STAGING ---"
    cd {{dbt_dir}} && uv run dbt run --select {{model}} --target staging
    @echo "✅ Comparison complete!"

# Monitoring & Debugging
# ======================

# Show compiled SQL for a model
show-sql model:
    @echo "📄 Compiled SQL for {{model}}:"
    @cat {{dbt_dir}}/target/compiled/*/models/**/*{{model}}*.sql 2>/dev/null || echo "Model not found. Run 'just compile' first."

# Show run results
show-results:
    @echo "📊 Last run results:"
    @cat {{dbt_dir}}/target/run_results.json | python -m json.tool 2>/dev/null || echo "No results found. Run dbt first."

# Tail dbt logs
logs:
    @tail -f {{dbt_dir}}/logs/dbt.log

# Git Hooks
# =========

# Setup git hooks (installs pre-commit if needed)
hooks-install:
    @echo "🔗 Installing git hooks..."
    @echo "First ensuring pre-commit is installed..."
    uv sync --group dev
    uv run pre-commit install
    @echo "✅ Git hooks installed!"

# Run git hooks manually
hooks-run:
    @if uv run pre-commit --version &>/dev/null; then \
        just pre-commit; \
    else \
        echo "⚠️  pre-commit not installed. Run 'just hooks-install' first."; \
    fi

# Help & Information
# ==================

# Show environment info
info:
    @echo "📊 Environment Information:"
    @echo "Shell: $SHELL"
    @echo "Python: $(uv run python --version 2>/dev/null || echo 'Not installed')"
    @echo "uv: $(uv --version 2>/dev/null || echo 'Not installed')"
    @echo "dbt: $(uv run dbt --version 2>/dev/null || echo 'Not installed')"
    @echo "Working directory: $(pwd)"
    @echo "dbt directory: {{dbt_dir}}"

# Show available targets from profiles.yml
targets:
    @echo "🎯 Available targets:"
    @grep -A 20 "outputs:" ~/.dbt/profiles.yml 2>/dev/null | grep "^\s\s[a-z]" | sed 's/://g' || echo "Could not read profiles.yml"

# Check project health
health:
    @echo "🏥 Checking project health..."
    @echo "\n✓ Checking Python environment..."
    @uv run python --version
    @echo "\n✓ Checking dbt installation..."
    @uv run dbt --version
    @echo "\n✓ Checking database connection..."
    @cd {{dbt_dir}} && uv run dbt debug --target dev || echo "⚠️  Database connection failed"
    @echo "\n✓ Checking project validity..."
    @cd {{dbt_dir}} && uv run dbt parse || echo "⚠️  Project parsing failed"
    @echo "\n✅ Health check complete!"