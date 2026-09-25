# Pop Mart — Databricks Asset Bundle CI/CD Demo

A production-ready example of deploying a **Lakeflow Declarative Pipeline (DLT)** + **Lakeflow Job** to Databricks using **Databricks Asset Bundles (DABs)** and **GitHub Actions**.

---

## What's in This Bundle?

| Component | Description |
|-----------|-------------|
| **DLT Pipeline** | Medallion architecture — Bronze → Silver → Gold across sales, inventory, members, and supply chain |
| **Lakeflow Job** | Orchestrates the DLT pipeline + runs a post-pipeline report notebook |
| **GitHub Actions** | Two workflows: PR validation (validate + unit tests) and deploy (dev → prod with approval gate) |

---

## Project Structure

```
popmart-dab-cicd/
├── databricks.yml                    ← Root bundle config (variables, targets)
├── resources/
│   ├── pipeline.yml                  ← DLT pipeline resource
│   └── job.yml                       ← Lakeflow Job (pipeline task + notebook task)
├── src/
│   ├── transformations/
│   │   └── medallion_pipeline.sql    ← All DLT views (Silver + Gold)
│   └── notebooks/
│       └── post_pipeline_report.py   ← Post-run validation & DQ report notebook
├── tests/
│   └── unit/
│       └── test_placeholder.py       ← Unit tests (run on every PR)
└── .github/workflows/
    ├── pr-validation.yml             ← Validates bundle + runs tests on PRs
    └── deploy.yml                    ← Deploys dev → prod on merge to main
```

---

## Environments

| Target | Mode | Behavior |
|--------|------|----------|
| `dev` | `development` | Names auto-prefixed `[dev <you>]`, schedules paused, isolated workspace paths |
| `prod` | `production` | Clean names, strict validation, runs as service principal |

---

## CI/CD Flow

```
PR Opened → pr-validation.yml
               ├── databricks bundle validate -t dev
               └── pytest tests/unit

Merge to main → deploy.yml
                   ├── Deploy → dev
                   └── Deploy → prod  ← requires manual approval (GitHub Environment gate)
```

---

## Setup

### 1. Configure workspace hosts

Edit `databricks.yml` and replace the placeholder URLs:

```yaml
targets:
  dev:
    workspace:
      host: https://YOUR-DEV-WORKSPACE.azuredatabricks.net   # ← replace
  prod:
    workspace:
      host: https://YOUR-PROD-WORKSPACE.azuredatabricks.net  # ← replace
```

### 2. Add GitHub Secrets

Go to **repo Settings → Secrets and variables → Actions** and add:

| Secret | Value |
|--------|-------|
| `DATABRICKS_DEV_HOST` | Your dev workspace URL |
| `DATABRICKS_DEV_TOKEN` | Dev workspace PAT |
| `DATABRICKS_PROD_HOST` | Your prod workspace URL |
| `DATABRICKS_PROD_TOKEN` | Prod workspace PAT (or use OIDC) |

### 3. Create the `production` GitHub Environment

Go to **repo Settings → Environments → New environment** → name it `production` → add **Required reviewers**.

This creates the manual approval gate before any prod deployment.

### 4. Deploy locally (dev)

```bash
# Install CLI
brew tap databricks/tap && brew install databricks

# Authenticate
databricks auth login

# Validate
databricks bundle validate

# Deploy to dev
databricks bundle deploy

# Run the job manually on dev
databricks bundle run popmart_medallion_job
```

---

## Key Demo Talking Points

1. **One repo, all environments** — the same `databricks.yml` drives dev and prod with per-target variable overrides
2. **`mode: development`** — auto-prefixes names, pauses schedules, isolates paths; no collisions between developers
3. **Job = pipeline + notebook** — Task 1 runs DLT, Task 2 runs a validation notebook; DABs wire the dependency automatically
4. **PR gate** — `bundle validate` catches YAML errors, missing references, and schema violations before code lands on main
5. **Manual approval for prod** — GitHub Environments provide the promotion gate without a separate tool
6. **Rollback** — revert the commit and rerun the deploy workflow; DABs redeploy the previous state

---

## Resources

- [Databricks Asset Bundles docs](https://docs.databricks.com/dev-tools/bundles/index.html)
- [CI/CD on Databricks](https://docs.databricks.com/dev-tools/ci-cd/)
- [STS DABs Demo Repo (internal)](https://github.com/databricks-field-eng/sts-dabs-demo)
