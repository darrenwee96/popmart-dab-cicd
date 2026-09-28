# Pop Mart — Databricks Asset Bundle CI/CD 演示

一个可用于生产的示例：使用 **Databricks Asset Bundles (DABs)** 和 **GitHub Actions**，将 **Lakeflow 声明式管道 (DLT)** + **Lakeflow 作业** 部署到 Databricks。

---

## 这个 Bundle 里有什么？

| 组件 | 说明 |
|------|------|
| **DLT 管道** | AMER Trade Orders 奖章架构 — Bronze → Silver → Gold，serverless + Photon，触发式（triggered）模式 |
| **Lakeflow 作业** | 编排 DLT 管道（任务 1）+ 运行管道后的报告 notebook（任务 2） |
| **GitHub Actions** | 两个工作流：PR 校验（validate + 单元测试）和部署（staging → prod，带审批门禁） |

---

## 项目结构

```
popmart-dab-cicd/
├── databricks.yml                              ← Bundle 根配置（变量、目标环境）
├── resources/
│   ├── pipeline.yml                            ← DLT 管道资源
│   └── job.yml                                 ← Lakeflow 作业（管道任务 + notebook 任务）
├── src/
│   ├── transformations/
│   │   └── amer_trade_orders_medallion.sql     ← 所有 DLT 视图（Bronze → Silver → Gold）
│   └── notebooks/
│       └── post_pipeline_report.py             ← 运行后的校验与数据质量报告 notebook
├── tests/
│   └── unit/
│       └── test_placeholder.py                 ← 5 个单元测试（每次 PR 运行）
└── .github/workflows/
    ├── pr-validation.yml                       ← 在 PR 上校验 bundle + 运行测试
    └── deploy.yml                              ← 合并到 main 时部署 staging → prod
```

---

## 环境（Targets）

三个目标环境，都指向同一个工作区，但通过 mode 和前缀彼此隔离：

| 目标 | Mode / 前缀 | 由谁部署 | 行为 |
|------|-------------|----------|------|
| `dev` | `development`，`[dev <你>]` | **你，本地部署** — 从不经过 CI/CD | 资源名自动加前缀，调度自动暂停，每个用户的路径相互隔离，schema 为 `popmart_medallion_dev` |
| `staging` | `[staging]` 前缀 | CI/CD，合并时自动部署 | 共享的 CI 环境，catalog 为 `popmart`，schema 为 `popmart_medallion` |
| `prod` | `production` | CI/CD，需人工审批后部署 | 干净的资源名，固定 `root_path`，catalog 为 `popmart-prod` |

> **注意：** `dev` 仅作为本地沙盒使用 — 部署工作流从不会触及它。生产环境的 `run_as`（服务主体）已在 `databricks.yml` 中预留但被注释掉；等服务主体（SP）配置好后再启用。

---

## CI/CD 流程

```
打开 PR → pr-validation.yml
               ├── databricks bundle validate -t staging
               └── pytest tests/unit

合并到 main → deploy.yml
                   ├── 部署 → staging   (databricks bundle deploy -t staging)
                   └── 部署 → prod      ← 需人工审批（GitHub Environment 门禁）
```

只有已提交到 `main` 的代码才会发布到 staging/prod — GitHub Actions 会先检出（checkout）仓库，再从该检出运行 bundle 部署。

---

## 本地部署 vs. CI/CD 部署

- **本地 `databricks bundle deploy`** 会把你**工作目录中的文件**（已提交*和*未提交的）快照上传到你个人的 `dev` 路径。它会记录 git 分支/提交作为元数据，但真正运行的文件是磁盘上的内容。
- **CI/CD 部署** 从 `main` 的干净 git 检出运行，因此 staging 和 prod 始终与已提交内容完全一致。若要依赖与 git 一致的行为，请先提交。

---

## 配置步骤

### 1. 配置工作区 host

工作区 host 在 `databricks.yml` 中每个目标的 `workspace.host` 下设置。目前三个目标都指向同一个工作区 — 如果你要把 staging/prod 分到不同工作区，请自行修改。

### 2. 添加 GitHub Secrets

进入 **仓库 Settings → Secrets and variables → Actions**，添加：

| Secret | 值 |
|--------|-----|
| `DATABRICKS_STAGING_HOST` | Staging 工作区 URL |
| `DATABRICKS_STAGING_TOKEN` | Staging 工作区 PAT |
| `DATABRICKS_PROD_HOST` | Prod 工作区 URL |
| `DATABRICKS_PROD_TOKEN` | Prod 工作区 PAT（或使用 OIDC） |

### 3. 创建 `production` GitHub Environment

进入 **仓库 Settings → Environments → New environment** → 命名为 `production` → 添加 **Required reviewers（必需审批人）**。

这会在任何 prod 部署之前创建人工审批门禁。

### 4. 本地部署（dev）

```bash
# 安装 CLI
brew tap databricks/tap && brew install databricks

# 认证
databricks auth login --host https://YOUR-WORKSPACE.cloud.databricks.com

# 校验（默认使用 dev 目标）
databricks bundle validate

# 部署你的个人 dev 副本
databricks bundle deploy

# 在 dev 上手动运行作业
databricks bundle run amer_trade_orders_job
```

---

## 演示要点

1. **一个仓库，覆盖所有环境** — 同一份 `databricks.yml` 通过按目标覆盖变量，驱动 dev、staging 和 prod
2. **`mode: development`** — 自动加前缀、暂停调度、隔离路径；开发者之间不会冲突
3. **作业 = 管道 + notebook** — 任务 1 运行 DLT，任务 2 在 serverless 上运行校验 notebook；DABs 自动串联依赖关系
4. **PR 门禁** — `bundle validate -t staging` 在代码合入 main 之前捕获 YAML 错误、缺失引用和 schema 违规
5. **prod 人工审批** — GitHub Environments 提供晋级门禁，无需额外工具
6. **回滚** — 回退提交并重新运行部署工作流；DABs 会重新部署到之前的状态

---

## 参考资料

- [Databricks Asset Bundles 文档](https://docs.databricks.com/dev-tools/bundles/index.html)
- [Databricks 上的 CI/CD](https://docs.databricks.com/dev-tools/ci-cd/)
- [STS DABs 演示仓库（内部）](https://github.com/databricks-field-eng/sts-dabs-demo)
