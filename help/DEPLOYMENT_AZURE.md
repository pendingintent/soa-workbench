# Azure Deployment Plan for SoA Workbench

## Context

The SoA Workbench is a FastAPI web application for clinical trial Schedule of Activities management. It currently runs locally using SQLite for persistence and serves HTML UI via Jinja2 templates. The application needs to be deployed to Azure for production use with:

- **Persistent data storage** (SQLite database must survive deployments)
- **Environment configuration** (CDISC API keys, database paths)
- **Static asset serving** (CSS, images, help files - 3.5 MB total)
- **External API access** (CDISC Library API for biomedical concepts)

Current state: No Docker configuration exists. Application runs via `uvicorn` on port 8000. Dependencies managed via pip/requirements.txt.

## Quick Start: Deploying with Existing Data

If you have an existing local database with data you want to preserve:

1. **First**: Follow Azure resource creation steps (sections 1-9)
2. **Then**: Upload your local database to Azure Files (see Phase 5: Database Migration)
3. **Finally**: Deploy the application code via GitHub Actions

Your local `soa_builder_web.db` will be uploaded to Azure Files and automatically used by the deployed application.

## Recommended Approach: Azure App Service (Web App for Linux)

**Why Azure App Service:**
- Native Python support (no Dockerfile needed initially)
- Built-in persistent storage via Azure Files
- Automatic HTTPS/SSL
- Easy environment variable configuration
- Integrated with GitHub Actions (existing CI pipeline)
- Cost-effective for single-instance applications
- Built-in logging and monitoring

**Why NOT Azure Container Apps or AKS:**
- Container Apps: Requires containerization (extra complexity)
- AKS: Significant operational overhead for a single web app
- Both are overkill for this application's scale

## Azure Resource Creation Steps (Manual via Portal)

**Prerequisites:**
- Azure subscription with Contributor access
- Azure Portal access (portal.azure.com)

**Step-by-step resource creation:**

### 1. Create Resource Group
- Navigate to: Home → Resource groups → Create
- Subscription: Select your subscription
- Resource group name: `rg-soa-workbench-prod`
- Region: `East US` (or your preferred region)
- Click: Review + create → Create

### 2. Create Storage Account (for SQLite persistence)
- Navigate to: Resource group → Add → Storage account
- Basics:
  - Storage account name: `stwbdata<uniqueid>` (must be globally unique, lowercase, no hyphens)
  - Region: Same as resource group
  - Performance: Standard
  - Redundancy: Locally-redundant storage (LRS)
- Advanced: Leave defaults
- Create → Wait for deployment

- After creation:
  - Go to: Storage account → File shares → + File share
  - Name: `soa-workbench-data`
  - Tier: Transaction optimized
  - Create

### 3. Create App Service Plan
- Navigate to: Resource group → Add → App Service Plan
- Basics:
  - Name: `plan-soa-workbench`
  - Operating System: Linux
  - Region: Same as resource group
  - Pricing tier: B1 (Basic - $13/month) or P1v2 (Production - $78/month)
- Create

### 4. Create Web App (App Service)
- Navigate to: Resource group → Add → Web App
- Basics:
  - Name: `app-soa-workbench` (must be globally unique - becomes app-soa-workbench.azurewebsites.net)
  - Publish: Code
  - Runtime stack: Python 3.13
  - Operating System: Linux
  - Region: Same as resource group
  - App Service Plan: Select `plan-soa-workbench` created above
- Deployment: Enable GitHub Actions (configure later)
- Networking: Leave defaults (public access)
- Monitoring: Enable Application Insights (optional but recommended)
- Create → Wait for deployment

### 5. Configure Storage Mount in Web App
- Navigate to: Web App → Settings → Configuration → Path mappings
- Click: + New Azure Storage Mount
  - Name: `data`
  - Configuration options: Advanced edit
  - Storage accounts: Select `stwbdata<uniqueid>`
  - Storage type: Azure Files
  - Share name: `soa-workbench-data`
  - Mount path: `/mnt/data`
- Save → Restart app

### 6. Create Key Vault (for secrets)
- Navigate to: Resource group → Add → Key Vault
- Basics:
  - Key vault name: `kv-soa-workbench`
  - Region: Same as resource group
  - Pricing tier: Standard
- Access configuration:
  - Permission model: Azure role-based access control
- Create

- After creation:
  - Go to: Key Vault → Secrets → + Generate/Import
  - Create two secrets:
    1. Name: `CDISC-API-KEY`, Value: Your CDISC API key
    2. Name: `CDISC-SUBSCRIPTION-KEY`, Value: Your CDISC subscription key

### 7. Configure App Service Managed Identity
**What this does:** Allows your Web App to securely access Key Vault secrets without storing credentials.

**Step 7a: Enable Managed Identity on Web App**
- Navigate to: Web App → Settings → Identity
- System assigned tab: 
  - Status = **On** → Save
  - Wait for confirmation message
  - Copy the **Object (principal) ID** (looks like: `12345678-1234-1234-1234-123456789abc`)
  - Note: This ID uniquely identifies your web app to Azure services

**Step 7b: Grant Key Vault Access to Web App**
- Navigate to: Key Vault (`kv-soa-workbench`) → Access control (IAM)
- Click: **+ Add** → **Add role assignment**
- Role tab:
  - Search for: `Key Vault Secrets User`
  - Select it → Click: **Next**
- Members tab:
  - Assign access to: **Managed identity**
  - Click: **+ Select members**
  - In the side panel:
    - Managed identity: Select **App Service** from dropdown
    - Select: Your web app (`app-soa-workbench`) - it will show the same Object ID from Step 7a
  - Click: **Select** (closes side panel)
  - Click: **Next**
- Conditions tab:
  - Leave default (no conditions needed) → Click: **Next**
- Review + assign tab:
  - Review settings → Click: **Review + assign**
  - Wait for "Role assignment added" confirmation

**Verify access:**
- Navigate back to: Key Vault → Access control (IAM) → Role assignments
- Filter by: Key Vault Secrets User role
- Should see: `app-soa-workbench` listed with type "Managed Identity"

### 8. Configure Web App Settings
- Navigate to: Web App → Settings → Environment variables → Application settings
- Add the following (+ New application setting):
  ```
  SOA_BUILDER_DB = /mnt/data/soa_builder_web.db
  CDISC_API_KEY = @Microsoft.KeyVault(SecretUri=https://kv-soa-workbench.vault.azure.net/secrets/CDISC-API-KEY/)
  CDISC_SUBSCRIPTION_KEY = @Microsoft.KeyVault(SecretUri=https://kv-soa-workbench.vault.azure.net/secrets/CDISC-SUBSCRIPTION-KEY/)
  PYTHONUNBUFFERED = 1
  SCM_DO_BUILD_DURING_DEPLOYMENT = true
  ```
- Save

- Navigate to: Web App → Settings → Configuration → General settings
  - Stack settings:
    - Stack: Python
    - Major version: Python 3.13
    - Minor version: (auto-select latest)
    - Startup Command: `bash startup.sh`
  - Platform settings:
    - Always On: On (prevents cold starts - requires Basic tier or higher)
    - HTTP version: 2.0
  - Save → Restart

### 9. Configure Deployment Center
- Navigate to: Web App → Deployment → Deployment Center
- Source: GitHub
- Organization: Your GitHub username
- Repository: `soa-workbench`
- Branch: `master`
- Workflow option: Use existing workflow (we'll create `.github/workflows/azure-deploy.yml`)
- Save

- Download publish profile:
  - Click: Download publish profile → Save the file
  - Content of this file will be added to GitHub secrets

## Implementation Steps

### Phase 1: Create Deployment Assets

**1.1 Create startup script for Azure App Service:**

File: `startup.sh` (project root)
```bash
#!/bin/bash
# Ensure database directory exists
mkdir -p /mnt/data

# Run database migrations (handled by app lifespan)
# Start gunicorn with uvicorn worker
gunicorn soa_builder.web.app:app \
    --bind 0.0.0.0:8000 \
    --workers 1 \
    --worker-class uvicorn.workers.UvicornWorker \
    --timeout 120 \
    --access-logfile - \
    --error-logfile -
```

This script will be configured in Azure App Service as the startup command.

**1.2 Create GitHub Actions deployment workflow:**

File: `.github/workflows/azure-deploy.yml`

Key workflow components:
- **Trigger:** Automatic deployment when PRs are merged to `master` branch
  - Deployment is triggered by GitHub merge (not local push)
  - Works with PR-based workflow: develop on feature branches → create PR → merge on GitHub → auto-deploy
  - Also triggers on `release-*` branches for releases
  
- **Build job:**
  - Checkout code with submodules (`--recurse-submodules` for cdisc-json-validation)
  - Set up Python 3.13
  - Install dependencies: `pip install -e ".[dev]"`
  - Run tests: `pytest -q tests --disable-warnings`
  - Fail deployment if tests fail
  
- **Deploy job:**
  - Use `azure/webapps-deploy@v2` action
  - Authenticate with publish profile (stored in GitHub secrets)
  - Deploy package to Azure App Service
  - No need to build artifacts - Azure will handle pip install

- **GitHub Secrets required:**
  - `AZURE_WEBAPP_NAME`: Name of the Azure App Service
  - `AZURE_WEBAPP_PUBLISH_PROFILE`: Download from Azure Portal (Deployment Center → Download publish profile)

- **Environment:** Create a "production" environment in GitHub for manual approvals (optional but recommended)

**1.3 Update `.gitignore`:**
- Add `.azure/` directory exceptions for deployment configs
- Ensure `soa_builder_web.db` is NOT committed

### Phase 2: Security Configuration

**2.1 Key Vault Setup:****
- Create Key Vault: `kv-soa-workbench`
- Store secrets:
  - `CDISC-API-KEY`
  - `CDISC-SUBSCRIPTION-KEY`
- Grant App Service managed identity access to Key Vault

**2.2 App Service Authentication (optional):**
- Enable Azure AD authentication if needed
- Configure allowed users/groups

**2.3 Network Security:**
- Enable App Service diagnostic logs
- Configure Azure Monitor alerts for errors
- Set up Application Insights for performance monitoring

### Phase 3: Deployment Pipeline

**3.1 Configure GitHub Repository Settings:**

**Step 3.1a: Add Repository Secrets**

Navigate to: GitHub repository → **Settings** tab → **Secrets and variables** → **Actions**

Click: **New repository secret** and add each of the following:

1. **AZURE_WEBAPP_NAME**
   - Name: `AZURE_WEBAPP_NAME`
   - Value: `app-soa-workbench` (or your actual web app name)
   - Click: **Add secret**

2. **AZURE_WEBAPP_PUBLISH_PROFILE**
   - Name: `AZURE_WEBAPP_PUBLISH_PROFILE`
   - Value: Open the `.PublishSettings` file you downloaded from Azure Portal
   - Copy the **entire XML content** (starts with `<publishData>`, ends with `</publishData>`)
   - Paste it into the Value field
   - Click: **Add secret**

**Verify secrets added:**
- You should see both secrets listed (values are hidden)
- If you need to update, click the secret name → **Update secret**

**Step 3.1b: Create Production Environment (Optional but Recommended)**

This adds a manual approval step before deployment to production.

1. Navigate to: GitHub repository → **Settings** tab → **Environments**
2. Click: **New environment**
3. Name: `production` (must be lowercase, match workflow file)
4. Click: **Configure environment**

**Configure protection rules:**

5. **Required reviewers**
   - Check: ✅ **Required reviewers**
   - Click: Search field and select yourself (and/or team members)
   - This means: deployment pauses until someone approves

6. **Deployment branches** (optional)
   - Click: **Deployment branches** dropdown → **Selected branches**
   - Click: **Add deployment branch rule**
   - Enter: `master`
   - This means: only master branch can deploy to production

7. **Wait timer** (optional)
   - Uncheck (not needed unless you want a mandatory delay)

8. Click: **Save protection rules** at the top

**What this does:**
- When PR is merged, workflow runs tests
- After tests pass, deployment **pauses** 
- GitHub shows: "Review pending deployments"
- You (or approved reviewer) click **Review deployments** → Select `production` → **Approve and deploy**
- Deployment continues to Azure

**To skip approval (not recommended):**
- Either don't create the environment
- Or comment out `environment: production` in `.github/workflows/azure-deploy.yml`

**Step 3.1c: Enable Actions (if needed)**

If this is your first GitHub Actions workflow:
1. Navigate to: GitHub repository → **Actions** tab
2. If you see "Get started with GitHub Actions": Click **I understand my workflows**
3. Or if asked to enable: Click **Enable Actions**

**3.2 Development and Deployment Workflow:**

**Standard workflow (recommended):**
1. **Develop locally** on feature branch (e.g., `feature/new-endpoint`)
2. **Push feature branch** to GitHub: `git push origin feature/new-endpoint`
3. **Create Pull Request** on GitHub to merge into `master`
4. **Review and merge PR** on GitHub (no local push to master needed)
5. **Automatic deployment** triggers when PR is merged to `master`
6. **Monitor deployment** in GitHub Actions tab

**Key points:**
- ✅ Never push to `master` from local - only merge PRs on GitHub
- ✅ Tests run automatically before deployment (must pass)
- ✅ Deployment happens automatically after PR merge
- ✅ Use "production" environment for manual approval gate (optional)

**3.3 Deployment Steps (automatic after PR merge):**
1. GitHub detects push to `master` (from PR merge)
2. GitHub Actions workflow starts
3. Checkout code with submodules
4. Set up Python 3.13
5. Install dependencies (`pip install -e ".[dev]"`)
6. Run tests (`pytest`) - deployment fails if tests fail
7. Deploy to Azure App Service
8. Verify deployment (health check)

### Phase 4: Database Migration (Upload Existing Local Database)

**4.1 Backup Local Database:**
Before uploading, create a backup of your local database:
```bash
# Create backup with timestamp
cp soa_builder_web.db soa_builder_web.db.backup.$(date +%Y%m%d_%H%M%S)

# Also backup WAL and SHM files if they exist
cp soa_builder_web.db-wal soa_builder_web.db-wal.backup.$(date +%Y%m%d_%H%M%S) 2>/dev/null || true
cp soa_builder_web.db-shm soa_builder_web.db-shm.backup.$(date +%Y%m%d_%H%M%S) 2>/dev/null || true
```

**4.2 Upload Database to Azure Files:**

**Quick Method: Using Upload Script (Recommended)**
```bash
# Edit the script first to set your storage account name
# Open scripts/upload_database_to_azure.sh and update:
# STORAGE_ACCOUNT="stwbdata<uniqueid>"

# Run the upload script
./scripts/upload_database_to_azure.sh
```

The script will:
- Create a timestamped backup of your local database
- Verify Azure CLI is installed and logged in
- Upload the database to Azure Files
- Verify the upload was successful

**Option A: Using Azure Portal (GUI method)**
1. Navigate to: Storage Account (`stwbdata<uniqueid>`) → File shares → `soa-workbench-data`
2. Click: Upload
3. Select your local `soa_builder_web.db` file
4. Click: Upload
5. Verify file appears in the file share

**Option B: Using Azure CLI (recommended for automation)**
```bash
# Install Azure CLI if needed
# brew install azure-cli  # macOS
# or download from https://aka.ms/installazurecli

# Login to Azure
az login

# Set variables (replace with your actual values)
RESOURCE_GROUP="rg-soa-workbench-prod"
STORAGE_ACCOUNT="stwbdata<uniqueid>"
FILE_SHARE="soa-workbench-data"
LOCAL_DB="soa_builder_web.db"

# Get storage account key
STORAGE_KEY=$(az storage account keys list \
    --resource-group $RESOURCE_GROUP \
    --account-name $STORAGE_ACCOUNT \
    --query '[0].value' \
    --output tsv)

# Upload database file
az storage file upload \
    --account-name $STORAGE_ACCOUNT \
    --account-key $STORAGE_KEY \
    --share-name $FILE_SHARE \
    --source $LOCAL_DB \
    --path soa_builder_web.db

# Verify upload
az storage file list \
    --account-name $STORAGE_ACCOUNT \
    --account-key $STORAGE_KEY \
    --share-name $FILE_SHARE \
    --output table
```

**Option C: Using Azure Storage Explorer (GUI tool)**
1. Download Azure Storage Explorer: https://azure.microsoft.com/features/storage-explorer/
2. Sign in with your Azure account
3. Navigate to: Storage Accounts → `stwbdata<uniqueid>` → File Shares → `soa-workbench-data`
4. Click: Upload → Upload Files
5. Select `soa_builder_web.db` from your local directory
6. Click: Upload

**4.3 Verify Database Upload:**
After upload, check the file is accessible:
- Navigate to: Web App → Development Tools → SSH
- Click: Go
- Run command: `ls -lh /mnt/data/soa_builder_web.db`
- Should show file size and timestamp

**4.4 Database Initialization:**
- If starting fresh (no upload): First deployment will create database automatically
- If uploaded existing database: Migrations will run on startup (via app.py lifespan) to ensure schema is up-to-date
- Verify database exists at `/mnt/data/soa_builder_web.db`

**⚠️ Important Notes:**
- **WAL mode files**: SQLite creates `-wal` and `-shm` files in WAL mode. These will be recreated automatically by Azure; you only need to upload the main `.db` file.
- **Database locking**: Ensure your local app is stopped before uploading the database to avoid corruption.
- **Schema migrations**: The app will automatically run any pending migrations on startup, so your local database will be updated to match the latest schema.

**4.5 Smoke Testing:**
- Access application URL: `https://app-soa-workbench.azurewebsites.net`
- Verify homepage loads
- Create a test SoA
- Test CDISC API integration (biomedical concepts page)

**5.3 Monitoring Setup:**
- Configure Log Analytics workspace
- Set up alerts for:
  - HTTP 5xx errors
  - Response time > 5s
  - CPU/Memory > 80%

## Deployment Architecture

**Azure App Service with Python 3.13 runtime:**
```
┌─────────────────────────────────────┐
│   GitHub Actions (CI/CD)            │
│   - Run tests                       │
│   - Deploy on push to master        │
└──────────────┬──────────────────────┘
               │
               ▼
┌─────────────────────────────────────┐
│   Azure App Service (Linux)         │
│   - Python 3.13 runtime             │
│   - Gunicorn + Uvicorn workers      │
│   - Port 8000 (mapped to 443)       │
│   - Environment variables from      │
│     Key Vault                       │
└──────────────┬──────────────────────┘
               │
               ▼
┌─────────────────────────────────────┐
│   Azure Files (Persistent Storage)  │
│   - Mounted at /mnt/data            │
│   - SQLite database file            │
│   - Survives deployments            │
└─────────────────────────────────────┘
```

## Critical Files to Create

### 1. `startup.sh` (project root)
**Purpose:** Azure App Service startup script for running the application

**Content:**
```bash
#!/bin/bash
set -e

echo "Starting SoA Workbench deployment..."

# Ensure database directory exists
mkdir -p /mnt/data

# Display Python version for debugging
python --version

# Display environment (sanitized)
echo "Database path: $SOA_BUILDER_DB"
echo "Mount check: $(ls -la /mnt/data 2>&1 || echo 'Mount not available')"

# Start gunicorn with uvicorn worker
echo "Starting gunicorn..."
gunicorn soa_builder.web.app:app \
    --bind 0.0.0.0:8000 \
    --workers 1 \
    --worker-class uvicorn.workers.UvicornWorker \
    --timeout 120 \
    --access-logfile - \
    --error-logfile - \
    --log-level info
```

### 2. `.github/workflows/azure-deploy.yml`
**Purpose:** Automated deployment pipeline from GitHub to Azure

**Content:**
```yaml
name: Deploy to Azure App Service

on:
  push:
    branches:
      - master       # Triggers when PRs are merged to master
      - release-*    # Also triggers for release branches
  workflow_dispatch:  # Allow manual triggers from GitHub UI if needed

permissions:
  contents: read

jobs:
  build-and-test:
    name: Build and Test
    runs-on: ubuntu-latest
    
    steps:
      - name: Checkout code
        uses: actions/checkout@v4
        with:
          submodules: recursive  # Include cdisc-json-validation submodule
      
      - name: Set up Python 3.13
        uses: actions/setup-python@v5
        with:
          python-version: '3.13'
          cache: 'pip'
          cache-dependency-path: |
            pyproject.toml
            requirements.txt
      
      - name: Install dependencies
        run: |
          python -m pip install --upgrade pip
          pip install -e ".[dev]"
      
      - name: Run tests
        run: |
          pytest -q tests --disable-warnings --tb=short
        env:
          CDISC_CONCEPTS_JSON: '[]'  # Bypass CDISC API for tests
      
      - name: Create deployment package
        run: |
          # Nothing special needed - Azure will handle pip install
          echo "Build successful"

  deploy:
    name: Deploy to Azure
    needs: build-and-test
    runs-on: ubuntu-latest
    environment: production  # Optional: requires manual approval
    
    steps:
      - name: Checkout code
        uses: actions/checkout@v4
        with:
          submodules: recursive
      
      - name: Deploy to Azure Web App
        uses: azure/webapps-deploy@v2
        with:
          app-name: ${{ secrets.AZURE_WEBAPP_NAME }}
          publish-profile: ${{ secrets.AZURE_WEBAPP_PUBLISH_PROFILE }}
          package: .
      
      - name: Verify deployment
        run: |
          echo "Deployment complete!"
          echo "App URL: https://${{ secrets.AZURE_WEBAPP_NAME }}.azurewebsites.net"
```

**Required GitHub Secrets:**
- `AZURE_WEBAPP_NAME`: The name of your Azure Web App (e.g., `app-soa-workbench`)
- `AZURE_WEBAPP_PUBLISH_PROFILE`: Contents of the publish profile downloaded from Azure Portal

### 3. `.gitignore` updates
**Add these lines:**
```gitignore
# Azure deployment
*.publish-settings
*.PublishSettings
.azure/

# Production database (never commit)
soa_builder_web.db
soa_builder_web.db-wal
soa_builder_web.db-shm

# Local environment
.env
```

### 4. `requirements.txt` update
**Add gunicorn dependency:**
```
# ... existing dependencies ...
gunicorn>=21.0.0
```

## Files to Modify/Create

### Files to Create:
1. `.github/workflows/azure-deploy.yml` - Deployment pipeline
2. `startup.sh` - Custom startup script for App Service
3. `docs/DEPLOYMENT_AZURE.md` - This deployment runbook

### Files to Update:
1. `.gitignore` - Add Azure-specific ignores (`*.publish-settings`, `.azure/`)
2. `requirements.txt` - Add `gunicorn>=21.0.0` for production WSGI server
3. `README.md` - Add "Deployment to Azure" section with link to this doc
4. `pyproject.toml` - Ensure Python >=3.9 compatibility statement is accurate

### Files NOT to Change:
- `src/soa_builder/web/app.py` - Already configured correctly
- `src/soa_builder/web/db.py` - Environment variable handling works as-is
- `pyproject.toml` - Dependencies are correct

## Verification Steps

**Pre-deployment checks:**
1. Run `pytest` - all tests must pass
2. Test locally with production-like config:
   ```bash
   export SOA_BUILDER_DB=/tmp/test.db
   export CDISC_API_KEY=<real-key>
   soa-builder-web
   ```
3. Verify static files load correctly
4. Test CDISC API integration

**Post-deployment verification:**
1. Access Azure app URL
2. **If uploaded existing database:** Verify your existing SoAs and data are visible
3. **If fresh database:** Create a new SoA (tests database writes)
4. View biomedical concepts (tests CDISC API)
5. Export USDM JSON (tests complex operations)
6. Check Azure logs for errors
7. Verify database persistence (redeploy and check data survives)

## Cost Estimate (Azure)

**Monthly costs (approximate):**
- App Service Plan (B1): ~$13/month
- Azure Files (10 GB): ~$2/month
- Bandwidth: ~$5/month
- **Total: ~$20/month** (dev/test)

**Production tier:**
- App Service Plan (P1v2): ~$78/month
- Azure Files (10 GB): ~$2/month
- Application Insights: ~$10/month
- **Total: ~$90/month**

## Risk Mitigation

**SQLite limitations on Azure:**
- Single-writer limitation (App Service single instance OK)
- No write scaling (consider Azure SQL if multi-instance needed)
- Backup strategy: Azure Files snapshots or manual exports

**Environment variables:**
- Use Azure Key Vault references, not plain text
- Never commit .env files with real keys

**Deployment failures:**
- Always run tests before deploy
- Use deployment slots for zero-downtime (P1v2 tier)
- Keep previous version for quick rollback

## Deployment Workflow Summary

### Initial Setup (One-time)
1. ✅ Create Azure resources (sections 1-9 above)
2. ✅ Upload your local database to Azure Files (Phase 5)
3. ✅ Create `startup.sh` and `.github/workflows/azure-deploy.yml`
4. ✅ Configure GitHub secrets (AZURE_WEBAPP_NAME, AZURE_WEBAPP_PUBLISH_PROFILE)
5. ✅ Push deployment files to GitHub

### Ongoing Development & Deployment

**Your complete workflow:**

1. **Local development:**
   - Create feature branch: `git checkout -b feature/my-change`
   - Make changes and test locally: `pytest && soa-builder-web`
   - Commit changes: `git commit -m "Add new feature"`
   - Push branch: `git push origin feature/my-change`

2. **Create Pull Request on GitHub:**
   - Go to GitHub repository
   - Click: **Pull requests** → **New pull request**
   - Base: `master` ← Compare: `feature/my-change`
   - Click: **Create pull request**
   - Add description of changes
   - (Optional) Request review from team member

3. **Merge and Deploy:**
   - Review the changes (or wait for peer review)
   - Click: **Merge pull request** → **Confirm merge**
   - **GitHub Actions automatically triggers deployment**
   
4. **Monitor Deployment:**
   - Go to: GitHub → **Actions** tab
   - Click on the latest workflow run (should be running)
   - Watch progress:
     - ✅ "Build and Test" job (runs pytest)
     - ⏸️ "Deploy to Azure" job (waits for approval if environment configured)
     - ✅ Deployment completes
   
5. **Approve Deployment (if production environment configured):**
   - During "Deploy to Azure" job, you'll see: **Review pending deployments**
   - Click: **Review deployments**
   - Select: ✅ `production`
   - Click: **Approve and deploy**
   - Deployment continues to Azure
   
6. **Verify Deployment:**
   - Wait for workflow to complete (green checkmark)
   - Visit: `https://app-soa-workbench.azurewebsites.net`
   - Verify your changes are live
   - Check Azure logs if issues

### Quick Reference Commands

**Local development:**
```bash
# Start working on new feature
git checkout -b feature/my-feature

# Test locally
pytest
soa-builder-web

# Push to GitHub (does NOT deploy)
git add .
git commit -m "Implement feature"
git push origin feature/my-feature
```

**Deploy to Azure:**
```bash
# No local commands needed!
# 1. Merge PR on GitHub
# 2. GitHub Actions automatically runs tests
# 3. If production environment configured:
#    - Go to GitHub → Actions tab
#    - Click on the running workflow
#    - Click "Review deployments" button
#    - Select "production" → Click "Approve and deploy"
# 4. If no environment: deployment happens automatically after tests pass
```

**Monitor deployment:**
```bash
# 1. Go to GitHub → Actions tab
# 2. Click on the latest workflow run
# 3. Watch "Build and Test" and "Deploy to Azure" jobs
# 4. Green checkmark = success
# 5. Red X = failure (click to see logs)
```

**Manual deployment (workflow_dispatch):**
```bash
# Use when you need to redeploy without a new commit
# 1. Go to GitHub → Actions tab
# 2. Click "Deploy to Azure App Service" workflow (left sidebar)
# 3. Click "Run workflow" button (right side)
# 4. Select branch: master
# 5. Click green "Run workflow" button
```

## Database Backup and Restore

### Creating Backups

**Automated backup via Azure Files snapshots:**
1. Navigate to: Storage Account → File shares → `soa-workbench-data`
2. Click: Snapshots → + Snapshot
3. Add description: "Manual backup before deployment"
4. Click: Create

**Schedule automated snapshots:**
- Set up Azure Backup for File Shares
- Navigate to: File share → Backup
- Configure backup policy (daily/weekly)

**Manual backup via download:**
```bash
# Using Azure CLI
az storage file download \
    --account-name $STORAGE_ACCOUNT \
    --account-key $STORAGE_KEY \
    --share-name $FILE_SHARE \
    --path soa_builder_web.db \
    --dest ./backups/soa_builder_web.db.$(date +%Y%m%d_%H%M%S)
```

### Restoring from Backup

**Restore from Azure Files snapshot:**
1. Navigate to: File share → Snapshots
2. Select snapshot to restore
3. Find `soa_builder_web.db` file
4. Click: Restore
5. Choose: Overwrite original file
6. Restart Web App

**Restore from local backup:**
- Follow the database upload steps in Phase 5
- Upload your backup file
- Restart Web App

## Troubleshooting

### Application won't start
- Check Azure logs: Web App → Monitoring → Log stream
- Verify startup command is correct: `bash startup.sh`
- Ensure Python version matches (3.13)
- Check environment variables are set

### Database connection errors
- Verify Azure Files mount is configured correctly
- Check `/mnt/data` is accessible (view logs)
- Ensure `SOA_BUILDER_DB` points to `/mnt/data/soa_builder_web.db`

### CDISC API failures
- Verify Key Vault secrets are accessible
- Check managed identity has Key Vault Secrets User role
- Test API keys are valid

### Performance issues
- Enable "Always On" to prevent cold starts
- Consider upgrading to P1v2 tier
- Check Application Insights for bottlenecks

## Next Steps

1. Follow Azure Resource Creation steps above
2. Create deployment files (startup.sh, azure-deploy.yml)
3. Configure GitHub secrets
4. Test deployment to Azure
5. Set up monitoring and alerts
