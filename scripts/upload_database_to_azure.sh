#!/bin/bash
# Upload local SQLite database to Azure Files
# This script uploads your local soa_builder_web.db to Azure for deployment

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration - UPDATE THESE VALUES
RESOURCE_GROUP="rg-soa-workbench-prod"
STORAGE_ACCOUNT="stwbdata<uniqueid>"  # Replace <uniqueid> with your storage account suffix
FILE_SHARE="soa-workbench-data"
LOCAL_DB="soa_builder_web.db"

echo -e "${GREEN}=== Azure Database Upload Script ===${NC}"
echo ""

# Check if local database exists
if [ ! -f "$LOCAL_DB" ]; then
    echo -e "${RED}Error: Local database file '$LOCAL_DB' not found${NC}"
    echo "Please run this script from the project root directory where soa_builder_web.db is located"
    exit 1
fi

# Check if Azure CLI is installed
if ! command -v az &> /dev/null; then
    echo -e "${RED}Error: Azure CLI is not installed${NC}"
    echo "Please install Azure CLI first:"
    echo "  macOS: brew install azure-cli"
    echo "  Linux: curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash"
    echo "  Windows: Download from https://aka.ms/installazurecli"
    exit 1
fi

# Create backup before upload
echo -e "${YELLOW}Creating backup of local database...${NC}"
BACKUP_FILE="$LOCAL_DB.backup.$(date +%Y%m%d_%H%M%S)"
cp "$LOCAL_DB" "$BACKUP_FILE"
echo -e "${GREEN}✓ Backup created: $BACKUP_FILE${NC}"
echo ""

# Check if logged in to Azure
echo "Checking Azure login status..."
if ! az account show &> /dev/null; then
    echo -e "${YELLOW}Not logged in to Azure. Opening login...${NC}"
    az login
fi

SUBSCRIPTION=$(az account show --query name -o tsv)
echo -e "${GREEN}✓ Logged in to Azure subscription: $SUBSCRIPTION${NC}"
echo ""

# Get storage account key
echo "Retrieving storage account key..."
STORAGE_KEY=$(az storage account keys list \
    --resource-group "$RESOURCE_GROUP" \
    --account-name "$STORAGE_ACCOUNT" \
    --query '[0].value' \
    --output tsv 2>&1)

if [ $? -ne 0 ]; then
    echo -e "${RED}Error: Failed to retrieve storage account key${NC}"
    echo "Please verify:"
    echo "  1. Resource group '$RESOURCE_GROUP' exists"
    echo "  2. Storage account '$STORAGE_ACCOUNT' exists"
    echo "  3. You have permission to access the storage account"
    exit 1
fi

echo -e "${GREEN}✓ Storage account key retrieved${NC}"
echo ""

# Get database file size
DB_SIZE=$(du -h "$LOCAL_DB" | cut -f1)
echo "Database file size: $DB_SIZE"
echo ""

# Upload database
echo -e "${YELLOW}Uploading database to Azure Files...${NC}"
echo "This may take a few moments depending on file size..."

az storage file upload \
    --account-name "$STORAGE_ACCOUNT" \
    --account-key "$STORAGE_KEY" \
    --share-name "$FILE_SHARE" \
    --source "$LOCAL_DB" \
    --path soa_builder_web.db \
    --no-progress

if [ $? -ne 0 ]; then
    echo -e "${RED}Error: Failed to upload database${NC}"
    exit 1
fi

echo -e "${GREEN}✓ Database uploaded successfully!${NC}"
echo ""

# Verify upload
echo "Verifying upload..."
az storage file list \
    --account-name "$STORAGE_ACCOUNT" \
    --account-key "$STORAGE_KEY" \
    --share-name "$FILE_SHARE" \
    --output table | grep soa_builder_web.db

echo ""
echo -e "${GREEN}=== Upload Complete ===${NC}"
echo ""
echo "Next steps:"
echo "  1. Restart your Azure Web App to use the new database"
echo "  2. Verify data is accessible at your app URL"
echo ""
echo "Backup location: $BACKUP_FILE"
