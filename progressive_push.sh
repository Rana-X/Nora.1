#!/bin/bash

# Progressive Git Push Script for Nora Open Source Release
# This script will create 12 commits spread over ~60 minutes (5 min intervals)

set -e  # Exit on error

PROJECT_DIR="/Users/ranax/Downloads/nora"
REMOTE_URL="git@github.com:Rana-X/Nora.1.git"
LOG_FILE="$PROJECT_DIR/push_progress.log"

# Color codes for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Logging function
log() {
    echo -e "${GREEN}[$(date '+%Y-%m-%d %H:%M:%S')]${NC} $1" | tee -a "$LOG_FILE"
}

log_info() {
    echo -e "${BLUE}[INFO]${NC} $1" | tee -a "$LOG_FILE"
}

log_wait() {
    echo -e "${YELLOW}[WAIT]${NC} $1" | tee -a "$LOG_FILE"
}

# Navigate to project directory
cd "$PROJECT_DIR"

log "Starting progressive git push process..."
log "This will take approximately 60 minutes to complete"

# Clear previous log
> "$LOG_FILE"

# Function to commit and push
commit_and_push() {
    local message="$1"
    shift
    local files=("$@")
    
    log "Creating commit: $message"
    git add "${files[@]}"
    git commit -m "$message"
    git push -u origin main
    log_info "Commit pushed successfully"
}

# Function to wait with countdown
wait_minutes() {
    local minutes=$1
    log_wait "Waiting $minutes minutes before next commit..."
    sleep $((minutes * 60))
}

# ============================================================
# PHASE 1: Foundation (0-10 minutes)
# ============================================================

log "=== PHASE 1: Foundation ==="

# Commit 1: Initial project setup
log_info "Commit 1/12: Initial project setup"
commit_and_push "Initial commit: Project structure and configuration" \
    ".gitignore" \
    "README.md"

wait_minutes 5

# Commit 2: Project documentation
log_info "Commit 2/12: Add project planning documentation"
commit_and_push "Add project planning and environment configuration" \
    ".env.example" \
    "plan.md"

wait_minutes 5

# ============================================================
# PHASE 2: Backend Foundation (10-25 minutes)
# ============================================================

log "=== PHASE 2: Backend Foundation ==="

# Commit 3: Backend requirements
log_info "Commit 3/12: Backend requirements"
commit_and_push "Add Python backend requirements and configuration" \
    "agent/requirements.txt" \
    "agent/.env.example"

wait_minutes 5

# Commit 4: Telegram service module
log_info "Commit 4/12: Telegram integration"
commit_and_push "Implement Telegram service and demo" \
    "agent/telegram_service.py" \
    "agent/telegram_demo.py"

wait_minutes 5

# Commit 5: Test utilities
log_info "Commit 5/12: Test utilities"
commit_and_push "Add Orgo browser testing utilities" \
    "agent/test_orgo.py"

wait_minutes 5

# ============================================================
# PHASE 3: Frontend Foundation (25-40 minutes)
# ============================================================

log "=== PHASE 3: Frontend Foundation ==="

# Commit 6: Frontend configuration
log_info "Commit 6/12: Frontend configuration"
commit_and_push "Initialize Next.js frontend with configuration" \
    "frontend/package.json" \
    "frontend/package-lock.json" \
    "frontend/tsconfig.json" \
    "frontend/next.config.ts" \
    "frontend/eslint.config.mjs" \
    "frontend/postcss.config.mjs" \
    "frontend/next-env.d.ts" \
    "frontend/.gitignore"

wait_minutes 5

# Commit 7: Frontend layout and styles
log_info "Commit 7/12: Frontend UI foundation"
commit_and_push "Add frontend layout, styles, and assets" \
    "frontend/app/layout.tsx" \
    "frontend/app/globals.css" \
    "frontend/app/favicon.ico" \
    "frontend/public/"

wait_minutes 5

# Commit 8: Frontend API routes
log_info "Commit 8/12: API routes"
commit_and_push "Implement LiveKit token generation API" \
    "frontend/app/api/token/route.ts"

wait_minutes 5

# ============================================================
# PHASE 4: Core Features (40-55 minutes)
# ============================================================

log "=== PHASE 4: Core Features ==="

# Commit 9: Frontend components
log_info "Commit 9/12: Core UI components"
commit_and_push "Add Avatar Room and Browser Display components" \
    "frontend/components/AvatarRoom.tsx" \
    "frontend/components/BrowserDisplay.tsx" \
    "frontend/app/page.tsx"

wait_minutes 5

# Commit 10: Main agent implementation
log_info "Commit 10/12: Core agent implementation"
commit_and_push "Implement LiveKit voice agent with Orgo integration" \
    "agent/agent.py"

wait_minutes 5

# ============================================================
# PHASE 5: Documentation & Polish (55-60 minutes)
# ============================================================

log "=== PHASE 5: Documentation & Polish ==="

# Commit 11: Documentation
log_info "Commit 11/12: Documentation"
commit_and_push "Add comprehensive project documentation" \
    "docs/"

wait_minutes 5

# Commit 12: Final polish (any remaining files)
log_info "Commit 12/12: Final touches"
# Check for any remaining untracked files
if [[ -n $(git status -s) ]]; then
    git add -A
    git commit -m "Final polish: Add remaining project files"
    git push -u origin main
    log_info "Final commit pushed successfully"
else
    log_info "No remaining files to commit"
fi

# ============================================================
# COMPLETE
# ============================================================

log "=== PUSH COMPLETE ==="
log "All commits have been pushed to $REMOTE_URL"
log "Total time elapsed: ~60 minutes"
log "Repository is ready for public viewing!"

echo ""
log_info "View your repository at: https://github.com/Rana-X/Nora.1"
