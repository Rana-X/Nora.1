#!/bin/bash

# Simple monitoring script to check progress

echo "=== Git Push Progress Monitor ==="
echo ""
echo "Script Status:"
ps aux | grep progressive_push.sh | grep -v grep || echo "Script not running"
echo ""
echo "Latest Commits:"
cd /Users/ranax/Downloads/nora
git log --oneline --all
echo ""
echo "Latest Log Entries:"
tail -15 push_output.log
echo ""
echo "=== End of Monitor ==="
