# Nora AI Agent - Development Progress

## CURRENT ISSUE
Avatar says "give me a moment to browse" but `browse_and_act` tool never executes. Orgo browser doesn't launch.

## COMPLETED CHANGES

1. **PiP Layout** (`frontend/components/AvatarRoom.tsx`): Avatar shrinks to 256x192 bottom-right when browser active
2. **Status Messages** (`agent/agent.py`): Added `browser_task_started`/`browser_task_completed` messages
3. **Explicit Dispatch** (`frontend/app/api/token/route.ts`): Added `RoomAgentDispatch` with `agentName: "nora-voice-agent"`
4. **Orgo Fix**: Patched `/opt/homebrew/lib/python3.11/site-packages/orgo/prompt.py` f-string bug

## DEBUG NEXT
1. Restart both services
2. Check agent logs for "Agent connecting to room" and "Connected to Orgo Computer"
3. If no room connection → dispatch still broken
4. If Orgo fails → check ORGO_API_KEY env var

## COMMANDS
```bash
# Agent
cd /Users/ranax/Downloads/nora/agent && /opt/homebrew/bin/python3.11 agent.py dev

# Frontend
cd /Users/ranax/Downloads/nora/frontend && npm run dev
```

## KEY FILES
- `agent/agent.py` - BrowserTools class with browse_and_act
- `frontend/components/AvatarRoom.tsx` - PiP layout
- `frontend/app/api/token/route.ts` - Token with agent dispatch
