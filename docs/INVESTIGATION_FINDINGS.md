# Investigation: Orgo + Voice Agent + VNC Display Integration

**Date:** January 24, 2026  
**Status:** Analysis Complete - Issues Identified

## Overview

This investigation examines the integration of:
1. **OpenAI Realtime API** (voice agent with LiveKit)
2. **Orgo Computer** (remote VM for browser control)
3. **VNC Display** (showing Orgo VM in frontend via `orgo-vnc`)

## Architecture Summary

```
┌─────────────────────────────────────────────────────────────┐
│                     LiveKit Room                             │
│                                                              │
│  User Audio ─▶ OpenAI Realtime ─▶ Agent Response           │
│                    (STT+LLM+TTS)                            │
│                        │                                     │
│                        ▼                                     │
│              @function_tool methods                          │
│                   browse_and_act()                           │
│                        │                                     │
│                        ▼                                     │
│              Orgo Computer.prompt()                          │
│                 (Claude Sonnet 4.5)                         │
│                        │                                     │
│                        ▼                                     │
│              Browser Actions in VM                           │
│                                                              │
│  ┌──────────────────────────────────────────┐               │
│  │  Data Channel (publish_data)              │               │
│  │    - browser_ready (VNC credentials)     │               │
│  │    - browser_task_started                 │               │
│  │    - browser_task_completed               │               │
│  └──────────────────────────────────────────┘               │
│                        │                                     │
└────────────────────────│─────────────────────────────────────┘
                         │
                         ▼
              ┌────────────────────────┐
              │   Frontend React App    │
              │                         │
              │  - Listens for data     │
              │  - Receives VNC creds   │
              │  - Shows ComputerDisplay│
              │    via orgo-vnc package │
              └────────────────────────┘
                         │
                         ▼
              ┌────────────────────────┐
              │   Orgo VNC Server       │
              │   (Remote Desktop)      │
              └────────────────────────┘
```

## Key Documentation Findings

### 1. Orgo VNC Credentials (CRITICAL FINDING)

**From Orgo docs (`embed-vms`):**

VNC credentials must be obtained manually from the Orgo dashboard:
1. Go to https://www.orgo.ai/start
2. Open a workspace and select a computer
3. Click the **⋮** menu → **Computer Settings**
4. Copy the **Hostname** and **Password**

**These credentials are NOT returned by the API!**

```python
# This does NOT give you VNC credentials:
computer = Computer(computer_id="abc123")
print(computer.url)  # This is the API URL, not VNC hostname
```

**Current Implementation Issue:**
```python
# agent/agent.py lines 271-293
async def publish_vnc_credentials_with_retry(room, max_retries: int = 5, delay: float = 1.0):
    """VNC credentials are read from environment variables"""
    vnc_host = os.environ.get("ORGO_VNC_HOST")  # ✗ Must be set manually
    vnc_password = os.environ.get("ORGO_VNC_PASSWORD")  # ✗ Must be set manually
    
    if not vnc_host or not vnc_password:
        logger.warning("VNC credentials not configured")
        return False
```

### 2. OpenAI Realtime API Function Tools

**From LiveKit docs:**

Function tools with OpenAI Realtime API work identically to regular LiveKit agents:

```python
class NoraAgent(Agent):
    @function_tool()
    async def browse_and_act(self, context: RunContext, instruction: str) -> str:
        """Tool docstring becomes description for LLM"""
        # Tool execution code
        return result
```

**Key Points:**
- Tools decorated with `@function_tool()` are automatically registered
- OpenAI Realtime model can call these tools during conversation
- The `context: RunContext` parameter is required
- Tool execution is asynchronous

**Current Implementation:** ✓ Correct

### 3. LiveKit Data Channel

**From LiveKit docs:**

Publishing data to room participants:

```python
# Agent side - publish data
await room.local_participant.publish_data(
    json.dumps({"type": "browser_ready", "hostname": "...", "password": "..."}).encode(),
    reliable=True
)
```

```typescript
// Frontend side - receive data
room.on(RoomEvent.DataReceived, (payload: Uint8Array) => {
    const data = JSON.parse(new TextDecoder().decode(payload));
    // Handle data
});
```

**Current Implementation:** ✓ Correct

### 4. Orgo Computer.prompt()

**From Orgo docs:**

The hosted agent service for browser control:

```python
result = computer.prompt(
    instruction="Go to Amazon and add bananas to cart",
    model="claude-sonnet-4-5-20250929",
    max_iterations=30,  # Limit to prevent infinite loops
    verbose=True        # Show detailed logs
)
```

**Key Points:**
- Blocks for 10-30 seconds during execution
- Uses Claude Sonnet 4.5 for autonomous browser control
- Returns either string or list of messages
- Requires ORGO_API_KEY environment variable

**Current Implementation:** ✓ Correct

## Issues Identified

### Issue #1: Missing VNC Credentials in Environment ⚠️ CRITICAL

**Problem:**
```bash
# .env file likely missing:
ORGO_VNC_HOST=orgo-computer-xxxxx.orgo.dev
ORGO_VNC_PASSWORD=<password from dashboard>
```

**Evidence:**
- Agent logs will show: "VNC credentials not configured. Set ORGO_VNC_HOST and ORGO_VNC_PASSWORD"
- Frontend will show: "Waiting for browser... No VNC credentials received yet"
- `BrowserDisplay` component won't connect

**Solution:**
1. Log into https://www.orgo.ai/start
2. Select your computer
3. Click ⋮ menu → Computer Settings
4. Copy Hostname and Password
5. Add to `.env` and `agent/.env`:
```bash
ORGO_VNC_HOST=orgo-computer-<your-id>.orgo.dev
ORGO_VNC_PASSWORD=<your-password>
```

### Issue #2: VNC Credentials Published Before Frontend Ready

**Problem:**
```python
# agent.py line 377-378
await asyncio.sleep(0.5)  # Only 500ms delay
asyncio.create_task(publish_vnc_credentials_with_retry(ctx.room))
```

Race condition: Frontend React component may not have mounted and attached the `DataReceived` listener yet.

**Solution:**
Increase retry attempts and use exponential backoff:

```python
async def publish_vnc_credentials_with_retry(
    room, 
    max_retries: int = 10,  # Increase from 5
    initial_delay: float = 1.0,
    max_delay: float = 5.0
):
    delay = initial_delay
    for attempt in range(max_retries):
        try:
            await room.local_participant.publish_data(
                browser_data.encode(),
                reliable=True
            )
            logger.info(f"VNC credentials published (attempt {attempt + 1}/{max_retries})")
            
            # Exponential backoff
            await asyncio.sleep(min(delay, max_delay))
            delay *= 1.5  # Increase delay each time
            
        except Exception as e:
            logger.warning(f"Failed attempt {attempt + 1}: {e}")
            await asyncio.sleep(delay)
```

### Issue #3: Orgo Computer Initialization Error Handling

**Problem:**
```python
# agent.py lines 328-342
try:
    if computer_id:
        computer = Computer(computer_id=computer_id, api_key=api_key)
    else:
        computer = Computer(api_key=api_key)
except Exception as e:
    logger.warning(f"Failed to initialize Orgo Computer: {e}")
    logger.warning("Browser control will not be available")
```

No verification that the computer is actually running and accessible.

**Solution:**
Add a health check:

```python
try:
    if computer_id:
        computer = Computer(computer_id=computer_id, api_key=api_key)
        logger.info(f"Connected to existing Orgo Computer: {computer_id}")
    else:
        computer = Computer(api_key=api_key)
        logger.info(f"Created new Orgo Computer: {computer.computer_id}")
    
    # Health check - verify computer is accessible
    try:
        details = computer.api.get_computer(computer.computer_id)
        status = details.get('status', 'unknown')
        logger.info(f"Orgo Computer status: {status}")
        
        if status != 'running':
            logger.warning(f"Computer is not running (status: {status})")
            logger.warning("You may need to start it via the dashboard")
    except Exception as e:
        logger.warning(f"Could not verify computer status: {e}")
    
    logger.info(f"Orgo Computer URL: {computer.url}")
    
except Exception as e:
    logger.error(f"Failed to initialize Orgo Computer: {e}")
    computer = None
```

### Issue #4: OpenAI Realtime Function Tool Execution Not Guaranteed

**Problem:**
OpenAI Realtime API may not always call function tools, even when appropriate.

**Evidence:**
- User says "browse Amazon"
- Agent responds "Okay, I'm heading to Amazon..." but tool never executes
- This is a known behavior with Realtime API - it's conversational first

**Solution:**
Make the system prompt more explicit about tool usage:

```python
SYSTEM_PROMPT = """...

BROWSER TASKS - CRITICAL RULES:
1. When user mentions browsing, shopping, or web tasks, YOU MUST use the browse_and_act tool
2. ALWAYS call browse_and_act BEFORE saying you're done with a web task
3. Examples that require browse_and_act:
   - "Go to Amazon" → MUST call browse_and_act
   - "Search for bananas" → MUST call browse_and_act
   - "Add to cart" → MUST call browse_and_act
   - "Check the news" → MUST call browse_and_act

IMPORTANT - TOOL USAGE:
- Never say you're browsing if you haven't called the tool
- Never describe what you "did" on a website without actually calling the tool
- If browse_and_act fails, tell the user it failed - don't pretend it worked
"""
```

### Issue #5: Missing Computer ID Persistence

**Problem:**
If `ORGO_COMPUTER_ID` is not set, a new computer is created on every agent restart, accumulating costs.

**Solution:**
Save and reuse computer ID:

```python
# After creating new computer
if not computer_id:
    computer = Computer(api_key=api_key)
    new_id = computer.computer_id
    logger.info(f"Created new computer: {new_id}")
    logger.info(f"Add to .env: ORGO_COMPUTER_ID={new_id}")
    
    # Optionally save to file
    with open(".orgo_computer_id", "w") as f:
        f.write(new_id)
```

## Verification Checklist

Use this checklist to verify the integration is working:

### Agent Startup
- [ ] "Connected to existing Orgo Computer: [id]" appears in logs
- [ ] "Orgo Computer status: running" appears
- [ ] "VNC credentials published" appears (multiple times)
- [ ] No "VNC credentials not configured" warning

### Frontend Connection
- [ ] Browser console shows "[DEBUG] Received data message: browser_ready"
- [ ] Console shows VNC hostname (first 20 chars): `orgo-computer-xxxxx...`
- [ ] Console shows password is present: `hasPassword: true`
- [ ] "[BrowserDisplay] VNC connected!" appears in console

### Tool Execution
- [ ] User request: "Go to Amazon"
- [ ] Agent says: "Okay, I'm heading to Amazon. Give me a moment."
- [ ] Agent logs show: "Starting browser task: Go to Amazon..."
- [ ] Agent logs show: "Browser task completed successfully"
- [ ] Frontend shows "Browsing" badge
- [ ] VNC display shows browser activity
- [ ] Agent responds with summary after task completes

### Error Cases
- [ ] If VNC fails: "VNC Error" overlay appears in BrowserDisplay
- [ ] If Orgo fails: Agent says "I encountered an error while trying to do that"
- [ ] If tool not called: Agent shouldn't claim to have browsed

## Required Environment Variables

### Root `.env`
```bash
# LiveKit
LIVEKIT_URL=wss://your-project.livekit.cloud
LIVEKIT_API_KEY=your_api_key
LIVEKIT_API_SECRET=your_api_secret

# OpenAI
OPENAI_API_KEY=sk-...

# Beyond Presence
BEY_AVATAR_ID=your_avatar_id

# Orgo - CRITICAL
ORGO_API_KEY=sk_live_...
ORGO_COMPUTER_ID=<from test_orgo.py or dashboard>
ORGO_VNC_HOST=orgo-computer-xxxxx.orgo.dev  # ⚠️ From dashboard!
ORGO_VNC_PASSWORD=<password>                 # ⚠️ From dashboard!
```

### Agent `.env` (same as root)
```bash
# Same values as root .env
```

## Testing Commands

### 1. Test Orgo Connection
```bash
cd agent
python test_orgo.py
```

Expected output:
```
Testing Orgo Computer connection...
----------------------------------------
Using existing computer ID: abc-123-def
URL: https://orgo.ai/api/computers/abc-123-def

Fetching computer details from API...
{
  "id": "abc-123-def",
  "status": "running",
  ...
}
```

### 2. Start Agent (with verbose logs)
```bash
cd agent
python agent.py dev
```

Watch for:
- ✓ "Connected to existing Orgo Computer"
- ✓ "VNC credentials published"
- ✗ "VNC credentials not configured" (should NOT appear)

### 3. Start Frontend
```bash
cd frontend
npm run dev
```

Open browser console, watch for:
- ✓ "[DEBUG] Received data message: browser_ready"
- ✓ "[BrowserDisplay] VNC connected!"

### 4. Test Voice Interaction
1. Click "Start" in frontend
2. Say: "Go to Amazon and search for bananas"
3. Watch agent terminal for "Starting browser task"
4. Watch frontend for "Browsing" badge and VNC display

## Common Problems and Solutions

| Symptom | Cause | Solution |
|---------|-------|----------|
| "Waiting for browser... No VNC credentials received" | Missing ORGO_VNC_HOST/PASSWORD in .env | Get from Orgo dashboard → Computer Settings |
| "VNC Error: Connection failed" | Wrong hostname or password | Verify credentials in dashboard |
| Agent says browsing but nothing happens | Tool not actually called | Make prompt more explicit about tool usage |
| "Failed to initialize Orgo Computer" | Wrong API key or computer ID | Check ORGO_API_KEY and ORGO_COMPUTER_ID |
| Browser display black screen | Computer not running | Start computer in Orgo dashboard |
| Tool executes but no VNC display | VNC not published before frontend ready | Increase retry attempts in publish function |

## Next Steps

1. **Immediate:**
   - Get VNC credentials from Orgo dashboard
   - Add to `.env` files
   - Restart agent and test connection

2. **Short-term:**
   - Implement Issue #2 fix (better retry logic)
   - Implement Issue #3 fix (health check)
   - Implement Issue #4 fix (explicit tool prompts)

3. **Long-term:**
   - Consider caching VNC credentials in agent state
   - Add telemetry for tool execution success rate
   - Implement automatic computer lifecycle management

## References

- **Orgo Embed VMs:** https://docs.orgo.ai/guides/embed-vms
- **LiveKit OpenAI Realtime:** https://docs.livekit.io/agents/models/realtime/plugins/openai
- **LiveKit Data Channel:** https://docs.livekit.io/transport/data/packets
- **orgo-vnc Package:** npm package for React VNC display
