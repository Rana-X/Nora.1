# Nora: AI Voice Agent (Python + LiveKit)

## Overview

Production-ready AI Voice Agent with hyper-realistic avatar using the "Natural Voice" stack:

- **Framework**: livekit-agents with VoicePipelineAgent
- **Face**: Beyond Presence (livekit-plugins-bey)
- **Voice (TTS)**: Cartesia Sonic (livekit-plugins-cartesia)
- **Ears (STT)**: Deepgram Nova-2 (livekit-plugins-deepgram)
- **Brain (LLM)**: OpenAI GPT-4o (livekit-plugins-openai)

## Stack Summary

LiveKit provides (via LiveKit Inference):
- ✅ STT (Deepgram) - included
- ✅ TTS (Cartesia) - included
- ✅ Transport/infrastructure

You provide:
- ✅ OpenAI API key for GPT-4o (brain)

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                      LiveKit Cloud Room                          │
│                                                                  │
│  User Audio ──▶ Deepgram STT ──▶ GPT-4o ──▶ Cartesia TTS ──┐   │
│      │              (Nova-2)      (Brain)    (Sonic)        │   │
│      │                                                       │   │
│      │                                         Agent Audio ◀─┘   │
│      │                                              │            │
│      ▼                                              ▼            │
│  ┌────────────────────────────────────────────────────────┐     │
│  │              Beyond Presence Avatar                     │     │
│  │   (Subscribes to audio tracks for lip-sync & reactions) │     │
│  └────────────────────────────────────────────────────────┘     │
└─────────────────────────────────────────────────────────────────┘
```

## Project Structure

```
nora/
├── agent/
│   ├── agent.py           # Main LiveKit agent
│   └── requirements.txt   # Python dependencies
├── frontend/
│   ├── app/
│   │   ├── page.tsx
│   │   ├── api/token/route.ts
│   │   └── components/AvatarRoom.tsx
│   ├── package.json
│   └── next.config.js
├── .env
└── .env.example
```

## Files to Create

### 1. agent/requirements.txt

```
livekit-agents>=0.8.0
livekit-plugins-openai>=0.8.0
livekit-plugins-deepgram>=0.6.0
livekit-plugins-cartesia>=0.4.0
livekit-plugins-silero>=0.6.0
livekit-plugins-bey>=0.1.0
python-dotenv>=1.0.0
```

### 2. .env.example

```
# LiveKit (includes hosted STT/TTS via LiveKit Inference)
LIVEKIT_URL=wss://your-project.livekit.cloud
LIVEKIT_API_KEY=your_api_key
LIVEKIT_API_SECRET=your_api_secret

# OpenAI (Brain - GPT-4o)
OPENAI_API_KEY=sk-...

# Beyond Presence (Avatar)
BEYOND_PRESENCE_API_KEY=sk-...
BEY_AVATAR_ID=your_avatar_id
```

### 3. agent/agent.py

Full implementation with:
- VoicePipelineAgent setup
- Deepgram Nova-2 for STT
- GPT-4o for LLM
- Cartesia Sonic for TTS
- Beyond Presence avatar
- `allow_interruptions=True`
- `min_endpointing_delay=0.5`
- Sophisticated system prompt with backchanneling
- Welcome message: "Hey there! I'm Nora. What's on your mind?"
- Logging for speech events
- Comments explaining lip-sync mechanism

### 4. Next.js Frontend

Display avatar video from LiveKit room.

## Environment Variables (Current .env)

```
# Beyond Presence
BEYOND_PRESENCE_API_KEY=sk-your-bey-api-key
BEY_AVATAR_ID=your-avatar-uuid

# LiveKit (includes access to hosted STT/TTS models via LiveKit Inference)
LIVEKIT_URL=wss://your-project.livekit.cloud
LIVEKIT_API_KEY=your-api-key
LIVEKIT_API_SECRET=your-api-secret

# OpenAI (for LLM brain)
OPENAI_API_KEY=sk-your-openai-key
```

## API Keys Status ✅

All keys available! Using LiveKit Inference for hosted models:

- ✅ **STT**: deepgram/nova-2 via LiveKit (no separate key needed)
- ✅ **TTS**: cartesia/sonic via LiveKit (no separate key needed)
- ✅ **LLM**: OpenAI GPT-4o (key provided)
- ✅ **Avatar**: Beyond Presence (key provided)

## Implementation Steps

### Step 1: Update .env
Add OpenAI key and avatar ID to existing .env file.

### Step 2: Create agent directory and files
- requirements.txt
- agent.py

### Step 3: Create Python virtual environment

```bash
cd agent
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### Step 4: Create Next.js frontend

```bash
npx create-next-app@latest frontend --typescript --tailwind --app
cd frontend
npm install livekit-client @livekit/components-react livekit-server-sdk
```

### Step 5: Run the system

- **Terminal 1**: `python agent.py dev`
- **Terminal 2**: `cd frontend && npm run dev`

## Key Features

### Interruption (Barge-in)
- `allow_interruptions=True` enables cutting off the agent
- `min_endpointing_delay=0.5` gives natural pause detection

### Active Listening
- System prompt encourages "mhm", "right", "I see"
- Avatar animates based on user speech detection

### Lip Sync
The livekit-plugins-bey avatar automatically:
1. Subscribes to the agent's published audio track
2. Analyzes audio in real-time
3. Generates synchronized lip movements
4. Publishes video frames to the room

## Verification

1. Run `python agent.py dev` - agent connects to LiveKit
2. Open frontend, click Join
3. Speak - see "User started speaking" in console
4. Agent responds with voice
5. Avatar lip-syncs to agent voice
6. Test interruption mid-sentence
