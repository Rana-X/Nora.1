# Nora

AI Voice Agent with a hyper-realistic animated avatar, built to assist elderly users through natural voice conversations.

Nora combines real-time voice interaction, browser automation, and messaging into a single conversational agent — designed for healthcare assistance use cases like medication refills, appointment scheduling, and family communication.

## Architecture

```
Frontend (Next.js)          LiveKit Cloud            Python Agent
┌──────────────────┐    ┌──────────────────┐    ┌──────────────────────┐
│ Landing Page     │    │ Room Management  │    │ NoraAgent            │
│ AvatarRoom       │◄──►│ Audio/Video      │◄──►│  ├─ Voice Pipeline   │
│ BrowserDisplay   │    │ Agent Dispatch   │    │  ├─ Function Tools   │
└──────────────────┘    └──────────────────┘    │  └─ Smart Narrator   │
                                                └──────┬───┬───┬───────┘
                                                       │   │   │
                                              Orgo  OpenAI  Telegram
                                             Browser GPT-4o  Bot API
                                             Control
```

**Voice Pipeline:** Deepgram Nova-2 (STT) → GPT-4o (LLM) → Cartesia Sonic (TTS) → Beyond Presence (Avatar lip-sync)

## Features

- **Voice Conversations** — Real-time speech-to-text and text-to-speech with natural turn-taking
- **Animated Avatar** — Beyond Presence avatar with real-time lip-sync and facial animations
- **Browser Automation** — Orgo.ai + Claude Sonnet for web tasks (shopping, form filling, pharmacy refills)
- **Prescription Refills** — Automated medication ordering through QuickCare pharmacy
- **Telegram Messaging** — Send and receive messages to family contacts
- **Smart Narrator** — Background LLM that converts tool results into natural speech

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Frontend | Next.js 16, React 19, TypeScript, Tailwind CSS 4 |
| Voice Agent | Python, LiveKit Agents SDK 1.0 |
| STT | Deepgram Nova-2 (via LiveKit Inference) |
| TTS | Cartesia Sonic (via LiveKit Inference) |
| LLM | OpenAI GPT-4o |
| Avatar | Beyond Presence |
| Browser Control | Orgo.ai + Anthropic Claude |
| Messaging | Telegram Bot API |

## Prerequisites

- Python 3.11+
- Node.js 18+
- API keys for: LiveKit, OpenAI, Beyond Presence, Orgo.ai, Anthropic

## Setup

### 1. Environment Variables

```bash
cp .env.example .env
```

Fill in your API keys:

```
LIVEKIT_URL=wss://your-project.livekit.cloud
LIVEKIT_API_KEY=...
LIVEKIT_API_SECRET=...
OPENAI_API_KEY=sk-...
BEYOND_PRESENCE_API_KEY=sk-...
BEY_AVATAR_ID=...
ORGO_API_KEY=...
ANTHROPIC_API_KEY=sk-ant-...
```

### 2. Backend (Agent)

```bash
cd agent
pip install -r requirements.txt
python agent.py dev
```

### 3. Frontend

```bash
cd frontend
npm install
npm run dev
```

The app will be available at `http://localhost:3000`.

## Project Structure

```
.
├── agent/
│   ├── agent.py              # Main voice agent (entry point)
│   ├── telegram_service.py   # Telegram messaging integration
│   └── requirements.txt      # Python dependencies
├── frontend/
│   ├── app/
│   │   ├── page.tsx          # Landing page
│   │   └── api/token/route.ts # LiveKit token generation
│   ├── components/
│   │   ├── AvatarRoom.tsx    # Avatar display + audio
│   │   └── BrowserDisplay.tsx # VNC browser view
│   └── package.json
├── docs/                     # Architecture docs & API references
├── .env.example              # Environment variable template
└── TESTING_GUIDE.md          # QA testing procedures
```

## Usage

1. Start both the agent and frontend servers
2. Open `http://localhost:3000` in your browser
3. Click **Start** to connect to Nora
4. Speak naturally — Nora will respond with voice and avatar animations

Example interactions:
- *"I need to refill my Adderall prescription"*
- *"Can you send a message to Rana?"*
- *"Search for the nearest pharmacy"*

## License

All rights reserved.
