# Nora - AI Voice Assistant

An ultra-low-latency AI voice assistant built with LiveKit, designed to provide natural conversational experiences with integrated web browsing capabilities.

## 🌟 Features

- **Ultra-Low Latency Voice** - Near real-time voice interaction using the "Sandwich Stack" (Deepgram STT, Groq LLM, Cartesia TTS)
- **Natural Turn-Taking** - Semantic turn detection for fluid conversations
- **Web Browsing Integration** - Orgo-powered autonomous web browsing
- **Telegram Integration** - Receive and read Telegram messages aloud
- **Avatar Interface** - Interactive visual interface with LiveKit avatar
- **Contextual Awareness** - Maintains conversation context and task state

## 🏗️ Architecture

### Backend (Python)
- **LiveKit Agent** - Custom voice agent with advanced features
- **Orgo Integration** - Autonomous web browsing capabilities
- **Telegram Service** - Real-time message monitoring and TTS

### Frontend (Next.js)
- **Avatar Room** - Real-time voice interaction interface
- **Browser Display** - Live view of autonomous browsing
- **LiveKit Client** - WebRTC-based voice communication

## 🚀 Quick Start

### Prerequisites

- Python 3.11+
- Node.js 18+
- LiveKit Cloud account or self-hosted LiveKit server
- API keys for:
  - Deepgram (Speech-to-Text)
  - Groq (LLM)
  - Cartesia (Text-to-Speech)
  - Telegram Bot Token (optional)

### Backend Setup

1. Navigate to the agent directory:
```bash
cd agent
```

2. Create and activate virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Copy and configure environment variables:
```bash
cp .env.example .env
# Edit .env with your API keys
```

5. Run the agent:
```bash
python agent.py dev
```

### Frontend Setup

1. Navigate to the frontend directory:
```bash
cd frontend
```

2. Install dependencies:
```bash
npm install
```

3. Configure environment variables:
```bash
cp .env.example .env.local
# Edit .env.local with your LiveKit credentials
```

4. Run the development server:
```bash
npm run dev
```

5. Open [http://localhost:3000](http://localhost:3000) in your browser

## 🔧 Configuration

### Agent Configuration

Edit `agent/.env`:

```env
# LiveKit Configuration
LIVEKIT_URL=wss://your-livekit-url
LIVEKIT_API_KEY=your-api-key
LIVEKIT_API_SECRET=your-api-secret

# AI Service APIs
DEEPGRAM_API_KEY=your-deepgram-key
GROQ_API_KEY=your-groq-key
CARTESIA_API_KEY=your-cartesia-key

# Optional: Telegram Integration
TELEGRAM_BOT_TOKEN=your-telegram-token
TELEGRAM_USER_ID=your-user-id
```

### Frontend Configuration

Edit `frontend/.env.local`:

```env
LIVEKIT_URL=wss://your-livekit-url
LIVEKIT_API_KEY=your-api-key
LIVEKIT_API_SECRET=your-api-secret
```

## 📚 Documentation

- [Project Progress](docs/PROGRESS.md) - Development timeline and achievements
- [LiveKit Integration](docs/live-kit.llms-full.md) - Detailed LiveKit implementation guide
- [Orgo Integration](docs/orgo-llms-full-txt.md) - Web browsing capabilities

## 🛠️ Development

### Testing Telegram Service

```bash
cd agent
python telegram_demo.py
```

### Testing Orgo Browser

```bash
cd agent
python test_orgo.py
```

## 🎯 Use Cases

- **Elderly Care** - Voice-controlled assistance for daily tasks
- **Hands-Free Shopping** - Voice-guided Amazon browsing
- **Message Monitoring** - Automatic Telegram message notifications
- **Personal Assistant** - Natural language task execution

## 🔐 Security Notes

- Never commit your `.env` files
- Keep API keys secure
- Use environment variables for all sensitive data
- Review Orgo browsing sessions for sensitive information

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## 📝 License

This project is open source and available under the [MIT License](LICENSE).

## 🙏 Acknowledgments

- [LiveKit](https://livekit.io/) - Real-time communication platform
- [Deepgram](https://deepgram.com/) - Speech-to-Text API
- [Groq](https://groq.com/) - Ultra-fast LLM inference
- [Cartesia](https://cartesia.ai/) - Natural Text-to-Speech
- [Orgo](https://orgo.so/) - Autonomous web browsing

## 📧 Contact

For questions or support, please open an issue on GitHub.

---

**Built with ❤️ for natural human-AI interaction**
