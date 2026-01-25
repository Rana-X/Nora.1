"""
Nora: AI Voice Agent with Beyond Presence Avatar + Orgo Browser Control
========================================================================

Based on the official bey-examples livekit-agent implementation.
Uses OpenAI's Realtime API for voice-to-voice conversation.
Integrates Orgo.ai for browser control capabilities.

Updated to use LiveKit Agents v1.0 API patterns.
"""

import asyncio
import base64
import json
import os
import sys
import logging

from dotenv import load_dotenv
from livekit.agents import (
    Agent,
    AgentSession,
    AutoSubscribe,
    JobContext,
    RunContext,
    WorkerOptions,
    WorkerType,
    cli,
    function_tool,
)
from livekit.plugins import bey, openai
from orgo import Computer
from telegram_service import get_telegram_service

# Load environment variables from parent directory's .env file
load_dotenv(dotenv_path="../.env")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("nora-agent")

# System prompt for Nora's personality with browser and messaging capabilities
SYSTEM_PROMPT = """You are Nora, a friendly AI assistant helping Rana, an elderly user. You can control a web browser and send/receive Telegram messages.

CAPABILITIES:
- You can browse the web, search for information, shop online, fill forms, etc.
- You can send and receive Telegram messages to/from family and friends
- When the user asks you to do something on the web, use the browse_and_act tool
- When the user wants to send a message, use the send_telegram_message tool
- Messages arrive automatically and you will read them aloud when they come in

MESSAGING:
- Rana can say things like "Send a message" or "Text that I'm doing well"
- Read incoming messages aloud naturally: "You have a new message. It says..."
- Confirm when messages are sent: "I've sent that message."
- Messages come in automatically, you don't need to check for them

CONVERSATION STYLE:
- Be concise and natural. Aim for 1-3 sentences per response.
- When performing browser tasks, give brief status updates.
- If a task fails, explain what went wrong simply.
- Speak clearly and at a moderate pace (Rana is elderly)

BROWSER TASKS:
- For shopping: Navigate to the site, search, and add items to cart (don't checkout without permission)
- For research: Search and summarize findings verbally
- For forms: Ask for any information you need before filling

IMPORTANT - BEFORE USING BROWSER:
- You CANNOT speak while the browser is working (it takes 10-30 seconds)
- ALWAYS tell the user you're starting BEFORE calling the browse_and_act tool
- Example: "Okay, I'm heading to Amazon to find those bananas. Give me a moment to browse." -> then call tool

ACTIVE LISTENING:
- When Rana pauses mid-thought, use brief acknowledgments like "mhm", "right", "I see"

PERSONALITY:
- Warm, patient, and approachable - like a helpful family member
- Proactive about offering to help with web tasks and staying connected
- Honest about limitations

IMPORTANT:
- Always speak in English only, regardless of what language you hear
- Never use emojis in speech (they can't be spoken)
- Avoid bullet points or lists - speak in natural sentences
- Don't start responses with filler phrases like "Great question!"
"""


class NoraAgent(Agent):
    """Nora voice agent with browser control and Telegram messaging capabilities"""

    def __init__(self, computer: Computer = None, room=None, telegram_service=None):
        super().__init__(instructions=SYSTEM_PROMPT)
        self.computer = computer
        self.room = room
        self.telegram = telegram_service
        # Track browser state for message queuing
        self.browser_busy = False
        self.queued_messages: list[dict] = []

    async def publish_browser_status(self, status_type: str):
        """Publish browser task status to frontend via data channel."""
        if not self.room:
            logger.warning("No room available to publish browser status")
            return
        try:
            status_data = json.dumps({"type": status_type})
            await self.room.local_participant.publish_data(
                status_data.encode(),
                reliable=True
            )
            logger.info(f"Published browser status: {status_type}")
        except Exception as e:
            logger.error(f"Failed to publish browser status: {e}")

    @function_tool()
    async def browse_and_act(self, context: RunContext, instruction: str) -> str:
        """
        Execute a multi-step browser task.
        Use this for shopping, research, navigation, or any web interaction.
        Example: "Go to Amazon and add bananas to cart"

        Args:
            instruction: The task to perform in the browser, e.g. "Go to Amazon and add bananas to cart"
        """
        logger.info(f"Starting browser task: {instruction}")

        if not self.computer:
            logger.error("No Orgo computer available for browser tasks")
            return "I'm sorry, the browser is not available right now."

        # Mark browser as busy - messages will be queued
        self.browser_busy = True
        
        # Notify frontend that browser task is starting
        await self.publish_browser_status("browser_task_started")

        try:

            # Build instruction with context
            full_instruction = f"""IMPORTANT: Always use English. For Amazon, navigate to amazon.com (US), not regional variants.

TASK: {instruction}"""

            # Use Orgo's hosted agent service for reliable execution
            result = await asyncio.to_thread(
                self.computer.prompt,
                instruction=full_instruction,
                model="claude-sonnet-4-5-20250929",  # Claude Sonnet 4.5
                max_iterations=30,  # Limit agent loops per docs
                verbose=True,  # Show detailed logs
            )

            logger.info(f"Browser task completed successfully")
            logger.info(f"Orgo result type: {type(result)}")
            
            # Simplify the result for Nora - extract just the summary
            if isinstance(result, str):
                # Already a string, use as is but limit length
                summary = result[:500] if len(result) > 500 else result
            elif isinstance(result, list):
                # List of messages - extract the last text response
                summary = "Task completed."
                for msg in reversed(result):
                    if isinstance(msg, dict) and msg.get("role") == "assistant":
                        content = msg.get("content", [])
                        for item in content:
                            if isinstance(item, dict) and item.get("type") == "text":
                                summary = item.get("text", "Task completed.")[:500]
                                break
                        break
            else:
                summary = f"Browser task completed: {str(result)[:200]}"
            
            # Check if any messages came in while browsing
            if self.queued_messages:
                queued_count = len(self.queued_messages)
                message_texts = []
                for msg in self.queued_messages:
                    message_texts.append(f"From {msg['from_name']}: {msg['text']}")
                self.queued_messages.clear()
                
                # Append message info to result so Nora mentions it
                summary += f"\n\nAlso, while I was browsing, you received {queued_count} new message(s): {' | '.join(message_texts)}"
                logger.info(f"Announcing {queued_count} queued message(s) after browser task")
            
            return summary
        except Exception as e:
            logger.error(f"Browser task failed: {e}")
            return f"I encountered an error while trying to do that: {str(e)}"
        finally:
            # Mark browser as not busy
            self.browser_busy = False
            # Always notify frontend that browser task is done
            await self.publish_browser_status("browser_task_completed")

    @function_tool()
    async def take_screenshot(self, context: RunContext) -> str:
        """Take a screenshot of the current browser state."""
        if not self.computer:
            return "Browser is not available."
        try:
            # Run blocking I/O in thread
            image_bytes = await asyncio.to_thread(self.computer.screenshot)

            # Convert bytes to base64 string
            base64_image = base64.b64encode(image_bytes).decode('utf-8')
            return f"data:image/png;base64,{base64_image}"
        except Exception as e:
            logger.error(f"Screenshot failed: {e}")
            return f"Failed to take screenshot: {str(e)}"

    @function_tool()
    async def send_telegram_message(self, context: RunContext, message: str) -> str:
        """
        Send a Telegram message.
        Use this when Rana wants to send a message.
        
        Args:
            message: The text message to send
        """
        if not self.telegram:
            return "Messaging is not available right now."
        
        logger.info(f"Sending Telegram message: {message[:50]}...")
        
        try:
            success = await asyncio.to_thread(self.telegram.send_message, message)
            if success:
                return f"Message sent to Rana successfully."
            else:
                return "I couldn't send that message. Please try again."
        except Exception as e:
            logger.error(f"Failed to send Telegram message: {e}")
            return f"There was a problem sending the message: {str(e)}"

    @function_tool()
    async def check_telegram_messages(self, context: RunContext) -> str:
        """
        Check for new Telegram messages.
        Use this when Rana asks if she has any messages.
        """
        if not self.telegram:
            return "Messaging is not available right now."
        
        logger.info("Checking for new Telegram messages")
        
        try:
            messages = await asyncio.to_thread(self.telegram.poll_messages)
            
            if not messages:
                return "No new messages."
            
            # Format messages for Nora to read aloud
            result_parts = []
            for msg in messages:
                result_parts.append(f"Message from {msg['from_name']}: {msg['text']}")
            
            return " ".join(result_parts)
        except Exception as e:
            logger.error(f"Failed to check Telegram messages: {e}")
            return f"I couldn't check messages right now: {str(e)}"


async def publish_vnc_credentials_with_retry(room, max_retries: int = 5, delay: float = 1.0):
    """
    Publish VNC credentials with retries to handle race condition.

    VNC credentials are read from environment variables:
    - ORGO_VNC_HOST: The VNC hostname (e.g., orgo-computer-p9noby06.orgo.dev)
    - ORGO_VNC_PASSWORD: The VNC password (from Orgo dashboard -> Computer Settings)
    """
    vnc_host = os.environ.get("ORGO_VNC_HOST")
    vnc_password = os.environ.get("ORGO_VNC_PASSWORD")
    
    if not vnc_host or not vnc_password:
        logger.warning("VNC credentials not configured. Set ORGO_VNC_HOST and ORGO_VNC_PASSWORD in .env")
        logger.warning("Get these from Orgo dashboard -> Computer Settings")
        return False
    
    browser_data = json.dumps({
        "type": "browser_ready",
        "hostname": vnc_host,
        "password": vnc_password
    })

    for attempt in range(max_retries):
        try:
            await room.local_participant.publish_data(
                browser_data.encode(),
                reliable=True
            )
            logger.info(f"VNC credentials published (attempt {attempt + 1}/{max_retries})")

            # Publish multiple times to ensure frontend receives it
            if attempt < 2:
                await asyncio.sleep(delay)
                continue
            return True
        except Exception as e:
            logger.warning(f"Failed to publish VNC credentials (attempt {attempt + 1}): {e}")
            await asyncio.sleep(delay)

    logger.error("Failed to publish VNC credentials after all retries")
    return False


async def entrypoint(ctx: JobContext) -> None:
    """Main entry point for the agent."""
    logger.info(f"Agent connecting to room: {ctx.room.name}")

    # Connect to room, only subscribing to audio
    await ctx.connect(auto_subscribe=AutoSubscribe.AUDIO_ONLY)
    logger.info("Connected to room")

    # Initialize Orgo Computer for browser control
    computer = None
    try:
        # Use existing persistent computer if ID provided
        computer_id = os.environ.get("ORGO_COMPUTER_ID")
        api_key = os.environ.get("ORGO_API_KEY")
        
        if not api_key:
            logger.error("ORGO_API_KEY not set in environment")
            raise ValueError("ORGO_API_KEY required for browser control")
        
        if computer_id:
            # Explicitly pass api_key to ensure SDK uses correct credentials
            computer = Computer(computer_id=computer_id, api_key=api_key)
            logger.info(f"Connected to existing Orgo Computer: {computer_id}")
        else:
            computer = Computer(api_key=api_key)
            logger.info(f"Created new Orgo Computer")
        logger.info(f"Orgo Computer URL: {computer.url}")
    except Exception as e:
        logger.warning(f"Failed to initialize Orgo Computer: {e}")
        logger.warning("Browser control will not be available")

    # Create the voice agent session using OpenAI's Realtime API
    # This handles STT, LLM, and TTS in one integrated model
    voice_agent_session = AgentSession(
        llm=openai.realtime.RealtimeModel(
            voice="shimmer",  # Options: alloy, echo, fable, onyx, nova, shimmer
        ),
    )

    # Initialize Telegram service for messaging
    telegram_service = get_telegram_service()
    logger.info("Telegram service initialized")
    logger.info(f"  - Contact: {telegram_service.CONTACT_NAME}")

    # Create the Nora agent with browser tools and Telegram
    # In v1.0, tools decorated with @function_tool() are automatically registered
    nora_agent = NoraAgent(computer=computer, room=ctx.room, telegram_service=telegram_service)
    
    if computer:
        logger.info(f"NoraAgent created with browser and messaging capabilities")
        logger.info(f"  - Tools: browse_and_act, take_screenshot, send_telegram_message, check_telegram_messages")
    else:
        logger.info("NoraAgent created with messaging only (Orgo not available)")
        logger.info(f"  - Tools: send_telegram_message, check_telegram_messages")

    # Initialize Beyond Presence avatar
    avatar_id = os.environ.get("BEY_AVATAR_ID")
    if not avatar_id:
        logger.error("BEY_AVATAR_ID not set in environment")
        return

    logger.info(f"Initializing Beyond Presence avatar: {avatar_id}")
    bey_avatar_session = bey.AvatarSession(avatar_id=avatar_id)

    # Start the voice agent session
    await voice_agent_session.start(agent=nora_agent, room=ctx.room)
    logger.info("Voice agent session started")

    # Start the avatar session (connects avatar to the voice agent's audio)
    await bey_avatar_session.start(voice_agent_session, room=ctx.room)
    logger.info("Avatar session started")

    # Start background Telegram listener to auto-read incoming messages
    # Capture the event loop for thread-safe scheduling
    loop = asyncio.get_event_loop()
    
    def on_telegram_message(msg: dict):
        """Callback when a new Telegram message arrives - inject it into the conversation."""
        logger.info(f"New Telegram message from {msg['from_name']}: {msg['text'][:50]}...")
        
        # If browser is busy, queue the message for later
        if nora_agent.browser_busy:
            nora_agent.queued_messages.append(msg)
            logger.info(f"Message queued (browser busy) - will announce after browsing")
            return
        
        # Browser not busy - announce immediately
        announcement = f"You have a new message from {msg['from_name']}. It says: {msg['text']}"
        
        # Use call_soon_threadsafe since this callback runs in a different thread
        def schedule_announcement():
            asyncio.create_task(voice_agent_session.generate_reply(user_input=announcement))
        
        loop.call_soon_threadsafe(schedule_announcement)
    
    telegram_service.start_listener(on_telegram_message, poll_interval=2)
    logger.info("Telegram listener started - incoming messages will be read aloud")

    # Publish VNC credentials AFTER sessions start to avoid race condition
    # VNC credentials are read from ORGO_VNC_HOST and ORGO_VNC_PASSWORD env vars
    await asyncio.sleep(0.5)  # Small delay for frontend listener setup
    asyncio.create_task(publish_vnc_credentials_with_retry(ctx.room))


if __name__ == "__main__":
    load_dotenv()

    # Override args for LiveKit CLI if running directly
    if len(sys.argv) == 1:
        sys.argv = [sys.argv[0], "dev"]

    cli.run_app(
        WorkerOptions(
            entrypoint_fnc=entrypoint,
            worker_type=WorkerType.ROOM,
            agent_name="nora-voice-agent",
        )
    )
