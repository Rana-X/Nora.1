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
from openai import AsyncOpenAI
import time

# Load environment variables from parent directory's .env file
load_dotenv(dotenv_path="../.env")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("nora-agent")

# System prompt for Nora's personality with browser and messaging capabilities
# System prompt for Nora's personality with browser and messaging capabilities
SYSTEM_PROMPT = """You are Nora, a warm and caring AI companion helping Garry, an elderly gentleman. You can control a web browser and send/receive Telegram messages.

TODAY'S CONTEXT (you know this about Garry's schedule):
- Tomorrow at 4:30 PM: Dinner with Rana at the Italian restaurant
- Garry takes daily medications

CONVERSATION FLOW:
When Garry greets you, respond warmly and check in on how he's doing first. Once you've exchanged pleasantries, naturally mention you noticed the dinner with Rana on his calendar tomorrow. Let the conversation flow from there. At some point, gently ask if he took his medications today.

CALENDAR & REMINDERS:
When Garry asks you to add something to his calendar or set a reminder, simply agree warmly and confirm - no tool is needed. Just say something like "Done! I've added that to your calendar" or "I'll remind you at that time, Garry." Keep it natural.

YOUR RELATIONSHIP WITH GARRY:
- You are like a trusted friend and helper who genuinely cares about Garry's wellbeing
- Speak with warmth, patience, and gentle encouragement
- Take your time - never rush Garry or make him feel hurried
- Celebrate small victories and be supportive when things are confusing

MEDICATION REFILLS - TOP PRIORITY:
When Garry asks for medication refills:
1. Ask which medication: "Which medication do you need, Garry?"
2. Reassure: "I've got your pharmacy details. Let me order that for you."
3. Use the refill_prescription tool with the medication name
4. After tool completes, I will tell you the result - wait for it before speaking
5. The tool will tell you exactly what happened - speak about that, not what you think happened

CAPABILITIES:
- You can browse the web, search for information, shop online, fill forms, etc.
- You can send and receive Telegram messages to/from family and friends (like Rana)
- When Garry asks you to do something on the web, use the browse_and_act tool
- When Garry wants to send a message, use the send_telegram_message tool
- Messages arrive automatically and you will read them aloud when they come in

MESSAGING:
- Garry can say things like "Send a message to Rana" or "Tell her I'm doing well"
- Read incoming messages aloud warmly: "Oh Garry, you have a lovely message! It says..."
- Confirm when messages are sent: "There we go, I've sent that off to Rana for you."
- Messages come in automatically, you don't need to check for them

CONVERSATION STYLE:
- Be warm and conversational, like chatting with a dear friend
- Keep responses concise but never cold - 1-3 sentences with genuine care
- Celebrate success: "There we go!", "All done!", "Perfect!"
- When performing browser tasks, give reassuring status updates
- If something goes wrong, be gentle and reassuring: "That's alright, let me try another way"
- Speak clearly and at a comfortable, unhurried pace

BROWSER RULES:
- Tell Garry before browsing: "Let me look that up for you"
- Browser takes 10-30 seconds (you'll be silent during this)
- A narrator will tell you the result after the tool completes
- WAIT for that result before speaking about success or failure
- Never claim you did something before hearing the result

ACTIVE LISTENING:
- When Garry pauses mid-thought, use warm acknowledgments like "mmhmm", "I'm listening", "take your time"
- Never interrupt - let Garry finish his thoughts

PERSONALITY:
- Warm, patient, and genuinely caring - like a loving companion
- Gently proactive: "Would you like me to help with anything else, Garry?"
- Reassuring when technology feels overwhelming
- Honest but kind about limitations

IMPORTANT:
- Always speak in English only, regardless of what language you hear
- Never use emojis in speech (they can't be spoken)
- Avoid bullet points or lists - speak in natural, flowing sentences
- Address Garry by name occasionally to make conversations feel personal
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
        
        # Background narrator LLM (GPT-4o-mini)
        self.narrator = AsyncOpenAI(api_key=os.environ.get("OPENAI_API_KEY"))
        
    async def narrate_result(self, tool_result: str, task_type: str = "general") -> str:
        """Convert tool result to natural speech using background LLM"""
        
        context_hints = {
            "medication": "Garry just ordered a medication refill",
            "shopping": "Rana was shopping online",
            "research": "Rana was looking up information"
        }
        
        try:
            response = await self.narrator.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{
                    "role": "system",
                    "content": f"""You are Nora speaking to Garry. {context_hints.get(task_type, '')}

Convert this tool result to warm, brief speech (1-2 sentences max):
- If "Order Confirmed" or "order confirmed": Celebrate! "All done!", "There we go!", "Perfect!"
- If error/failed: Explain simply and warmly
- Be a caring companion, not a robot
- Keep it under 2 sentences"""
                }, {
                    "role": "user",
                    "content": f"Tool result: {tool_result}\n\nSpeak to Garry:"
                }],
                max_tokens=100,
                temperature=0.7
            )
            
            return response.choices[0].message.content
            
        except Exception as e:
            logger.error(f"Narrator LLM failed: {e}")
            # Fallback to simple response
            if "order confirmed" in tool_result.lower():
                return "All done! Your order is placed."
            elif "error" in tool_result.lower() or "failed" in tool_result.lower():
                return "I ran into a problem with that. Want me to try again?"
            else:
                return "Task completed."
    
    def should_narrate(self, tool_result: str, execution_time_ms: float) -> bool:
        """Smart triggering: only narrate on success, error, or ambiguous results"""
        
        result_lower = tool_result.lower()
        
        # ALWAYS narrate explicit success
        if "order confirmed" in result_lower or "order placed" in result_lower:
            logger.info("Narrator triggered: SUCCESS (order confirmed)")
            return True
        
        # ALWAYS narrate errors
        if "error" in result_lower or "failed" in result_lower or "could not" in result_lower:
            logger.info("Narrator triggered: ERROR detected")
            return True
        
        # Narrate long tasks (over 30 seconds)
        if execution_time_ms > 30000:
            logger.info(f"Narrator triggered: LONG TASK ({execution_time_ms}ms)")
            return True
        
        # Narrate ambiguous results (neither clear success nor clear step)
        if "successfully" not in result_lower and "completed" not in result_lower:
            logger.info("Narrator triggered: AMBIGUOUS result")
            return True
        
        # Don't narrate intermediate steps
        logger.info("Narrator skipped: intermediate step")
        return False

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
        start_time = time.time()
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

            execution_time_ms = (time.time() - start_time) * 1000
            logger.info(f"Browser task completed in {execution_time_ms:.0f}ms")
            logger.info(f"Orgo result type: {type(result)}")
            
            # Simplify the result for logging
            if isinstance(result, str):
                summary = result[:500] if len(result) > 500 else result
            elif isinstance(result, list):
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
            
            # Smart narrator triggering
            if self.should_narrate(summary, execution_time_ms):
                # Determine task type from instruction
                task_type = "general"
                if "quickcare" in instruction.lower() or "medication" in instruction.lower():
                    task_type = "medication"
                elif "amazon" in instruction.lower() or "shopping" in instruction.lower():
                    task_type = "shopping"
                
                # Generate natural speech
                speech = await self.narrate_result(summary, task_type)
                logger.info(f"Narrator speaking: {speech}")
                
                # Use LiveKit session to speak and wait for completion
                try:
                    speech_handle = await context.session.generate_reply(
                        instructions=f"Tell Garry this result warmly and briefly: {speech}"
                    )
                    # Wait for Nora to finish speaking
                    await speech_handle.wait_for_playout()
                    logger.info("Speech completed, starting 15-second display timer")
                except Exception as e:
                    logger.error(f"Failed to speak via session: {e}")
            
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
            
            # Signal task completion (but don't hide browser yet)
            await self.publish_browser_status("browser_task_completed")
            
            # Wait 15 seconds so Garry can see the result
            await asyncio.sleep(15)
            
            # NOW tell frontend it can hide the browser
            await self.publish_browser_status("browser_can_hide")
            logger.info("Sent browser_can_hide after 15-second display")
            
            return summary
        except Exception as e:
            logger.error(f"Browser task failed: {e}")
            error_msg = f"I encountered an error while trying to do that: {str(e)}"
            
            # Always narrate errors
            try:
                speech = await self.narrate_result(error_msg, "error")
                speech_handle = await context.session.generate_reply(
                    instructions=f"Tell Garry about this problem gently: {speech}"
                )
                await speech_handle.wait_for_playout()
            except Exception as narrate_error:
                logger.error(f"Failed to narrate error: {narrate_error}")
            
            # Signal error completion
            await self.publish_browser_status("browser_task_completed")
            await asyncio.sleep(15)
            await self.publish_browser_status("browser_can_hide")
            
            return error_msg
        finally:
            # Mark browser as not busy
            self.browser_busy = False

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
        Use this when Garry wants to send a message.
        
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
        Use this when Garry asks if he has any messages.
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

    @function_tool()
    async def refill_prescription(self, context: RunContext, medication_name: str) -> str:
        """
        Refill a prescription at QuickCare Pharmacy.
        Use this when Garry needs a medication refill.
        
        Args:
            medication_name: The name of the medication to refill (e.g., "Adderall", "Lisinopril")
        """
        logger.info(f"Starting prescription refill for: {medication_name}")

        if not self.computer:
            logger.error("No Orgo computer available for prescription refill")
            return "I'm sorry, the pharmacy system is not available right now."

        # Mark browser as busy - messages will be queued
        self.browser_busy = True
        
        # Notify frontend that browser task is starting
        await self.publish_browser_status("browser_task_started")
        
        # Start the pharmacy task in background - don't await it!
        asyncio.create_task(self._run_pharmacy_task(context, medication_name))
        
        # Return immediately so Nora can keep chatting
        return f"I'm heading to the pharmacy now to order your {medication_name}. I'll let you know when it's done - feel free to keep chatting with me in the meantime!"
    
    async def _run_pharmacy_task(self, context: RunContext, medication_name: str):
        """Background task that runs the pharmacy flow and announces when done."""
        import time
        start_time = time.time()
        
        # Track if we've already announced the order confirmation
        order_confirmed_announced = False
        
        # Get the event loop for thread-safe callback scheduling
        loop = asyncio.get_event_loop()
        
        # Callback to detect order confirmation in real-time
        def on_orgo_progress(event_type, event_data):
            nonlocal order_confirmed_announced
            
            # Only check text events for order confirmation
            if event_type != "text":
                return
                
            # Convert event data to string for checking
            event_str = str(event_data).lower()
            
            # Only trigger on order confirmation - ignore everything else
            if not order_confirmed_announced and "order confirmed" in event_str:
                order_confirmed_announced = True
                logger.info("ORDER CONFIRMATION DETECTED via callback!")
                
                # Immediately announce to Garry (thread-safe)
                def schedule_immediate_announcement():
                    asyncio.create_task(self._announce_order_confirmed(context))
                
                loop.call_soon_threadsafe(schedule_immediate_announcement)

        try:
            # Hardcoded QuickCare pharmacy flow for reliability
            pharmacy_instruction = f"""IMPORTANT: Complete these steps carefully on the QuickCare pharmacy website.

STEPS:
1. Navigate to https://quickcare-flow.vercel.app/
2. Click "Sign in to your account" button
3. After sign-in page loads, click "My Prescriptions" or "View Prescriptions" 
4. Look for the medication named "{medication_name}" in the list
5. Click the "Refill" button next to {medication_name}
6. Click the "Pay" or "Pay!" button to complete the order
7. Wait for and confirm you see "Order Confirmed" or similar success message

REPORT: Tell me exactly what happened - did the order get confirmed?"""

            # Use Orgo's hosted agent service with callback for real-time detection
            result = await asyncio.to_thread(
                self.computer.prompt,
                instruction=pharmacy_instruction,
                model="claude-sonnet-4-5-20250929",
                max_iterations=30,
                verbose=True,
                callback=on_orgo_progress,  # Real-time order confirmation detection
            )

            execution_time_ms = (time.time() - start_time) * 1000
            logger.info(f"Prescription refill completed in {execution_time_ms:.0f}ms")
            
            # If callback didn't fire, announce completion now
            if not order_confirmed_announced:
                logger.info("Callback didn't detect confirmation, announcing fallback")
                try:
                    speech_handle = await context.session.generate_reply(
                        instructions="Tell Garry: I finished at the pharmacy. Your prescription should be on its way!"
                    )
                    await speech_handle.wait_for_playout()
                except Exception as e:
                    logger.error(f"Failed to announce fallback: {e}")
            
            # Signal completion and display timer
            await self.publish_browser_status("browser_task_completed")
            await asyncio.sleep(15)
            await self.publish_browser_status("browser_can_hide")
            logger.info("Sent browser_can_hide after 15-second display")
            
        except Exception as e:
            logger.error(f"Prescription refill failed: {e}")
            try:
                speech_handle = await context.session.generate_reply(
                    instructions=f"Tell Garry gently: I had a little trouble at the pharmacy, but I'll try again."
                )
                await speech_handle.wait_for_playout()
            except Exception as narrate_error:
                logger.error(f"Failed to narrate error: {narrate_error}")
            
            await self.publish_browser_status("browser_task_completed")
            await asyncio.sleep(15)
            await self.publish_browser_status("browser_can_hide")
            
        finally:
            self.browser_busy = False
    
    async def _announce_order_confirmed(self, context: RunContext):
        """Immediately announce order confirmation to Garry."""
        try:
            logger.info("Announcing order confirmation immediately via callback")
            speech_handle = await context.session.generate_reply(
                instructions="Tell Garry warmly and briefly: All done! Your prescription order is confirmed!"
            )
            await speech_handle.wait_for_playout()
            logger.info("Order confirmation announcement completed")
        except Exception as e:
            logger.error(f"Failed to announce order confirmation: {e}")


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
