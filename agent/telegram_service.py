"""
Telegram Service for Nora Voice Assistant
==========================================

Enables sending and receiving Telegram messages for elderly users.
Uses polling (not webhooks) for local execution.
"""

import logging
import threading
import time
from datetime import datetime
from typing import Callable, Optional

import requests

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("telegram-service")


class TelegramService:
    """
    Telegram messaging service for Nora.
    
    Provides:
    - send_message(): Send text to configured contact
    - poll_messages(): Get new messages since last poll
    - start_listener(): Background thread for continuous polling
    """
    
    # Hardcoded configuration for Rana's contact
    BOT_TOKEN = "8549941701:AAEbPaGDh3G4GGtBPAVLC383Tbw1LDm-Lfc"
    CONTACT_CHAT_ID = 8341798694
    CONTACT_NAME = "Rana"
    
    BASE_URL = f"https://api.telegram.org/bot{BOT_TOKEN}"
    
    def __init__(self):
        self.last_update_id: Optional[int] = None
        self._listener_thread: Optional[threading.Thread] = None
        self._listener_running = False
        
    def send_message(self, text: str) -> bool:
        """
        Send a message to the configured contact.
        
        Args:
            text: The message text to send
            
        Returns:
            True if message sent successfully, False otherwise
        """
        try:
            url = f"{self.BASE_URL}/sendMessage"
            payload = {
                "chat_id": self.CONTACT_CHAT_ID,
                "text": text
            }
            
            response = requests.post(url, json=payload, timeout=10)
            response.raise_for_status()
            
            result = response.json()
            if result.get("ok"):
                logger.info(f"Message sent to {self.CONTACT_NAME}: {text[:50]}...")
                return True
            else:
                logger.error(f"Telegram API error: {result}")
                return False
                
        except requests.RequestException as e:
            logger.error(f"Failed to send message: {e}")
            return False
    
    def poll_messages(self) -> list[dict]:
        """
        Poll for new messages since last check.
        
        Returns:
            List of message dicts with keys: text, from_name, timestamp
        """
        try:
            url = f"{self.BASE_URL}/getUpdates"
            params = {
                "timeout": 1,  # Short timeout for polling
                "allowed_updates": ["message"]
            }
            
            # Only get updates after last processed one
            if self.last_update_id is not None:
                params["offset"] = self.last_update_id + 1
            
            response = requests.get(url, params=params, timeout=15)
            response.raise_for_status()
            
            result = response.json()
            if not result.get("ok"):
                logger.error(f"Telegram API error: {result}")
                return []
            
            messages = []
            for update in result.get("result", []):
                update_id = update.get("update_id", 0)
                
                # Track the latest update ID
                if self.last_update_id is None or update_id > self.last_update_id:
                    self.last_update_id = update_id
                
                message = update.get("message", {})
                
                # Only accept messages from configured chat
                chat_id = message.get("chat", {}).get("id")
                if chat_id != self.CONTACT_CHAT_ID:
                    logger.debug(f"Ignoring message from chat_id: {chat_id}")
                    continue
                
                # Get message text
                text = message.get("text", "")
                
                # Skip commands (like /start)
                if text.startswith("/"):
                    logger.debug(f"Ignoring command: {text}")
                    continue
                
                # Skip empty messages
                if not text.strip():
                    continue
                
                # Extract sender info
                from_user = message.get("from", {})
                from_name = from_user.get("first_name", "Unknown")
                if from_user.get("last_name"):
                    from_name += f" {from_user['last_name']}"
                
                # Convert timestamp
                timestamp = message.get("date", 0)
                timestamp_str = datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S")
                
                messages.append({
                    "text": text,
                    "from_name": from_name,
                    "timestamp": timestamp_str
                })
                
                logger.info(f"New message from {from_name}: {text[:50]}...")
            
            return messages
            
        except requests.RequestException as e:
            logger.error(f"Failed to poll messages: {e}")
            return []
    
    def start_listener(self, callback: Callable[[dict], None], poll_interval: int = 2) -> None:
        """
        Start background listener that polls for new messages.
        
        Args:
            callback: Function to call for each new message. 
                     Receives dict with: text, from_name, timestamp
            poll_interval: Seconds between polls (default: 2)
        """
        if self._listener_running:
            logger.warning("Listener already running")
            return
        
        def listener_loop():
            logger.info(f"Telegram listener started (polling every {poll_interval}s)")
            self._listener_running = True
            
            while self._listener_running:
                try:
                    messages = self.poll_messages()
                    for msg in messages:
                        try:
                            callback(msg)
                        except Exception as e:
                            logger.error(f"Callback error: {e}")
                except Exception as e:
                    logger.error(f"Listener error: {e}")
                
                time.sleep(poll_interval)
            
            logger.info("Telegram listener stopped")
        
        self._listener_thread = threading.Thread(target=listener_loop, daemon=True)
        self._listener_thread.start()
    
    def stop_listener(self) -> None:
        """Stop the background listener."""
        self._listener_running = False
        if self._listener_thread:
            self._listener_thread.join(timeout=5)
            self._listener_thread = None
            logger.info("Listener stopped")


# Singleton instance for easy import
_service: Optional[TelegramService] = None


def get_telegram_service() -> TelegramService:
    """Get the singleton TelegramService instance."""
    global _service
    if _service is None:
        _service = TelegramService()
    return _service
