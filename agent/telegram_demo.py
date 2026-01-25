#!/usr/bin/env python3
"""
Telegram Demo for Nora
======================

Simple script to test Telegram integration before connecting to Nora's voice system.

Usage:
    python telegram_demo.py

Features:
- Sends "Hello from Nora!" on startup
- Listens for incoming messages and prints them
- Allows you to type messages to send
"""

import sys
from telegram_service import get_telegram_service


def on_message_received(message: dict) -> None:
    """Callback when a new message arrives."""
    print(f"\n📨 Message from {message['from_name']} at {message['timestamp']}:")
    print(f"   {message['text']}")
    print("\nType a message to send (or 'quit' to exit): ", end="", flush=True)


def main():
    print("=" * 50)
    print("  Nora Telegram Demo")
    print("=" * 50)
    print()
    
    # Get the telegram service
    telegram = get_telegram_service()
    
    # Send hello message on startup
    print("Sending startup message...")
    if telegram.send_message("Hello from Nora! 👋"):
        print("✅ Startup message sent successfully!")
    else:
        print("❌ Failed to send startup message")
        print("   Check your bot token and chat ID")
        sys.exit(1)
    
    print()
    print("Starting message listener...")
    telegram.start_listener(on_message_received, poll_interval=2)
    print("✅ Listener started (polling every 2 seconds)")
    print()
    print("-" * 50)
    print("You can now:")
    print("  • Type messages here to send to Rana")
    print("  • Messages from Rana will appear automatically")
    print("  • Type 'quit' to exit")
    print("-" * 50)
    print()
    
    try:
        while True:
            user_input = input("Type a message to send (or 'quit' to exit): ")
            
            if user_input.lower() == 'quit':
                print("\nGoodbye! 👋")
                break
            
            if user_input.strip():
                if telegram.send_message(user_input):
                    print(f"✅ Sent: {user_input}")
                else:
                    print("❌ Failed to send message")
            
    except KeyboardInterrupt:
        print("\n\nInterrupted. Goodbye! 👋")
    finally:
        telegram.stop_listener()


if __name__ == "__main__":
    main()
