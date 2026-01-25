#!/usr/bin/env python3
"""
Network Connectivity Check for Nora AI Agent
============================================

Tests all external services that Nora depends on:
1. LiveKit Cloud (WebSocket)
2. OpenAI API (Realtime + Chat)
3. Beyond Presence Avatar API
4. Orgo.ai Browser Control API
5. Telegram Bot API
6. QuickCare Pharmacy Website
7. Anthropic API (Claude for browser vision)
"""

import asyncio
import os
import sys
import time
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv(dotenv_path="../.env")

# Results tracking
results = []

def log_result(service: str, status: str, details: str = "", latency_ms: float = None):
    """Record a test result."""
    emoji = "✅" if status == "OK" else "❌" if status == "FAIL" else "⚠️"
    latency_str = f" ({latency_ms:.0f}ms)" if latency_ms else ""
    print(f"{emoji} {service}: {status}{latency_str}")
    if details:
        print(f"   └─ {details}")
    results.append({
        "service": service,
        "status": status,
        "details": details,
        "latency_ms": latency_ms
    })

def check_env_var(name: str) -> str:
    """Check if environment variable is set."""
    value = os.environ.get(name)
    if not value:
        log_result(f"ENV: {name}", "FAIL", "Not set in environment")
        return None
    return value

# =============================================================================
# 1. LiveKit Cloud
# =============================================================================
def check_livekit():
    """Test LiveKit Cloud connectivity."""
    import requests
    
    livekit_url = check_env_var("LIVEKIT_URL")
    api_key = check_env_var("LIVEKIT_API_KEY")
    api_secret = check_env_var("LIVEKIT_API_SECRET")
    
    if not all([livekit_url, api_key, api_secret]):
        return
    
    # Convert WSS URL to HTTPS for API check
    http_url = livekit_url.replace("wss://", "https://")
    
    try:
        start = time.time()
        # Try to reach the server (will get 401 without proper auth, but confirms network)
        response = requests.get(http_url, timeout=10)
        latency = (time.time() - start) * 1000
        
        # Any response (even 401/403) means network is working
        if response.status_code in [200, 401, 403, 404]:
            log_result("LiveKit Cloud", "OK", f"Server reachable at {livekit_url}", latency)
        else:
            log_result("LiveKit Cloud", "WARN", f"HTTP {response.status_code}", latency)
    except requests.RequestException as e:
        log_result("LiveKit Cloud", "FAIL", str(e))

# =============================================================================
# 2. OpenAI API
# =============================================================================
def check_openai():
    """Test OpenAI API connectivity."""
    import requests
    
    api_key = check_env_var("OPENAI_API_KEY")
    if not api_key:
        return
    
    try:
        start = time.time()
        response = requests.get(
            "https://api.openai.com/v1/models",
            headers={"Authorization": f"Bearer {api_key}"},
            timeout=10
        )
        latency = (time.time() - start) * 1000
        
        if response.status_code == 200:
            models = response.json().get("data", [])
            # Check for realtime model
            model_ids = [m.get("id", "") for m in models]
            has_realtime = any("realtime" in mid for mid in model_ids)
            log_result("OpenAI API", "OK", f"Realtime available: {has_realtime}", latency)
        elif response.status_code == 401:
            log_result("OpenAI API", "FAIL", "Invalid API key")
        else:
            log_result("OpenAI API", "WARN", f"HTTP {response.status_code}", latency)
    except requests.RequestException as e:
        log_result("OpenAI API", "FAIL", str(e))

# =============================================================================
# 3. Beyond Presence Avatar API
# =============================================================================
def check_beyond_presence():
    """Test Beyond Presence avatar API connectivity."""
    import requests
    
    api_key = check_env_var("BEY_API_KEY")
    avatar_id = check_env_var("BEY_AVATAR_ID")
    
    if not api_key:
        api_key = os.environ.get("BEYOND_PRESENCE_API_KEY")
    
    if not api_key or not avatar_id:
        return
    
    try:
        start = time.time()
        # Test the Beyond Presence API endpoint
        response = requests.get(
            "https://api.beyondpresence.ai/v1/avatars",
            headers={"Authorization": f"Bearer {api_key}"},
            timeout=10
        )
        latency = (time.time() - start) * 1000
        
        if response.status_code == 200:
            log_result("Beyond Presence API", "OK", f"Avatar ID: {avatar_id[:8]}...", latency)
        elif response.status_code == 401:
            log_result("Beyond Presence API", "FAIL", "Invalid API key")
        else:
            # Try domain reachability
            response2 = requests.get("https://beyondpresence.ai", timeout=10)
            if response2.status_code == 200:
                log_result("Beyond Presence API", "WARN", f"Domain reachable, API returned {response.status_code}", latency)
            else:
                log_result("Beyond Presence API", "FAIL", f"HTTP {response.status_code}", latency)
    except requests.RequestException as e:
        log_result("Beyond Presence API", "FAIL", str(e))

# =============================================================================
# 4. Orgo.ai Browser Control API
# =============================================================================
def check_orgo():
    """Test Orgo.ai API and VNC connectivity."""
    import requests
    
    api_key = check_env_var("ORGO_API_KEY")
    computer_id = check_env_var("ORGO_COMPUTER_ID")
    vnc_host = os.environ.get("ORGO_VNC_HOST")
    
    if not api_key:
        return
    
    # Check API
    try:
        start = time.time()
        response = requests.get(
            "https://api.orgo.ai/api/v1/computers",
            headers={"Authorization": f"Bearer {api_key}"},
            timeout=10
        )
        latency = (time.time() - start) * 1000
        
        if response.status_code == 200:
            log_result("Orgo.ai API", "OK", f"Computer ID: {computer_id[:8]}..." if computer_id else "No computer ID", latency)
        elif response.status_code == 401:
            log_result("Orgo.ai API", "FAIL", "Invalid API key")
        else:
            log_result("Orgo.ai API", "WARN", f"HTTP {response.status_code}", latency)
    except requests.RequestException as e:
        log_result("Orgo.ai API", "FAIL", str(e))
    
    # Check VNC host reachability (if configured)
    if vnc_host:
        try:
            start = time.time()
            response = requests.get(f"https://{vnc_host}", timeout=10)
            latency = (time.time() - start) * 1000
            log_result("Orgo VNC Host", "OK", f"{vnc_host}", latency)
        except requests.RequestException as e:
            log_result("Orgo VNC Host", "WARN", f"Could not reach {vnc_host}: {e}")

# =============================================================================
# 5. Telegram Bot API
# =============================================================================
def check_telegram():
    """Test Telegram Bot API connectivity."""
    import requests
    
    # Use hardcoded bot token from telegram_service.py
    bot_token = "8549941701:AAEbPaGDh3G4GGtBPAVLC383Tbw1LDm-Lfc"
    
    try:
        start = time.time()
        response = requests.get(
            f"https://api.telegram.org/bot{bot_token}/getMe",
            timeout=10
        )
        latency = (time.time() - start) * 1000
        
        if response.status_code == 200:
            data = response.json()
            if data.get("ok"):
                bot_name = data.get("result", {}).get("first_name", "Unknown")
                log_result("Telegram Bot API", "OK", f"Bot: {bot_name}", latency)
            else:
                log_result("Telegram Bot API", "FAIL", data.get("description", "Unknown error"))
        elif response.status_code == 401:
            log_result("Telegram Bot API", "FAIL", "Invalid bot token")
        else:
            log_result("Telegram Bot API", "WARN", f"HTTP {response.status_code}", latency)
    except requests.RequestException as e:
        log_result("Telegram Bot API", "FAIL", str(e))

# =============================================================================
# 6. QuickCare Pharmacy Website
# =============================================================================
def check_quickcare():
    """Test QuickCare pharmacy website connectivity."""
    import requests
    
    try:
        start = time.time()
        response = requests.get(
            "https://quickcare-flow.vercel.app/",
            timeout=15
        )
        latency = (time.time() - start) * 1000
        
        if response.status_code == 200:
            log_result("QuickCare Pharmacy", "OK", "Website reachable", latency)
        else:
            log_result("QuickCare Pharmacy", "WARN", f"HTTP {response.status_code}", latency)
    except requests.RequestException as e:
        log_result("QuickCare Pharmacy", "FAIL", str(e))

# =============================================================================
# 7. Anthropic API (Claude)
# =============================================================================
def check_anthropic():
    """Test Anthropic API connectivity."""
    import requests
    
    api_key = check_env_var("ANTHROPIC_API_KEY")
    if not api_key:
        return
    
    try:
        start = time.time()
        response = requests.get(
            "https://api.anthropic.com/v1/models",
            headers={
                "x-api-key": api_key,
                "anthropic-version": "2023-06-01"
            },
            timeout=10
        )
        latency = (time.time() - start) * 1000
        
        if response.status_code == 200:
            models = response.json().get("data", [])
            log_result("Anthropic API", "OK", f"{len(models)} models available", latency)
        elif response.status_code == 401:
            log_result("Anthropic API", "FAIL", "Invalid API key")
        else:
            log_result("Anthropic API", "WARN", f"HTTP {response.status_code}", latency)
    except requests.RequestException as e:
        log_result("Anthropic API", "FAIL", str(e))

# =============================================================================
# 8. Amazon.com (for shopping tasks)
# =============================================================================
def check_amazon():
    """Test Amazon.com connectivity."""
    import requests
    
    try:
        start = time.time()
        response = requests.get(
            "https://www.amazon.com/",
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=15
        )
        latency = (time.time() - start) * 1000
        
        if response.status_code == 200:
            log_result("Amazon.com", "OK", "Website reachable", latency)
        else:
            log_result("Amazon.com", "WARN", f"HTTP {response.status_code}", latency)
    except requests.RequestException as e:
        log_result("Amazon.com", "FAIL", str(e))

# =============================================================================
# Main
# =============================================================================
def main():
    print("=" * 60)
    print("Nora AI Agent - Network Connectivity Check")
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    print()
    
    print("🔍 Checking environment variables and services...\n")
    
    check_livekit()
    check_openai()
    check_beyond_presence()
    check_orgo()
    check_anthropic()
    check_telegram()
    check_quickcare()
    check_amazon()
    
    print()
    print("=" * 60)
    print("Summary")
    print("=" * 60)
    
    ok_count = sum(1 for r in results if r["status"] == "OK")
    warn_count = sum(1 for r in results if r["status"] == "WARN")
    fail_count = sum(1 for r in results if r["status"] == "FAIL")
    
    print(f"✅ OK: {ok_count}  ⚠️ WARN: {warn_count}  ❌ FAIL: {fail_count}")
    
    if fail_count > 0:
        print("\n🚨 Failed services:")
        for r in results:
            if r["status"] == "FAIL":
                print(f"   - {r['service']}: {r['details']}")
    
    if warn_count > 0:
        print("\n⚠️ Warnings:")
        for r in results:
            if r["status"] == "WARN":
                print(f"   - {r['service']}: {r['details']}")
    
    print()
    return 0 if fail_count == 0 else 1

if __name__ == "__main__":
    sys.exit(main())
