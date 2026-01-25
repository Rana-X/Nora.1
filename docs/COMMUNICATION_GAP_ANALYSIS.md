# The Communication Gap Problem: Voice Agent Hallucinating Tool Results

## The Actual Problem You're Experiencing

**What's happening:**
1. User says: "Go to Amazon and add bananas to cart"
2. Voice agent says: "I added bananas to the cart" ← **BEFORE** the tool executes
3. Tool executes (10-30 seconds later)
4. Tool might fail or succeed
5. Voice agent never corrects itself if it failed

**Or:**
1. Tool succeeds and adds to cart
2. Voice agent says: "I couldn't add them" ← **WRONG**

This is a **timing hallucination** problem with OpenAI Realtime API.

## Root Cause: OpenAI Realtime API Behavior

Based on the documentation research:

### How OpenAI Realtime API SHOULD Work:
```
User speaks → LLM decides → Call tool → Wait for result → Speak based on result
```

### How It ACTUALLY Works (The Problem):
```
User speaks → LLM decides → "I'll add bananas!" (SPEAKS IMMEDIATELY)
                    ↓
               Call tool → Wait for result → (SILENT, IGNORES RESULT)
```

**From OpenAI Community (2025):**
> "Realtime API: No Response After Function Calling Until Next User Turn"
> 
> The Realtime API produces no response after function calling until the next user turn.

This is a **KNOWN BUG** in the OpenAI Realtime API.

## Why Your Current Prompt Doesn't Fix It

Your current system prompt (lines 72-75):

```
IMPORTANT - BEFORE USING BROWSER:
- You CANNOT speak while the browser is working (it takes 10-30 seconds)
- ALWAYS tell the user you're starting BEFORE calling the browse_and_act tool
- Example: "Okay, I'm heading to Amazon to find those bananas. Give me a moment to browse." -> then call tool
```

**This helps with speaking BEFORE the tool, but does nothing about:**
1. Speaking AFTER the tool
2. Accurately reporting what the tool actually did
3. Not hallucinating results

## The Function Tool Return Value

Look at lines 161-191 in your code:

```python
@function_tool()
async def browse_and_act(self, context: RunContext, instruction: str) -> str:
    # ... executes task ...
    
    # Returns detailed summary
    return summary  # ← This is what Claude actually did
```

**The tool returns a factual summary** like:
- "Successfully navigated to Amazon, searched for bananas, found 'Organic Bananas 3lb' and added 1 to cart"
- "Failed to add to cart - item was out of stock"
- "Encountered an error: Could not locate add to cart button"

**But OpenAI Realtime API often:**
- Speaks BEFORE receiving this return value
- Ignores the return value when it arrives
- Hallucinates what it THINKS happened

## Why This Happens

From LiveKit docs (line 7559):

> "The tool return value is automatically converted to a string before being sent to the LLM. **The LLM generates a new reply or additional tool calls based on the return value.**"

**Key phrase:** "generates a new reply"

But OpenAI Realtime API has a bug where:
1. It generates a reply **BEFORE** calling the tool
2. It doesn't generate a **SECOND** reply after getting the tool result
3. So the actual tool result is ignored

## The Solution (Based on Documentation)

From LiveKit docs (line 7559):

> "Return `None` or nothing at all to complete the tool silently **without requiring a reply from the LLM.**"

### Strategy: Force the Tool to Be Silent First

**Current behavior:**
```
User: "Add bananas"
Agent: "I'll add bananas!" → [calls tool] → [tool returns "added"] → [SILENT]
```

**Desired behavior:**
```
User: "Add bananas"
Agent: "Let me browse for that" → [calls tool] → [tool returns result] → Agent: "I added bananas to cart"
```

## The Fix (No Code Changes Needed - Just Prompt Engineering)

You need to update the system prompt to make the LLM understand:

### Current Prompt Issues:

**Line 63: "When performing browser tasks, give brief status updates."**
- ❌ This encourages speaking DURING the task

**Line 74: "ALWAYS tell the user you're starting BEFORE calling the browse_and_act tool"**
- ✓ Good for before
- ❌ Says nothing about after

**Line 73: "You CANNOT speak while the browser is working"**
- ✓ Good intent
- ❌ But doesn't prevent hallucinating results

### What the Prompt MUST Say:

```markdown
CRITICAL BROWSER TOOL RULES:

1. BEFORE calling browse_and_act:
   - Say: "Okay, let me browse for that. Give me a moment."
   - Do NOT describe what you will do (don't say "I'll add bananas")
   - Do NOT predict the outcome (don't say "I'll find them for you")
   
2. DURING tool execution:
   - The tool takes 10-30 seconds
   - You will be SILENT during this time
   - Do NOT try to speak
   
3. AFTER the tool returns:
   - The tool will tell you EXACTLY what happened
   - ONLY speak about what the tool result says
   - NEVER say you did something unless the tool confirms it
   - If the tool says "failed", you say it failed
   - If the tool says "added to cart", you say you added it
   
4. EXAMPLES:

   GOOD:
   User: "Add bananas to cart"
   You: "Okay, let me browse for that. Give me a moment."
   [Tool executes, returns: "Found Organic Bananas and added to cart"]
   You: "I found organic bananas and added them to your cart."
   
   BAD:
   User: "Add bananas to cart"
   You: "I'll add bananas to your cart!" ← NO! Don't claim you did it yet!
   [Tool executes, returns: "Item out of stock"]
   You: [silent] ← Now you never corrected yourself!
   
   GOOD (failure case):
   User: "Add bananas to cart"
   You: "Let me check Amazon for that."
   [Tool executes, returns: "Could not find add to cart button"]
   You: "I couldn't add them - I had trouble finding the button."
```

## The Function Tool Description

Your current description (lines 122-128):

```python
@function_tool()
async def browse_and_act(self, context: RunContext, instruction: str) -> str:
    """
    Execute a multi-step browser task.
    Use this for shopping, research, navigation, or any web interaction.
    Example: "Go to Amazon and add bananas to cart"

    Args:
        instruction: The task to perform in the browser, e.g. "Go to Amazon and add bananas to cart"
    """
```

**Problem:** Doesn't tell the LLM about the return value!

### Better Description:

```python
"""
Execute a multi-step browser task. 

IMPORTANT: 
- This tool takes 10-30 seconds to complete
- You MUST wait for the return value before speaking about results
- The return value tells you EXACTLY what happened
- NEVER claim you did something before this tool returns
- NEVER hallucinate results - only report what the return value says

The tool will return a detailed summary like:
- "Successfully added [item] to cart"
- "Could not find [item]"
- "Task failed: [reason]"

Use this for shopping, research, navigation, or any web interaction.

Args:
    instruction: The task to perform, e.g. "Go to Amazon and add bananas to cart"
    
Returns:
    A factual report of what actually happened in the browser
"""
```

## Why This Might Not Be Enough (OpenAI Realtime API Limitation)

Even with perfect prompting, the OpenAI Realtime API has a fundamental bug:

**From the research:**
> "The Realtime API produces no response after function calling until the next user turn"

This means:
1. The LLM calls the tool
2. The tool returns a result
3. **The LLM doesn't automatically speak after getting the result**
4. It waits for the user to speak again

### Workaround (From LiveKit Docs)

From the search results:

> "An `automaticallyTriggerResponseForMcpToolCalls` option can be set to `true` (the default) to auto-trigger a model response when an MCP tool call completes."

But this is for MCP tools, not standard function tools.

## The Real Solution: Use LiveKit's Voice Pipeline Instead

Your current stack:
```
OpenAI Realtime API (STT + LLM + TTS in one)
```

**Problem:** OpenAI Realtime API is conversational-first, tools are an afterthought

**Alternative:** LiveKit's traditional voice pipeline
```
Deepgram STT → OpenAI GPT-4 → Cartesia TTS
```

**Why this works better:**
- GPT-4 (non-realtime) has mature, reliable function calling
- LLM **always** responds after tool execution
- No timing bugs
- More control over the pipeline

## Comparison: Realtime API vs Traditional Pipeline

| Feature | OpenAI Realtime | Traditional Pipeline |
|---------|----------------|---------------------|
| Latency | Very Low (100-300ms) | Higher (300-800ms) |
| Function calling reliability | **BUGGY** ❌ | **SOLID** ✓ |
| Speaks after tools | **NO** (known bug) | **YES** (guaranteed) |
| Tool result accuracy | Hallucinates ❌ | Accurate ✓ |
| Interruption handling | Native ✓ | Requires VAD |
| Cost | Medium | Medium |
| Complexity | Simple | More config |

**For your use case (tool-heavy with Orgo):**
- Traditional pipeline is better
- You NEED reliable tool execution
- Extra 200ms latency is acceptable for elderly user
- Accuracy > Speed for shopping tasks

## Summary: The Communication Gap Explained

### What You Thought Was Happening:
"Voice agent and Orgo are not synced"

### What's Actually Happening:
1. **OpenAI Realtime API speaks BEFORE calling tools** (hallucinating what it will do)
2. **OpenAI Realtime API ignores tool return values** (doesn't speak after tools complete)
3. **Result:** Agent claims success/failure without knowing actual outcome

### Why Your Code Is Fine:
- ✓ Tools return accurate results
- ✓ Orgo executes correctly
- ✓ VNC shows real browser state
- ✓ Data channel works
- ❌ OpenAI Realtime API just ignores the tool results

### The Communication Gap Is:
**Between OpenAI Realtime API and its own function tools**

Not between:
- ~~Voice agent and Orgo~~ (these communicate fine via the function call)
- ~~Frontend and backend~~ (VNC shows correct state)

### Solutions (In Order of Preference):

1. **Switch to Traditional Voice Pipeline** (Best)
   - Use Deepgram + GPT-4 + Cartesia
   - Reliable function calling
   - Agent speaks accurate results after tool execution
   
2. **Aggressive Prompt Engineering** (Partial Fix)
   - Force agent to be vague before tools: "Let me check"
   - Never claim outcomes before tool returns
   - Might reduce hallucinations, won't fix the core bug
   
3. **Wait for OpenAI to Fix Their Bug** (Unreliable)
   - Known issue since 2025
   - No timeline for fix
   - Not under your control

## Example of The Bug in Action

**Current behavior with your prompt:**

```
User: "Add organic bananas to my cart"
Agent: "Okay, I'm heading to Amazon to find those bananas. Give me a moment to browse."
[browse_and_act() called]
[Agent goes silent for 20 seconds]
[Orgo Claude: navigates to Amazon, searches "organic bananas", finds item, adds to cart]
[Tool returns: "Successfully navigated to Amazon.com, searched for 'organic bananas', found 'Organic Bananas, 3 lbs' for $4.99, and added 1 to your cart. Cart total is now $4.99."]
[Agent receives this return value]
[Agent remains SILENT] ← BUG: Should speak but doesn't
[User waits awkwardly]
User: "Did it work?" ← User has to prompt again
Agent: "Yes! I added organic bananas to your cart for $4.99."
```

**What you want:**

```
User: "Add organic bananas to my cart"  
Agent: "Let me browse for that. One moment."
[Tool executes for 20 seconds]
[Tool returns result]
Agent: "Done! I added organic bananas to your cart for $4.99." ← AUTOMATIC
```

The second one **requires fixing the OpenAI Realtime API bug or switching away from it**.

---

## No Code Changes Needed? 

**Correct - the code is fine.**

The problem is:
1. **OpenAI Realtime API limitation** (can't fix without changing APIs)
2. **Prompt needs improvement** (can help reduce hallucinations)

But you can't fully fix this communication gap without either:
- Switching to traditional voice pipeline, OR
- Waiting for OpenAI to fix their Realtime API bug
