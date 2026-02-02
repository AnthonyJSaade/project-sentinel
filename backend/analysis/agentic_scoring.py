#!/usr/bin/env python3
"""
==============================================================================
AGENTIC SCORING: Multi-Agent Debate System for Project Sentinel
==============================================================================

Purpose:
    Replaces simple heuristic scoring with an LLM-based "Multi-Agent Debate"
    system using LangGraph. Three agents analyze each conflict event:
    
    1. ANALYST_PRO:  Argues for event credibility (supporting evidence)
    2. ANALYST_CON:  Argues against credibility (doubts/gaps in evidence)
    3. JUDGE:        Weighs both arguments, outputs final score with reasoning

Architecture:
    -------------------------------------------------------------------------
    Uses LangGraph's StateGraph to orchestrate the debate flow:
    
    [Event + Evidence] → [Analyst_Pro] → [Analyst_Con] → [Judge] → [Score]
    
    The Judge outputs structured JSON validated by Pydantic:
    {
        "score": 0-100,
        "status": "Confirmed" | "Plausible" | "Unverified",
        "reasoning": "Detailed explanation..."
    }
    -------------------------------------------------------------------------

Cost Optimization:
    - Only processes events with heuristic score > 0 (has some evidence)
    - Batches context to minimize token usage
    - Falls back to mock mode if API keys not configured

Author: Project Sentinel Team
Created: 2026
==============================================================================
"""

import json
import os
import re
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional, Annotated
from operator import add

from dotenv import load_dotenv
from pydantic import BaseModel, Field

try:
    from langchain_core.messages import HumanMessage, AIMessage, SystemMessage
    from langchain_core.prompts import ChatPromptTemplate
    from langchain_anthropic import ChatAnthropic
    from langgraph.graph import StateGraph, END
except ImportError as e:
    print("❌ ERROR: LangChain/LangGraph dependencies not installed.")
    print("   Run: pip install langchain langchain-anthropic langgraph")
    print(f"   Missing: {e}")
    sys.exit(1)

try:
    from neo4j import GraphDatabase
    from neo4j.exceptions import ServiceUnavailable, AuthError
except ImportError:
    print("❌ ERROR: neo4j driver not installed.")
    print("   Run: pip install neo4j")
    sys.exit(1)


# ==============================================================================
# CONFIGURATION
# ==============================================================================

load_dotenv()

# Neo4j connection settings
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "sentinel_password")

# LLM Configuration
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")
LLM_MODEL = "claude-sonnet-4-20250514"  # Claude 4.5 Opus equivalent
LLM_TEMPERATURE = 0.3  # Lower for more consistent structured output

# Processing thresholds
MIN_HEURISTIC_SCORE = 1  # Only process events with score > 0
DEBUG_MODE = os.getenv("DEBUG", "false").lower() == "true"


# ==============================================================================
# PYDANTIC MODELS FOR STRUCTURED OUTPUT
# ==============================================================================

class JudgmentOutput(BaseModel):
    """Structured output from the Judge agent."""
    score: int = Field(
        description="Confidence score from 0-100",
        ge=0, le=100
    )
    status: str = Field(
        description="Event classification: 'Confirmed', 'Plausible', or 'Unverified'"
    )
    reasoning: str = Field(
        description="Detailed explanation of the judgment, 2-3 sentences"
    )


class AnalystArgument(BaseModel):
    """Structured output from analyst agents."""
    key_points: List[str] = Field(
        description="List of 2-4 key points supporting/doubting the event"
    )
    confidence: str = Field(
        description="Analyst's confidence: 'High', 'Medium', or 'Low'"
    )
    summary: str = Field(
        description="One sentence summary of the argument"
    )


# ==============================================================================
# LANGGRAPH STATE DEFINITION
# ==============================================================================

class DebateState(dict):
    """
    State object passed through the debate graph.
    
    Contains event data, evidence context, and agent outputs.
    """
    # Input data
    event_id: str
    event_data: Dict[str, Any]
    evidence_context: str
    heuristic_score: int
    
    # Agent outputs (accumulated during debate)
    pro_argument: Optional[Dict] = None
    con_argument: Optional[Dict] = None
    judgment: Optional[Dict] = None
    
    # Metadata
    error: Optional[str] = None


# ==============================================================================
# AGENT PROMPTS
# ==============================================================================

ANALYST_PRO_PROMPT = """You are Analyst_Pro, a senior intelligence analyst tasked with SUPPORTING the credibility of a reported conflict event.

Your role is to find and emphasize evidence that corroborates this event actually happened.

EVENT DATA:
{event_data}

SUPPORTING EVIDENCE:
{evidence_context}

HEURISTIC SCORE (baseline): {heuristic_score}/100

Analyze the evidence and argue FOR the event's credibility. Focus on:
- Multiple independent sources reporting the same event
- Military aircraft detected in the area (strongest signal)
- Tier 1 sources (wire services like NPR, BBC) confirming
- Geographic and temporal consistency
- Historical patterns supporting this type of event

Respond with a JSON object containing:
- "key_points": List of 2-4 specific points supporting credibility
- "confidence": Your confidence level ("High", "Medium", or "Low")  
- "summary": One sentence summarizing why this event is credible"""

ANALYST_CON_PROMPT = """You are Analyst_Con, a senior intelligence analyst tasked with CHALLENGING the credibility of a reported conflict event.

Your role is to find gaps, inconsistencies, and reasons to doubt this event.

EVENT DATA:
{event_data}

EVIDENCE PRESENTED:
{evidence_context}

PRO ARGUMENT (from colleague):
{pro_argument}

HEURISTIC SCORE (baseline): {heuristic_score}/100

Analyze the evidence and argue AGAINST the event's credibility. Focus on:
- Lack of Tier 1 source confirmation
- Over-reliance on Telegram/social media (potential bot swarms)
- No military aircraft detected (missing kinetic confirmation)
- Vague geographic data or inconsistent timestamps
- Potential for misinformation or propaganda

Respond with a JSON object containing:
- "key_points": List of 2-4 specific doubts or gaps in evidence
- "confidence": Your confidence that doubts are valid ("High", "Medium", or "Low")
- "summary": One sentence summarizing why this event should be questioned"""

JUDGE_PROMPT = """You are the Judge, the final arbiter of intelligence credibility for Project Sentinel.

You have heard arguments from two analysts:
- Analyst_Pro argued FOR the event's credibility
- Analyst_Con argued AGAINST the event's credibility

EVENT DATA:
{event_data}

ANALYST_PRO'S ARGUMENT:
{pro_argument}

ANALYST_CON'S ARGUMENT:
{con_argument}

BASELINE HEURISTIC SCORE: {heuristic_score}/100

Weigh both arguments carefully and render your judgment. Consider:
- Strength of corroborating evidence (especially Tier 1 sources and military signals)
- Validity of doubts raised (especially lack of official confirmation)
- Historical context of conflict in this region
- Risk of false positives vs false negatives

Respond with a JSON object containing:
- "score": Final confidence score (0-100)
- "status": Classification - "Confirmed" (>60), "Plausible" (30-60), or "Unverified" (<30)
- "reasoning": 2-3 sentences explaining your judgment, referencing specific evidence"""


# ==============================================================================
# LLM INITIALIZATION
# ==============================================================================

def get_llm(mock: bool = False):
    """
    Initialize the LLM for agent use.
    
    Args:
        mock: If True, return a mock LLM for testing without API keys
        
    Returns:
        ChatAnthropic instance or MockLLM
    """
    if mock or not ANTHROPIC_API_KEY:
        print("   ⚠️  Using MOCK LLM (no API key configured)")
        return MockLLM()
    
    return ChatAnthropic(
        model=LLM_MODEL,
        temperature=LLM_TEMPERATURE,
        api_key=ANTHROPIC_API_KEY,
        max_tokens=1024,
    )


class MockLLM:
    """Mock LLM for testing without API calls."""
    
    def invoke(self, messages: List) -> AIMessage:
        """Return mock responses based on agent type."""
        last_message = messages[-1].content if messages else ""
        
        if "SUPPORTING" in last_message or "Analyst_Pro" in last_message:
            response = json.dumps({
                "key_points": [
                    "Multiple sources report this event",
                    "Geographic coordinates are consistent",
                    "Timing aligns with regional patterns"
                ],
                "confidence": "Medium",
                "summary": "Evidence suggests credible report with moderate corroboration."
            })
        elif "CHALLENGING" in last_message or "Analyst_Con" in last_message:
            response = json.dumps({
                "key_points": [
                    "No Tier 1 source confirmation",
                    "Reliance on social media sources",
                    "No military flight correlation"
                ],
                "confidence": "Medium", 
                "summary": "Gaps in official confirmation warrant caution."
            })
        else:  # Judge
            response = json.dumps({
                "score": 45,
                "status": "Plausible",
                "reasoning": "Event has moderate supporting evidence but lacks official confirmation. Multiple sources increase credibility, but absence of Tier 1 verification and military signals suggests treating as plausible but unconfirmed."
            })
        
        return AIMessage(content=response)


# ==============================================================================
# JSON EXTRACTION HELPER
# ==============================================================================

def extract_json(text: str) -> Dict:
    """
    Extract JSON from LLM response, handling markdown code blocks.
    
    Claude often wraps JSON in ```json ... ``` blocks. This function
    handles that and extracts the raw JSON.
    
    Args:
        text: Raw LLM response text
        
    Returns:
        Parsed JSON dict
        
    Raises:
        json.JSONDecodeError: If no valid JSON found
    """
    if not text or not text.strip():
        raise json.JSONDecodeError("Empty response", text or "", 0)
    
    # Try direct JSON parse first
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    
    # Try extracting from markdown code block
    # Pattern: ```json ... ``` or ``` ... ```
    code_block_pattern = r'```(?:json)?\s*\n?(.*?)\n?```'
    match = re.search(code_block_pattern, text, re.DOTALL)
    if match:
        json_str = match.group(1).strip()
        return json.loads(json_str)
    
    # Try finding JSON object pattern { ... }
    json_pattern = r'\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}'
    match = re.search(json_pattern, text, re.DOTALL)
    if match:
        return json.loads(match.group(0))
    
    # Last resort: raise error with actual content for debugging
    raise json.JSONDecodeError(f"No JSON found in: {text[:200]}...", text, 0)


# ==============================================================================
# LANGGRAPH NODES (AGENT FUNCTIONS)
# ==============================================================================

def analyst_pro_node(state: Dict) -> Dict:
    """
    Analyst_Pro: Generates arguments SUPPORTING event credibility.
    """
    llm = get_llm(mock=not ANTHROPIC_API_KEY)
    
    prompt = ANALYST_PRO_PROMPT.format(
        event_data=json.dumps(state.get("event_data", {}), indent=2),
        evidence_context=state.get("evidence_context", "No evidence provided"),
        heuristic_score=state.get("heuristic_score", 0)
    )
    
    try:
        response = llm.invoke([HumanMessage(content=prompt)])
        if DEBUG_MODE:
            print(f"      [DEBUG] Pro response: {response.content[:200]}...")
        pro_argument = extract_json(response.content)
        return {"pro_argument": pro_argument}
    except Exception as e:
        if DEBUG_MODE:
            print(f"      [DEBUG] Pro error: {e}")
        return {"pro_argument": {"error": str(e), "key_points": [], "confidence": "Low", "summary": "Analysis failed"}}


def analyst_con_node(state: Dict) -> Dict:
    """
    Analyst_Con: Generates arguments AGAINST event credibility.
    """
    llm = get_llm(mock=not ANTHROPIC_API_KEY)
    
    prompt = ANALYST_CON_PROMPT.format(
        event_data=json.dumps(state.get("event_data", {}), indent=2),
        evidence_context=state.get("evidence_context", "No evidence provided"),
        pro_argument=json.dumps(state.get("pro_argument", {}), indent=2),
        heuristic_score=state.get("heuristic_score", 0)
    )
    
    try:
        response = llm.invoke([HumanMessage(content=prompt)])
        if DEBUG_MODE:
            print(f"      [DEBUG] Con response: {response.content[:200]}...")
        con_argument = extract_json(response.content)
        return {"con_argument": con_argument}
    except Exception as e:
        if DEBUG_MODE:
            print(f"      [DEBUG] Con error: {e}")
        return {"con_argument": {"error": str(e), "key_points": [], "confidence": "Low", "summary": "Analysis failed"}}


def judge_node(state: Dict) -> Dict:
    """
    Judge: Weighs both arguments and renders final judgment with score.
    """
    llm = get_llm(mock=not ANTHROPIC_API_KEY)
    
    prompt = JUDGE_PROMPT.format(
        event_data=json.dumps(state.get("event_data", {}), indent=2),
        pro_argument=json.dumps(state.get("pro_argument", {}), indent=2),
        con_argument=json.dumps(state.get("con_argument", {}), indent=2),
        heuristic_score=state.get("heuristic_score", 0)
    )
    
    try:
        response = llm.invoke([HumanMessage(content=prompt)])
        if DEBUG_MODE:
            print(f"      [DEBUG] Judge response: {response.content[:200]}...")
        judgment = extract_json(response.content)
        
        # Validate with Pydantic
        validated = JudgmentOutput(**judgment)
        return {"judgment": validated.model_dump()}
    except Exception as e:
        if DEBUG_MODE:
            print(f"      [DEBUG] Judge error: {e}")
        # Fallback to heuristic score if LLM fails
        heuristic = state.get("heuristic_score", 0)
        status = "Confirmed" if heuristic > 60 else "Plausible" if heuristic > 30 else "Unverified"
        return {
            "judgment": {
                "score": heuristic,
                "status": status,
                "reasoning": f"LLM analysis failed ({str(e)}), using heuristic score."
            }
        }


# ==============================================================================
# LANGGRAPH WORKFLOW
# ==============================================================================

def build_debate_graph():
    """
    Build the LangGraph workflow for multi-agent debate.
    
    Flow: analyst_pro → analyst_con → judge → END
    """
    workflow = StateGraph(dict)
    
    # Add nodes
    workflow.add_node("analyst_pro", analyst_pro_node)
    workflow.add_node("analyst_con", analyst_con_node)
    workflow.add_node("judge", judge_node)
    
    # Define edges (linear flow)
    workflow.set_entry_point("analyst_pro")
    workflow.add_edge("analyst_pro", "analyst_con")
    workflow.add_edge("analyst_con", "judge")
    workflow.add_edge("judge", END)
    
    return workflow.compile()


# ==============================================================================
# NEO4J INTEGRATION
# ==============================================================================

class AgenticScorer:
    """
    Orchestrates multi-agent debate scoring for Neo4j events.
    """
    
    def __init__(self, uri: str, user: str, password: str):
        """Initialize Neo4j connection and LangGraph workflow."""
        self.driver = None
        self.graph = build_debate_graph()
        
        try:
            self.driver = GraphDatabase.driver(uri, auth=(user, password))
            with self.driver.session() as session:
                session.run("RETURN 1")
            print(f"   ✓ Connected to Neo4j at {uri}")
        except ServiceUnavailable:
            print(f"   ❌ Cannot connect to Neo4j at {uri}")
            print("      Is Docker running? Try: docker compose up -d")
            sys.exit(1)
        except AuthError:
            print(f"   ❌ Authentication failed for user '{user}'")
            sys.exit(1)
    
    def close(self):
        """Close the driver connection."""
        if self.driver:
            self.driver.close()
    
    def run_query(self, query: str, parameters: Dict = None) -> List[Dict]:
        """Execute a Cypher query and return results."""
        with self.driver.session() as session:
            result = session.run(query, parameters or {})
            return [record.data() for record in result]
    
    def get_events_for_debate(self) -> List[Dict]:
        """
        Get events with heuristic score > 0 for agentic analysis.
        
        Returns:
            List of events with their linked evidence
        """
        query = """
        MATCH (e:Event)
        WHERE e.confidence_score IS NOT NULL 
          AND e.confidence_score > $min_score
        
        // Get linked articles
        OPTIONAL MATCH (a:Article)-[:CORROBORATES]->(e)
        OPTIONAL MATCH (s:Source)-[:PUBLISHED]->(a)
        
        // Get linked posts
        OPTIONAL MATCH (p:Post)-[:CORROBORATES]->(e)
        
        // Get linked flights
        OPTIONAL MATCH (f:Flight)-[:DETECTED_NEAR]->(e)
        
        // Get location
        OPTIONAL MATCH (e)-[:OCCURRED_IN]->(loc:Location)
        
        WITH e, loc,
             collect(DISTINCT {title: a.title, source: s.name, tier: s.tier}) AS articles,
             collect(DISTINCT {text: left(p.text, 200), priority: p.priority}) AS posts,
             collect(DISTINCT {callsign: f.callsign, tag: f.tag, country: f.origin_country}) AS flights
        
        RETURN 
            e.id AS event_id,
            e.timestamp AS timestamp,
            e.lat AS lat,
            e.lon AS lon,
            e.event_code AS event_code,
            e.confidence_score AS heuristic_score,
            loc.name AS location,
            articles,
            posts,
            flights
        ORDER BY e.confidence_score DESC
        LIMIT 50
        """
        
        return self.run_query(query, {"min_score": MIN_HEURISTIC_SCORE})
    
    def build_evidence_context(self, event: Dict) -> str:
        """
        Build a text summary of evidence for LLM consumption.
        
        Args:
            event: Event dict with linked evidence
            
        Returns:
            Formatted evidence string
        """
        lines = []
        
        # Articles
        articles = event.get("articles", [])
        if articles:
            lines.append("NEWS ARTICLES:")
            for a in articles[:5]:  # Limit to 5
                if a.get("title"):
                    tier = a.get("tier", "?")
                    source = a.get("source", "Unknown")
                    lines.append(f"  - [{source}, Tier {tier}] {a['title']}")
        
        # Telegram posts
        posts = event.get("posts", [])
        if posts:
            lines.append("\nTELEGRAM POSTS:")
            for p in posts[:3]:  # Limit to 3
                if p.get("text"):
                    priority = "🔴" if p.get("priority") == "high" else "⚪"
                    lines.append(f"  - {priority} {p['text'][:100]}...")
        
        # Flights
        flights = event.get("flights", [])
        if flights:
            lines.append("\nDETECTED FLIGHTS:")
            for f in flights[:3]:  # Limit to 3
                tag = "⚠️ MILITARY" if f.get("tag") == "high_altitude_fast" else "civilian"
                callsign = f.get("callsign", "Unknown")
                country = f.get("country", "Unknown")
                lines.append(f"  - {callsign} ({country}) - {tag}")
        
        if not lines:
            return "No linked evidence found."
        
        return "\n".join(lines)
    
    def score_event(self, event: Dict) -> Dict:
        """
        Run multi-agent debate for a single event.
        
        Args:
            event: Event dict with evidence
            
        Returns:
            Judgment dict with score, status, reasoning
        """
        # Build event data summary
        event_data = {
            "id": event.get("event_id"),
            "timestamp": event.get("timestamp"),
            "location": event.get("location"),
            "coordinates": f"{event.get('lat')}, {event.get('lon')}",
            "event_code": event.get("event_code"),
        }
        
        # Build evidence context
        evidence_context = self.build_evidence_context(event)
        
        # Initial state for LangGraph
        initial_state = {
            "event_id": event.get("event_id"),
            "event_data": event_data,
            "evidence_context": evidence_context,
            "heuristic_score": event.get("heuristic_score", 0),
        }
        
        # Run the debate graph
        final_state = self.graph.invoke(initial_state)
        
        return final_state.get("judgment", {})
    
    def update_event_judgment(self, event_id: str, judgment: Dict):
        """
        Write judgment back to Neo4j Event node.
        
        Args:
            event_id: Event node ID
            judgment: Judgment dict with score, status, reasoning
        """
        query = """
        MATCH (e:Event {id: $event_id})
        SET e.ai_score = $score,
            e.ai_status = $status,
            e.ai_reasoning = $reasoning,
            e.ai_scored_at = $scored_at
        """
        
        self.run_query(query, {
            "event_id": event_id,
            "score": judgment.get("score", 0),
            "status": judgment.get("status", "Unverified"),
            "reasoning": judgment.get("reasoning", ""),
            "scored_at": datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
        })


# ==============================================================================
# MAIN ENTRY POINT
# ==============================================================================

def main():
    """
    Main entry point for Agentic Scoring.
    
    Orchestrates the multi-agent debate pipeline:
    1. Get events with heuristic score > 0
    2. For each event, run Pro/Con/Judge debate
    3. Write AI judgment back to Neo4j
    4. Print summary table
    """
    print("=" * 80)
    print("🤖 AGENTIC SCORING: Multi-Agent Debate System")
    print("   Project Sentinel - LLM-Powered Intelligence Verification")
    print("=" * 80)
    
    # Check for API key
    if ANTHROPIC_API_KEY:
        print(f"\n🔑 Anthropic API: Configured")
        print(f"   Model: {LLM_MODEL}")
    else:
        print(f"\n⚠️  Anthropic API: NOT CONFIGURED (using mock mode)")
        print(f"   Set ANTHROPIC_API_KEY in .env for real LLM analysis")
    
    print(f"\n🔌 Connecting to Neo4j...")
    print(f"   URI: {NEO4J_URI}")
    
    scorer = AgenticScorer(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)
    
    try:
        # Get events for debate
        print(f"\n📊 Fetching events with heuristic score > {MIN_HEURISTIC_SCORE}...")
        events = scorer.get_events_for_debate()
        
        if not events:
            print("   ⚠️  No events found for analysis")
            print("   Run the heuristic scoring first: python analysis/correlate.py")
            return
        
        print(f"   ✓ Found {len(events)} events for AI analysis")
        
        # Process each event
        print(f"\n🎭 Running Multi-Agent Debates...")
        results = []
        
        for i, event in enumerate(events):
            event_id = event.get("event_id", "unknown")
            short_id = event_id[:16] + "..." if len(event_id) > 16 else event_id
            
            print(f"   [{i+1}/{len(events)}] Debating {short_id}...")
            
            # Run debate
            judgment = scorer.score_event(event)
            
            # Write back to Neo4j
            scorer.update_event_judgment(event_id, judgment)
            
            # Collect result
            results.append({
                "id": short_id,
                "location": event.get("location", "Unknown"),
                "heuristic": event.get("heuristic_score", 0),
                "ai_score": judgment.get("score", 0),
                "status": judgment.get("status", "Unknown"),
                "reasoning": judgment.get("reasoning", "")[:50] + "..."
            })
        
        # Print summary table
        print("\n" + "=" * 80)
        print("📊 AI SCORING RESULTS")
        print("=" * 80)
        print(f"{'Event ID':<20} {'Location':<12} {'Heuristic':>9} {'AI Score':>8} {'Status':<12}")
        print("-" * 80)
        
        for r in results:
            status_icon = "✅" if r['status'] == "Confirmed" else "⚠️ " if r['status'] == "Plausible" else "❓"
            print(f"{r['id']:<20} {r['location']:<12} {r['heuristic']:>9} {r['ai_score']:>8} {status_icon} {r['status']:<10}")
        
        # Summary stats
        print("\n" + "-" * 80)
        confirmed = sum(1 for r in results if r['status'] == "Confirmed")
        plausible = sum(1 for r in results if r['status'] == "Plausible")
        unverified = sum(1 for r in results if r['status'] == "Unverified")
        
        print(f"📈 AI ANALYSIS SUMMARY")
        print(f"   Confirmed:   {confirmed:>5} events")
        print(f"   Plausible:   {plausible:>5} events")
        print(f"   Unverified:  {unverified:>5} events")
        print(f"   Total:       {len(results):>5} events analyzed")
        print("=" * 80)
        
    finally:
        scorer.close()


if __name__ == "__main__":
    main()
