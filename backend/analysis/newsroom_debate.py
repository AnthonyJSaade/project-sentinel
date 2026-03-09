#!/usr/bin/env python3
"""
==============================================================================
NEWSROOM DEBATE SYSTEM - Project Sentinel
==============================================================================

Multi-agent AI system for analyzing news credibility.

Architecture:
    Score < 46:   No analysis (too weak)
    Score 46-74:  Full Newsroom Debate (Reporter → Fact-Checker → Editor)
    Score 75+:    Source Comparison (highlights differences)

Agents:
    - Reporter:     Summarizes the story objectively
    - Fact-Checker: Identifies verified vs unverified claims
    - Editor:       Synthesizes into credibility assessment
    - Comparator:   Highlights differences between sources

==============================================================================
"""

import json
import os
from typing import Dict, List, Optional, TypedDict
from datetime import datetime

from dotenv import load_dotenv

load_dotenv()

# ==============================================================================
# CONFIGURATION
# ==============================================================================

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")

# Thresholds for analysis type
THRESHOLD_MIN_DEBATE = 46      # Minimum score for any analysis
THRESHOLD_MAX_DEBATE = 74      # Maximum score for full debate
# Above 74 = Comparator only

# ==============================================================================
# PROMPTS
# ==============================================================================

REPORTER_PROMPT = """You are a news REPORTER. Your job is to summarize what happened objectively.

ARTICLE:
Title: {title}
Source: {source}
Summary: {summary}

INSTRUCTIONS:
1. Summarize the key claims in 2-3 bullet points
2. Be factual and neutral - no opinions
3. Note any specific numbers, names, or dates mentioned

Respond in this exact JSON format:
{{
    "key_claims": ["claim 1", "claim 2", "claim 3"],
    "specifics": ["any specific numbers, dates, names mentioned"],
    "story_type": "breaking_news|analysis|opinion|official_statement"
}}"""

FACT_CHECKER_PROMPT = """You are a FACT-CHECKER at a newsroom. Review the reporter's summary and assess verification.

REPORTER'S SUMMARY:
{reporter_output}

SCORING CONTEXT:
- Source: {source} (Tier {tier})
- Cross-referenced by {cross_ref_count} other sources
- Scoring notes: {scoring_notes}

INSTRUCTIONS:
1. For each claim, assess if it's verified, unverified, or disputed
2. Note any source biases or limitations
3. Identify what's missing or needs more verification

Respond in this exact JSON format:
{{
    "verified_claims": ["claims that are well-supported"],
    "unverified_claims": ["claims we can't confirm yet"],
    "source_notes": "any biases or limitations of the source",
    "gaps": ["what's missing from this report"]
}}"""

EDITOR_PROMPT = """You are the EDITOR making the final credibility call. Review the fact-check and give readers a clear assessment.

REPORTER'S SUMMARY:
{reporter_output}

FACT-CHECKER'S ANALYSIS:
{fact_checker_output}

ARTICLE SCORE: {score}/100 ({status})

INSTRUCTIONS:
Write a 2-3 sentence assessment that:
1. Tells readers whether to trust this
2. Highlights the key verified points
3. Notes any cautions

Respond in this exact JSON format:
{{
    "recommendation": "trust|caution|wait",
    "summary": "Your 2-3 sentence assessment for readers",
    "confidence": "high|medium|low"
}}"""

COMPARATOR_PROMPT = """You are analyzing how different sources report the same story.

ARTICLE:
Title: {title}
Source: {source}
Summary: {summary}

CONTEXT:
This story was reported by {cross_ref_count} other sources.
Related keywords: {keywords}

INSTRUCTIONS:
1. Note what all sources likely agree on (the core facts)
2. Identify potential differences in reporting angles
3. Note any source-specific biases to watch for

Respond in this exact JSON format:
{{
    "core_agreement": "What all sources likely agree on",
    "potential_differences": ["Aspects that may vary between sources"],
    "source_perspective": "Any known bias or angle of this particular source",
    "reader_note": "One sentence advice for the reader"
}}"""


# ==============================================================================
# LLM HELPERS
# ==============================================================================

def get_llm():
    """Get Claude LLM instance."""
    if not ANTHROPIC_API_KEY:
        return None
    
    try:
        from langchain_anthropic import ChatAnthropic
        return ChatAnthropic(
            model="claude-opus-4-5-20251101",
            api_key=ANTHROPIC_API_KEY,
            max_tokens=500,
            temperature=0.3
        )
    except ImportError:
        print("⚠️ langchain-anthropic not installed")
        return None


def extract_json(text: str) -> dict:
    """Extract JSON from LLM response, handling markdown code blocks."""
    import re
    
    # Try to find JSON in code blocks
    code_block_match = re.search(r'```(?:json)?\s*([\s\S]*?)\s*```', text)
    if code_block_match:
        text = code_block_match.group(1)
    
    # Try to parse
    try:
        return json.loads(text.strip())
    except json.JSONDecodeError:
        # Try to find JSON object in text
        json_match = re.search(r'\{[\s\S]*\}', text)
        if json_match:
            try:
                return json.loads(json_match.group())
            except:
                pass
    
    return {}


def call_agent(llm, prompt: str, agent_name: str) -> dict:
    """Call an agent and parse its JSON response."""
    from langchain_core.messages import HumanMessage
    
    try:
        response = llm.invoke([HumanMessage(content=prompt)])
        result = extract_json(response.content)
        return result
    except Exception as e:
        print(f"      ⚠️ {agent_name} failed: {e}")
        return {}


# ==============================================================================
# NEWSROOM DEBATE (Score 46-74)
# ==============================================================================

def run_newsroom_debate(article: Dict, llm) -> Dict:
    """
    Run full newsroom debate for gray-zone articles.
    
    Flow: Reporter → Fact-Checker → Editor
    
    Returns:
        Dict with reporter_output, fact_checker_output, editor_output, and final summary
    """
    result = {
        "analysis_type": "newsroom_debate",
        "reporter": None,
        "fact_checker": None,
        "editor": None,
        "summary": None
    }
    
    # Agent 1: Reporter
    reporter_prompt = REPORTER_PROMPT.format(
        title=article.get("title", "Unknown"),
        source=article.get("source_id", "Unknown"),
        summary=article.get("summary", "No summary")
    )
    reporter_output = call_agent(llm, reporter_prompt, "Reporter")
    result["reporter"] = reporter_output
    
    if not reporter_output:
        return {"analysis_type": "newsroom_debate", "error": "Reporter failed"}
    
    # Agent 2: Fact-Checker
    fact_checker_prompt = FACT_CHECKER_PROMPT.format(
        reporter_output=json.dumps(reporter_output, indent=2),
        source=article.get("source_id", "Unknown"),
        tier=article.get("source_tier", 3),
        cross_ref_count=article.get("cross_ref_count", 0),
        scoring_notes=article.get("scoring_notes", [])
    )
    fact_checker_output = call_agent(llm, fact_checker_prompt, "Fact-Checker")
    result["fact_checker"] = fact_checker_output
    
    if not fact_checker_output:
        return {"analysis_type": "newsroom_debate", "error": "Fact-Checker failed", "reporter": reporter_output}
    
    # Agent 3: Editor
    editor_prompt = EDITOR_PROMPT.format(
        reporter_output=json.dumps(reporter_output, indent=2),
        fact_checker_output=json.dumps(fact_checker_output, indent=2),
        score=article.get("final_score", 0),
        status=article.get("status", "Unknown")
    )
    editor_output = call_agent(llm, editor_prompt, "Editor")
    result["editor"] = editor_output
    
    # Build user-facing summary
    if editor_output:
        result["summary"] = editor_output.get("summary", "Analysis complete.")
        result["recommendation"] = editor_output.get("recommendation", "caution")
        result["confidence"] = editor_output.get("confidence", "medium")
    
    return result


# ==============================================================================
# SOURCE COMPARATOR (Score 75+)
# ==============================================================================

def run_source_comparison(article: Dict, llm) -> Dict:
    """
    Run source comparison for high-confidence articles.
    Highlights differences between how sources report the same story.
    
    Returns:
        Dict with comparison analysis
    """
    comparator_prompt = COMPARATOR_PROMPT.format(
        title=article.get("title", "Unknown"),
        source=article.get("source_id", "Unknown"),
        summary=article.get("summary", "No summary"),
        cross_ref_count=article.get("cross_ref_count", 0),
        keywords=article.get("keywords", [])
    )
    
    comparator_output = call_agent(llm, comparator_prompt, "Comparator")
    
    if comparator_output:
        return {
            "analysis_type": "source_comparison",
            "comparison": comparator_output,
            "summary": comparator_output.get("reader_note", "Multiple sources confirm this story."),
            "core_agreement": comparator_output.get("core_agreement"),
            "potential_differences": comparator_output.get("potential_differences", [])
        }
    
    return {
        "analysis_type": "source_comparison",
        "summary": "This story is confirmed by multiple sources.",
        "error": "Comparison analysis failed"
    }


# ==============================================================================
# MAIN ANALYSIS FUNCTION
# ==============================================================================

def analyze_article(article: Dict) -> Optional[Dict]:
    """
    Analyze an article using the appropriate method based on score.
    
    Args:
        article: Scored article dict
        
    Returns:
        Analysis result dict or None if no analysis needed
    """
    score = article.get("final_score", 0)
    
    # Score < 46: No analysis
    if score < THRESHOLD_MIN_DEBATE:
        return None
    
    # Check for LLM availability
    llm = get_llm()
    if not llm:
        return {"error": "LLM not available", "analysis_type": "none"}
    
    # Score 46-74: Full Newsroom Debate
    if score <= THRESHOLD_MAX_DEBATE:
        return run_newsroom_debate(article, llm)
    
    # Score 75+: Source Comparison
    else:
        return run_source_comparison(article, llm)


def analyze_articles_batch(articles: List[Dict], progress_callback=None) -> List[Dict]:
    """
    Analyze a batch of articles, adding AI analysis to each.
    
    Args:
        articles: List of scored article dicts
        progress_callback: Optional function to call with progress updates
        
    Returns:
        List of articles with added ai_analysis field
    """
    llm = get_llm()
    if not llm:
        print("⚠️ ANTHROPIC_API_KEY not set - skipping AI analysis")
        return articles
    
    # Count articles that need analysis
    debate_articles = [a for a in articles if THRESHOLD_MIN_DEBATE <= a.get("final_score", 0) <= THRESHOLD_MAX_DEBATE]
    compare_articles = [a for a in articles if a.get("final_score", 0) > THRESHOLD_MAX_DEBATE]
    
    total_to_analyze = len(debate_articles) + len(compare_articles)
    
    if total_to_analyze == 0:
        print("📊 No articles qualify for AI analysis")
        return articles
    
    print(f"\n🤖 AI Analysis:")
    print(f"   📰 Newsroom Debate (46-74): {len(debate_articles)} articles")
    print(f"   🔄 Source Comparison (75+): {len(compare_articles)} articles")
    print(f"   Total: {total_to_analyze} articles")
    
    analyzed = 0
    for i, article in enumerate(articles):
        score = article.get("final_score", 0)
        
        if score < THRESHOLD_MIN_DEBATE:
            articles[i]["ai_analysis"] = None
            articles[i]["ai_analyzed"] = False
            continue
        
        analyzed += 1
        title_short = article.get("title", "")[:40]
        analysis_type = "Debate" if score <= THRESHOLD_MAX_DEBATE else "Compare"
        print(f"   [{analyzed}/{total_to_analyze}] {analysis_type}: {title_short}...")
        
        analysis = analyze_article(article)
        articles[i]["ai_analysis"] = analysis
        articles[i]["ai_analyzed"] = bool(analysis and "error" not in analysis)
        
        # Extract summary for backwards compatibility
        if analysis and "summary" in analysis:
            articles[i]["ai_reasoning"] = analysis["summary"]
    
    success_count = sum(1 for a in articles if a.get("ai_analyzed"))
    print(f"   ✓ Successfully analyzed {success_count} articles")
    
    return articles


# ==============================================================================
# STANDALONE TEST
# ==============================================================================

if __name__ == "__main__":
    # Test with sample article
    test_article = {
        "id": "test_123",
        "title": "Iran warns of regional war if US attacks",
        "source_id": "aljazeera_english",
        "source_tier": 2,
        "summary": "Iranian officials have warned that any US military action would trigger a devastating regional conflict affecting multiple countries.",
        "final_score": 65,
        "status": "Developing",
        "cross_ref_count": 3,
        "scoring_notes": ["Base: 50 (Tier 2)", "Cross-ref: +15 (2 sources)"],
        "keywords": ["iran", "israel"]
    }
    
    print("=" * 60)
    print("🧪 Testing Newsroom Debate System")
    print("=" * 60)
    
    result = analyze_article(test_article)
    
    if result:
        print("\n📋 Analysis Result:")
        print(json.dumps(result, indent=2))
    else:
        print("\n❌ No analysis generated")
