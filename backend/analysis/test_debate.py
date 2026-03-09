#!/usr/bin/env python3
"""
==============================================================================
NEWSROOM DEBATE - TEST & VISUALIZATION
==============================================================================

Test harness for the Newsroom Debate system with:
1. Mock LLM responses (no API needed)
2. Rich terminal visualization
3. HTML report output

Run: python analysis/test_debate.py

==============================================================================
"""

import json
from pathlib import Path
from datetime import datetime
from typing import Dict, List

# ==============================================================================
# MOCK DATA - Simulates LLM responses
# ==============================================================================

MOCK_ARTICLES = [
    {
        "id": "test_debate_1",
        "title": "Iran warns of regional war if US attacks",
        "source_id": "telegram_Middle_East_Spectator",
        "source_tier": 3,
        "source_type": "Social Media",
        "content_type": "telegram",
        "summary": "Iranian officials have warned that any US military action would trigger a devastating regional conflict.",
        "final_score": 55,
        "status": "Developing",
        "cross_ref_count": 2,
        "scoring_notes": ["Base: 30 (Tier 3)", "Cross-ref: +25 (3+ sources)"],
        "keywords": ["iran", "israel"]
    },
    {
        "id": "test_debate_2",
        "title": "Gaza's Rafah border crossing reopens for limited evacuations",
        "source_id": "aljazeera_english",
        "source_tier": 2,
        "source_type": "International",
        "content_type": "article",
        "summary": "Egypt and Israel have agreed to reopen the Rafah crossing for medical evacuations, though full humanitarian access remains restricted.",
        "final_score": 65,
        "status": "Developing",
        "cross_ref_count": 3,
        "scoring_notes": ["Base: 50 (Tier 2)", "Cross-ref: +15 (2 sources)"],
        "keywords": ["gaza", "rafah", "egypt"]
    },
    {
        "id": "test_compare_1",
        "title": "UN Security Council meets on Iran nuclear program",
        "source_id": "bbc_world",
        "source_tier": 1,
        "source_type": "Wire Service",
        "content_type": "article",
        "summary": "The UN Security Council convened an emergency session to discuss new evidence of Iran's uranium enrichment activities.",
        "final_score": 85,
        "status": "Verified",
        "cross_ref_count": 5,
        "scoring_notes": ["Base: 70 (Tier 1)", "Cross-ref: +15 (2 sources)"],
        "keywords": ["iran", "nuclear", "un"]
    }
]

MOCK_RESPONSES = {
    "reporter": {
        "key_claims": [
            "Iranian officials issued direct warning about regional war",
            "Warning was specifically in response to potential US military action",
            "Threat implies coordinated response from regional allies"
        ],
        "specifics": ["Ali Shamkhani (senior advisor)", "mentioned Tel Aviv as potential target"],
        "story_type": "official_statement"
    },
    "fact_checker": {
        "verified_claims": [
            "Iran has made similar warnings before (consistent messaging)",
            "Ali Shamkhani is a real official in Iran's leadership"
        ],
        "unverified_claims": [
            "Specific military preparations",
            "Coordination with regional allies"
        ],
        "source_notes": "Telegram channel may prioritize sensational framing. Original source is Al-Mayadeen.",
        "gaps": ["No confirmation from Western sources yet", "Missing official US response"]
    },
    "editor": {
        "recommendation": "caution",
        "summary": "This report accurately conveys Iranian official statements but should be viewed with caution. The core claim (Iran warning of regional war) is consistent with known Iranian messaging, but the specific threats and implied coordination remain unverified by independent sources.",
        "confidence": "medium"
    },
    "comparator": {
        "core_agreement": "UN Security Council held emergency session on Iran's nuclear program",
        "potential_differences": [
            "Western sources emphasize Iranian non-compliance",
            "Regional sources may focus on diplomatic solutions",
            "Russian/Chinese sources likely highlight Western aggression"
        ],
        "source_perspective": "BBC is generally balanced but has a Western editorial lens",
        "reader_note": "Core facts are solid. Watch for diverging interpretations on who is at fault."
    }
}


# ==============================================================================
# VISUALIZATION - Terminal
# ==============================================================================

def print_header(title: str):
    """Print a section header."""
    print("\n" + "=" * 80)
    print(f"  {title}")
    print("=" * 80)


def print_article_card(article: Dict):
    """Print a nicely formatted article card."""
    status_icons = {
        "Verified": "🟢",
        "Likely Verified": "🟢",
        "Developing": "🟡",
        "Unverified": "🔴",
        "Unconfirmed": "🔴"
    }
    icon = status_icons.get(article["status"], "❓")
    type_icon = "📱" if article["content_type"] == "telegram" else "📰"
    
    print(f"\n┌{'─' * 78}┐")
    print(f"│ {type_icon} {article['title'][:72]:<74} │")
    print(f"├{'─' * 78}┤")
    print(f"│ Source: {article['source_id']:<30} Tier: {article['source_tier']:<20} │")
    print(f"│ Score: {article['final_score']:<5} {icon} {article['status']:<20} Cross-refs: {article['cross_ref_count']:<10} │")
    print(f"└{'─' * 78}┘")


def print_debate_result(debate: Dict):
    """Print the debate result in a visual format."""
    
    # Reporter section
    print("\n┌─────────────────────────────────────────────────────────────────────────────┐")
    print("│ 📝 REPORTER                                                                 │")
    print("├─────────────────────────────────────────────────────────────────────────────┤")
    reporter = debate.get("reporter", {})
    for claim in reporter.get("key_claims", []):
        print(f"│   • {claim[:71]:<71} │")
    if reporter.get("specifics"):
        print(f"│   📌 Specifics: {', '.join(reporter.get('specifics', []))[:58]:<58} │")
    print("└─────────────────────────────────────────────────────────────────────────────┘")
    
    # Fact-checker section
    print("\n┌─────────────────────────────────────────────────────────────────────────────┐")
    print("│ ✓ FACT-CHECKER                                                              │")
    print("├─────────────────────────────────────────────────────────────────────────────┤")
    fc = debate.get("fact_checker", {})
    print("│  ✅ Verified:                                                               │")
    for claim in fc.get("verified_claims", []):
        print(f"│      {claim[:70]:<70} │")
    print("│  ❓ Unverified:                                                             │")
    for claim in fc.get("unverified_claims", []):
        print(f"│      {claim[:70]:<70} │")
    if fc.get("source_notes"):
        print(f"│  ⚠️  Note: {fc['source_notes'][:64]:<64} │")
    print("└─────────────────────────────────────────────────────────────────────────────┘")
    
    # Editor section
    print("\n┌─────────────────────────────────────────────────────────────────────────────┐")
    print("│ 📋 EDITOR'S VERDICT                                                         │")
    print("├─────────────────────────────────────────────────────────────────────────────┤")
    editor = debate.get("editor", {})
    rec = editor.get("recommendation", "unknown").upper()
    rec_icon = {"TRUST": "✅", "CAUTION": "⚠️", "WAIT": "⏳"}.get(rec, "❓")
    print(f"│  {rec_icon} Recommendation: {rec:<58} │")
    
    # Wrap summary text
    summary = editor.get("summary", "No summary")
    words = summary.split()
    lines = []
    current_line = ""
    for word in words:
        if len(current_line) + len(word) + 1 <= 70:
            current_line += (" " if current_line else "") + word
        else:
            lines.append(current_line)
            current_line = word
    if current_line:
        lines.append(current_line)
    
    for line in lines:
        print(f"│  {line:<73} │")
    print("└─────────────────────────────────────────────────────────────────────────────┘")


def print_comparison_result(comparison: Dict):
    """Print the comparison result in a visual format."""
    comp = comparison.get("comparison", comparison)
    
    print("\n┌─────────────────────────────────────────────────────────────────────────────┐")
    print("│ 🔄 SOURCE COMPARISON                                                        │")
    print("├─────────────────────────────────────────────────────────────────────────────┤")
    
    core = comp.get("core_agreement", "N/A")
    print(f"│  ✅ All sources agree: {core[:52]:<52} │")
    
    print("│                                                                             │")
    print("│  📊 Potential differences:                                                  │")
    for diff in comp.get("potential_differences", []):
        print(f"│      • {diff[:66]:<66} │")
    
    if comp.get("source_perspective"):
        print(f"│                                                                             │")
        print(f"│  👁️ Source perspective: {comp['source_perspective'][:50]:<50} │")
    
    print("├─────────────────────────────────────────────────────────────────────────────┤")
    note = comp.get("reader_note", "")
    print(f"│  💡 {note[:70]:<70} │")
    print("└─────────────────────────────────────────────────────────────────────────────┘")


# ==============================================================================
# HTML REPORT GENERATOR
# ==============================================================================

def generate_html_report(articles: List[Dict], debates: List[Dict]) -> str:
    """Generate an HTML report for the debate results."""
    
    html = """<!DOCTYPE html>
<html>
<head>
    <title>Newsroom Debate - Test Results</title>
    <style>
        * { box-sizing: border-box; margin: 0; padding: 0; }
        body { 
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: #0f0f0f; 
            color: #e0e0e0; 
            padding: 40px;
            line-height: 1.6;
        }
        .container { max-width: 900px; margin: 0 auto; }
        h1 { 
            font-size: 2rem; 
            margin-bottom: 1rem;
            background: linear-gradient(90deg, #00d4ff, #7c3aed);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
        }
        .subtitle { color: #888; margin-bottom: 2rem; }
        
        .article-card {
            background: #1a1a1a;
            border-radius: 12px;
            padding: 24px;
            margin-bottom: 24px;
            border: 1px solid #333;
        }
        .article-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 16px;
        }
        .article-title { font-size: 1.2rem; font-weight: 600; }
        .badge {
            padding: 4px 12px;
            border-radius: 20px;
            font-size: 0.8rem;
            font-weight: 600;
        }
        .badge-debate { background: #7c3aed; color: white; }
        .badge-compare { background: #00d4ff; color: black; }
        .meta { color: #888; font-size: 0.9rem; margin-bottom: 16px; }
        
        .agent-section {
            background: #252525;
            border-radius: 8px;
            padding: 16px;
            margin-top: 16px;
        }
        .agent-header {
            display: flex;
            align-items: center;
            gap: 8px;
            font-weight: 600;
            margin-bottom: 12px;
            color: #00d4ff;
        }
        .claims-list { margin-left: 20px; }
        .claims-list li { margin: 4px 0; }
        .verified { color: #22c55e; }
        .unverified { color: #f59e0b; }
        
        .verdict {
            background: linear-gradient(135deg, #1a1a2e, #16213e);
            border-left: 4px solid #7c3aed;
            padding: 16px;
            margin-top: 16px;
            border-radius: 0 8px 8px 0;
        }
        .rec-trust { border-left-color: #22c55e; }
        .rec-caution { border-left-color: #f59e0b; }
        .rec-wait { border-left-color: #3b82f6; }
        .recommendation {
            font-size: 1.1rem;
            font-weight: 600;
            margin-bottom: 8px;
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>🗞️ Newsroom Debate System</h1>
        <p class="subtitle">Test Results · Generated """ + datetime.now().strftime("%Y-%m-%d %H:%M") + """</p>
"""
    
    for article, debate in zip(articles, debates):
        analysis_type = debate.get("analysis_type", "unknown")
        badge_class = "badge-debate" if analysis_type == "newsroom_debate" else "badge-compare"
        badge_text = "Full Debate" if analysis_type == "newsroom_debate" else "Source Comparison"
        
        html += f"""
        <div class="article-card">
            <div class="article-header">
                <div class="article-title">{'📱' if article['content_type'] == 'telegram' else '📰'} {article['title']}</div>
                <span class="badge {badge_class}">{badge_text}</span>
            </div>
            <div class="meta">
                Source: {article['source_id']} · Score: {article['final_score']} · Status: {article['status']}
            </div>
"""
        
        if analysis_type == "newsroom_debate":
            reporter = debate.get("reporter", {})
            fc = debate.get("fact_checker", {})
            editor = debate.get("editor", {})
            
            html += f"""
            <div class="agent-section">
                <div class="agent-header">📝 Reporter</div>
                <ul class="claims-list">
                    {"".join(f"<li>{c}</li>" for c in reporter.get("key_claims", []))}
                </ul>
            </div>
            
            <div class="agent-section">
                <div class="agent-header">✓ Fact-Checker</div>
                <div class="verified"><strong>Verified:</strong></div>
                <ul class="claims-list">
                    {"".join(f"<li>{c}</li>" for c in fc.get("verified_claims", []))}
                </ul>
                <div class="unverified"><strong>Unverified:</strong></div>
                <ul class="claims-list">
                    {"".join(f"<li>{c}</li>" for c in fc.get("unverified_claims", []))}
                </ul>
            </div>
            
            <div class="verdict rec-{editor.get('recommendation', 'caution')}">
                <div class="recommendation">
                    {"✅" if editor.get('recommendation') == 'trust' else "⚠️" if editor.get('recommendation') == 'caution' else "⏳"} 
                    {editor.get('recommendation', 'Unknown').upper()}
                </div>
                <p>{editor.get('summary', 'No summary')}</p>
            </div>
"""
        else:
            comp = debate.get("comparison", debate)
            html += f"""
            <div class="agent-section">
                <div class="agent-header">🔄 Source Comparison</div>
                <p><strong>All sources agree:</strong> {comp.get('core_agreement', 'N/A')}</p>
                <br>
                <p><strong>Potential differences:</strong></p>
                <ul class="claims-list">
                    {"".join(f"<li>{d}</li>" for d in comp.get("potential_differences", []))}
                </ul>
            </div>
            
            <div class="verdict rec-trust">
                <div class="recommendation">💡 Reader Note</div>
                <p>{comp.get('reader_note', 'No note')}</p>
            </div>
"""
        
        html += """
        </div>
"""
    
    html += """
    </div>
</body>
</html>
"""
    return html


# ==============================================================================
# MAIN TEST RUNNER
# ==============================================================================

def run_mock_test():
    """Run tests with mock data."""
    
    print_header("NEWSROOM DEBATE SYSTEM - TEST MODE")
    print("\n📋 Using mock LLM responses (no API calls)")
    print("   This simulates what the real output would look like\n")
    
    debates = []
    
    for article in MOCK_ARTICLES:
        print_article_card(article)
        
        score = article["final_score"]
        
        if score < 46:
            print("   ⏭️  Score too low - no analysis")
            debates.append({"analysis_type": "none"})
            
        elif score <= 74:
            # Newsroom Debate
            print("   🎭 Running Newsroom Debate (mock)...")
            debate = {
                "analysis_type": "newsroom_debate",
                "reporter": MOCK_RESPONSES["reporter"],
                "fact_checker": MOCK_RESPONSES["fact_checker"],
                "editor": MOCK_RESPONSES["editor"]
            }
            print_debate_result(debate)
            debates.append(debate)
            
        else:
            # Source Comparison
            print("   🔄 Running Source Comparison (mock)...")
            comparison = {
                "analysis_type": "source_comparison",
                "comparison": MOCK_RESPONSES["comparator"]
            }
            print_comparison_result(comparison)
            debates.append(comparison)
    
    # Generate HTML report
    print_header("GENERATING HTML REPORT")
    html = generate_html_report(MOCK_ARTICLES, debates)
    
    output_path = Path(__file__).parent.parent.parent / "data" / "debate_test_report.html"
    output_path.parent.mkdir(exist_ok=True)
    output_path.write_text(html)
    
    print(f"\n✅ HTML report saved to: {output_path}")
    print(f"   Open in browser: file://{output_path.absolute()}")
    
    print_header("TEST COMPLETE")
    print("\nSummary:")
    print(f"   • Articles tested: {len(MOCK_ARTICLES)}")
    print(f"   • Newsroom Debates: {sum(1 for d in debates if d.get('analysis_type') == 'newsroom_debate')}")
    print(f"   • Source Comparisons: {sum(1 for d in debates if d.get('analysis_type') == 'source_comparison')}")


if __name__ == "__main__":
    run_mock_test()
