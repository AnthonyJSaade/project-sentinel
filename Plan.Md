📂 PROJECT SENTINEL: Master Design Document
1. The Core Mission

To build a Hybrid Conflict Monitor that visualizes the correlation between Kinetic Events (explosions, troop movements) and Information Reliability (source consensus, denials).

2. The Tech Stack

Frontend: Next.js (React), TypeScript, Tailwind CSS, Tremor (Charts), Mapbox GL JS (Map).

Backend: Python (FastAPI).

Database: Neo4j (Graph Database - Critical for the GNN/Relationship logic).

AI/ML: PyTorch Geometric (GNNs), LangChain/LangGraph (Agents).

Infrastructure: Docker (Containerized services).

3. The Architecture Modules

Module A: The "Ears" (Ingestion Agents)

Agent_Kinetic: Polls GDELT/ACLED for physical events.

Agent_Skywatcher: Polls ADS-B Exchange for military aircraft patterns.

Agent_News: Scrapes RSS/Telegram for narrative claims.

Module B: The "Brain" (Verification Engine)

Consensus Algorithm: Calculates "Corroboration Score" based on Source Tiers.

Escalation Predictor: Correlates Flight Data + Rhetoric Spikes.

Module C: The "Face" (UI)

DeepState-style Map: Interactive, dark mode, performance-optimized for thousands of points.

Sidebar & Evidence Graph: Drill-down views for source verification.
