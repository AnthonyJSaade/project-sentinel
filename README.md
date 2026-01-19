# Project Sentinel 🎯

A geopolitical conflict monitor focused on the Middle East, providing real-time OSINT data visualization and analysis.

## Overview

Project Sentinel is a hybrid conflict monitoring system that correlates kinetic events (explosions, troop movements) with information reliability metrics. This repository contains the codebase for the data ingestion agents and visualization frontend.

## Project Structure

```
├── backend/           # Python/FastAPI backend
│   ├── agents/        # Data ingestion agents
│   └── requirements.txt
├── frontend/          # Next.js frontend (coming soon)
└── data/              # Raw data storage
```

## Quick Start

### Backend Setup

```bash
cd backend
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### Run Data Ingestion

```bash
python agents/ingest_gdelt.py
```

## Agent_Kinetic

The first data ingestion agent queries GDELT 2.0 for material conflict events:

- **CAMEO Codes**: 190-195 (Military force, bombing, shelling)
- **Region**: Lebanon, Israel, Syria bounding box
- **Output**: Clean JSON with events containing timestamp, coordinates, source URLs, and actor codes

## Tech Stack

- **Backend**: Python, FastAPI
- **Frontend**: Next.js, React, Tailwind CSS, Mapbox GL JS
- **Database**: Neo4j (Graph Database)
- **AI/ML**: PyTorch Geometric, LangChain

## License

MIT
