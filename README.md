# MAX Intelligence

**An independent analytics platform for Portland's TriMet MAX light rail system.**

## Overview

MAX Intelligence aims to provide better visibility into MAX operations through live vehicle tracking, service monitoring, and transit performance analytics.

The project uses TriMet's public transit data to build an independent platform for exploring and understanding MAX operations.

## Planned Features

- Live MAX train tracking on an interactive map.
- Service alerts and disruption monitoring.
- Vehicle arrival predictions.
- Historical transit data collection.
- Schedule adherence analysis.
- Headway consistency analysis.
- Transit reliability and performance analytics.

## Technology Stack

| Component | Technology |
|---|---|
| Backend | Python, FastAPI |
| Frontend | React, TypeScript, Vite |
| Mapping | Leaflet |
| State Management | Zustand |
| HTTP Client | HTTPX |
| Data Sources | TriMet APIs, GTFS, GTFS-Realtime |
| Testing | Pytest |
| Deployment | Docker Compose |

## Development Status

**Current Phase:** Phase 1

**Current Milestone:** M0 - Feed Audit & Access

The immediate objective is to verify TriMet's data feeds before beginning application development.

## Project Documentation

- [Architecture](docs/ARCHITECTURE.md)
- [Development Roadmap](docs/ROADMAP.md)
- [TriMet API Audit Notes](docs/trimet-api-notes.md)
- [AI Agent Instructions](AGENTS.md)

## Data Sources

MAX Intelligence uses publicly available TriMet transit data, including:

- TriMet Vehicle Location Service.
- TriMet Service Alerts.
- TriMet GTFS-Realtime feeds.
- TriMet static GTFS datasets.

All data usage must comply with TriMet's applicable terms and licensing requirements.

## Project Goals

The primary goal of Phase 1 is to deliver a functional live MAX tracking map. More advanced historical and analytical capabilities will follow in later development phases.