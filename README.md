# AI Cognitive Architecture

A personal project where I'm building an AI cognitive architecture from scratch in Python while learning AI engineering.

The goal is to build individual cognitive components inspired by neuroscience and cognitive science, then integrate them into a complete intelligent system.

## Current Progress

- ✅ Global Workspace Theory (GWT)
- ✅ Memory System
- ⏳ Attention
- ⏳ Reasoning
- ⏳ Planning
- ⏳ Learning
- ⏳ Perception
- ⏳ World Model
- ⏳ Social Cognition

---

# Module 1: Global Workspace Theory

The Global Workspace serves as the communication backbone of the architecture. Rather than allowing modules to operate independently, it provides a shared workspace where information competes for attention before being broadcast to every subscribed module.

### Features

- Priority-based competition
- Thread-safe message queue
- Priority decay
- Message expiration
- Subscriber system
- Continuous broadcast loop
- Configurable runtime settings

---

# Module 2: Memory

The Memory module stores high-value experiences as **episodes** instead of saving every incoming message. Incoming broadcasts are evaluated using salience and grouped into meaningful experiences through event boundary detection.

### Features

- Salience-based memory storage
- Event boundary detection
- Novelty detection
- Episodic memory representation
- Memory retrieval
- Memory consolidation stages
- Retrieval statistics

---

## Technologies

- Python
- dataclasses
- threading
- heapq
- logging
- typing
- uuid
- datetime

---

## Planned Modules

- Perception
- World Model
- Attention
- Learning
- Reasoning
- Planning
- Decision Making
- Social Cognition
- Language
- Metacognition

---

## Folder Structure

```text
ai-base/
│
├── GWT/
│   ├── __init__.py
│   ├── base_module.py
│   ├── global_workspace.py
│   └── messages.py
│
├── memory/
│   ├── __init__.py
│   └── memory.py
│   
│
└── README.md
```

## Project Vision

This repository documents the incremental development of an AGI-inspired cognitive architecture. Rather than relying on a single monolithic model, each cognitive capability is implemented as an independent module before being integrated into a larger intelligent system.