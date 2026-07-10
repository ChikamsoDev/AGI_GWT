# AI Cognitive Architecture

A personal project where I'm building an AI cognitive architecture from scratch in Python while learning AI engineering.

The goal is to build individual cognitive components inspired by neuroscience and cognitive science, then integrate them into a complete intelligent system.

Current progress:

- ✅ Global Workspace Theory (GWT)
- ⏳ Memory System
- ⏳ Attention
- ⏳ Reasoning
- ⏳ Planning
- ⏳ Learning

---

## Module 1: Global Workspace Theory

The Global Workspace acts as the central communication hub as well as the entry point into this project due to the simple fact that building the AI to work in isolation with no clear communication mechanism would result in failure.

Modules compete for attention by submitting messages with priorities.

The highest-priority message is broadcast to every subscribed module.

Current features include:

- Priority-based competition
- Thread-safe message queue
- Priority decay over time
- Message expiration
- Subscriber system
- Continuous broadcast loop
- Configurable runtime settings

---

## Technologies

- Python
- heapq
- threading
- dataclasses
- logging
- typing

---

## Future Goals

- Episodic memory
- Semantic memory
- Executive control
- Attention system
- Long-term planning
- Learning algorithms

---

## Folder structure
ai-base/
│
├── GWT/
│   ├── base_module.py
│   ├── global_workspace.py
│   ├── messages.py
│   └── __init__.py