# Python nREPL Client

A Python client for interacting with an **nREPL-compatible server** over TCP using **bencode** encoding.

This client is designed for synchronous request / response workflows while handling asynchronous message delivery internally via a background reader thread.

The implementation is suitable for editor integrations, tooling, automation, and programmatic interaction with Clojure nREPL servers or other nREPL-compatible endpoints.

---

## Features

- TCP connection management with timeout
- Fully asynchronous socket reader thread
- Bencode message decoding and encoding
- Request / response correlation via request IDs
- Automatic session tracking
- Blocking convenience methods for common nREPL operations
- Robust handling of partial socket reads and streamed responses

---

## Requirements

- Python 3.9 or later
- `bencode2` library

Install dependencies:

```sh
pip install bencode2
``
