"""
Lightweight Nexus Client for GozerAI Services.

Drop-in client for any GozerAI product to access Nexus shared services.
Zero dependencies beyond stdlib + httpx (which all products already have).

Usage:
    from nexus.client import NexusClient

    # Or copy this file into your project — it's self-contained
    nexus = NexusClient()  # reads NEXUS_BASE_URL from env

    # LLM gateway (multi-provider ensemble, cost-tracked)
    response = await nexus.generate("Analyze this market data...")

    # Knowledge base
    results = await nexus.search_knowledge("customer churn patterns")
    await nexus.add_knowledge("Churn rate dropped 15% after pricing change", source="shopforge")

    # Discovery
    models = await nexus.list_models()
    papers = await nexus.search_arxiv("recommendation systems")

    # Sync version for non-async code
    response = nexus.generate_sync("Analyze this data...")
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from typing import Any, Dict, List, Optional
from urllib.error import URLError
from urllib.request import Request, urlopen

logger = logging.getLogger(__name__)

_DEFAULT_URL = "http://localhost:8008"
_TIMEOUT = 15


class NexusClient:
    """Lightweight client for Nexus shared services.

    Works with both async (httpx) and sync (urllib) code.
    Reads NEXUS_BASE_URL from environment by default.
    """

    def __init__(self, base_url: Optional[str] = None, timeout: int = _TIMEOUT):
        self.base_url = (base_url or os.environ.get("NEXUS_BASE_URL", _DEFAULT_URL)).rstrip("/")
        self.timeout = timeout
        self._failures = 0
        self._max_failures = 5

    @property
    def is_available(self) -> bool:
        """Check if Nexus appears to be reachable (not circuit-broken)."""
        return bool(self.base_url) and self._failures < self._max_failures

    def _reset_failures(self) -> None:
        self._failures = 0

    def _record_failure(self) -> None:
        self._failures += 1
        if self._failures >= self._max_failures:
            logger.warning("Nexus client circuit breaker tripped after %d failures", self._failures)

    # ------------------------------------------------------------------
    # Async API (for FastAPI / async services)
    # ------------------------------------------------------------------

    async def generate(
        self,
        prompt: str,
        *,
        system_prompt: Optional[str] = None,
        model: Optional[str] = None,
        max_tokens: int = 4096,
        source: str = "unknown",
    ) -> Optional[Dict[str, Any]]:
        """Generate text via Nexus LLM gateway.

        Returns dict with: content, model, provider, tokens_used, latency_ms
        Returns None if Nexus is unavailable.
        """
        if not self.is_available:
            return None
        try:
            import httpx
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                resp = await client.post(
                    f"{self.base_url}/api/generate",
                    json={
                        "prompt": prompt,
                        "system_prompt": system_prompt,
                        "model": model,
                        "max_tokens": max_tokens,
                        "source": source,
                    },
                )
                resp.raise_for_status()
                self._reset_failures()
                return resp.json()
        except Exception as e:
            self._record_failure()
            logger.debug("Nexus generate failed: %s", e)
            return None

    async def search_knowledge(self, query: str, limit: int = 10) -> List[Dict[str, Any]]:
        """Search the shared knowledge base."""
        if not self.is_available:
            return []
        try:
            import httpx
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                resp = await client.get(
                    f"{self.base_url}/api/knowledge/search",
                    params={"q": query[:200], "limit": limit},
                )
                resp.raise_for_status()
                self._reset_failures()
                return resp.json().get("items", [])
        except Exception as e:
            self._record_failure()
            logger.debug("Nexus knowledge search failed: %s", e)
            return []

    async def add_knowledge(
        self,
        content: str,
        source: str,
        knowledge_type: str = "factual",
        confidence: float = 0.8,
        tags: Optional[List[str]] = None,
    ) -> Optional[str]:
        """Add knowledge to the shared knowledge base. Returns ID or None."""
        if not self.is_available:
            return None
        try:
            import httpx
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                resp = await client.post(
                    f"{self.base_url}/api/knowledge",
                    json={
                        "content": content,
                        "source": source,
                        "knowledge_type": knowledge_type,
                        "confidence": confidence,
                        "context_tags": tags or [],
                    },
                )
                resp.raise_for_status()
                self._reset_failures()
                return resp.json().get("id")
        except Exception as e:
            self._record_failure()
            logger.debug("Nexus add knowledge failed: %s", e)
            return None

    async def list_models(self) -> List[Dict[str, Any]]:
        """List available LLM models."""
        if not self.is_available:
            return []
        try:
            import httpx
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                resp = await client.get(f"{self.base_url}/api/models")
                resp.raise_for_status()
                self._reset_failures()
                return resp.json().get("models", [])
        except Exception as e:
            self._record_failure()
            logger.debug("Nexus list models failed: %s", e)
            return []

    async def health(self) -> Optional[Dict[str, Any]]:
        """Check Nexus health."""
        try:
            import httpx
            async with httpx.AsyncClient(timeout=5) as client:
                resp = await client.get(f"{self.base_url}/health")
                resp.raise_for_status()
                self._reset_failures()
                return resp.json()
        except Exception:
            self._record_failure()
            return None

    # ------------------------------------------------------------------
    # Sync API (for non-async services like Knowledge Harvester)
    # ------------------------------------------------------------------

    def generate_sync(
        self,
        prompt: str,
        *,
        system_prompt: Optional[str] = None,
        max_tokens: int = 4096,
        source: str = "unknown",
    ) -> Optional[str]:
        """Synchronous LLM generation. Returns content string or None."""
        if not self.is_available:
            return None
        try:
            body = json.dumps({
                "prompt": prompt,
                "system_prompt": system_prompt,
                "max_tokens": max_tokens,
                "source": source,
            }).encode("utf-8")
            req = Request(
                f"{self.base_url}/api/generate",
                data=body,
                headers={"Content-Type": "application/json"},
            )
            with urlopen(req, timeout=self.timeout) as resp:
                data = json.loads(resp.read().decode("utf-8"))
                self._reset_failures()
                return data.get("content")
        except Exception as e:
            self._record_failure()
            logger.debug("Nexus sync generate failed: %s", e)
            return None

    def search_knowledge_sync(self, query: str, limit: int = 10) -> List[Dict[str, Any]]:
        """Synchronous knowledge search."""
        if not self.is_available:
            return []
        try:
            from urllib.parse import quote
            url = f"{self.base_url}/api/knowledge/search?q={quote(query[:200])}&limit={limit}"
            req = Request(url, headers={"Accept": "application/json"})
            with urlopen(req, timeout=self.timeout) as resp:
                data = json.loads(resp.read().decode("utf-8"))
                self._reset_failures()
                return data.get("items", [])
        except Exception as e:
            self._record_failure()
            logger.debug("Nexus sync knowledge search failed: %s", e)
            return []

    def add_knowledge_sync(
        self,
        content: str,
        source: str,
        knowledge_type: str = "factual",
        confidence: float = 0.8,
    ) -> Optional[str]:
        """Synchronous knowledge addition. Returns ID or None."""
        if not self.is_available:
            return None
        try:
            body = json.dumps({
                "content": content,
                "source": source,
                "knowledge_type": knowledge_type,
                "confidence": confidence,
            }).encode("utf-8")
            req = Request(
                f"{self.base_url}/api/knowledge",
                data=body,
                headers={"Content-Type": "application/json"},
            )
            with urlopen(req, timeout=self.timeout) as resp:
                data = json.loads(resp.read().decode("utf-8"))
                self._reset_failures()
                return data.get("id")
        except Exception as e:
            self._record_failure()
            logger.debug("Nexus sync add knowledge failed: %s", e)
            return None

    def health_sync(self) -> bool:
        """Synchronous health check. Returns True if Nexus is up."""
        try:
            req = Request(f"{self.base_url}/health")
            with urlopen(req, timeout=5) as resp:
                self._reset_failures()
                return resp.status == 200
        except Exception:
            self._record_failure()
            return False
