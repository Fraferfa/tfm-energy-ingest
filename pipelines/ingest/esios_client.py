import os
import time
from typing import Any, Dict

import requests


class EsiosClient:
    def __init__(
        self,
        api_key: str | None = None,
        rate_limit_per_sec: float = 1.0,
        timeout_seconds: int = 30,
    ):
        self.api_key = (api_key or os.environ.get("ESIOS_TOKEN", "")).strip()
        if not self.api_key:
            raise RuntimeError("ESIOS_TOKEN no configurado")
        self.rate_interval = 1.0 / max(rate_limit_per_sec, 0.01)
        self.timeout = timeout_seconds
        self._last_call = 0.0

    def _throttle(self):
        elapsed = time.time() - self._last_call
        if elapsed < self.rate_interval:
            time.sleep(self.rate_interval - elapsed)

    def get_indicator(
        self,
        indicator_id: int,
        start_iso_utc: str,
        end_iso_utc: str,
        base_url: str,
        time_trunc: str | None = "hour",
    ) -> Dict[str, Any]:
        self._throttle()
        params = {
            "start_date": start_iso_utc,
            "end_date": end_iso_utc,
            "locale": "es",
        }
        if time_trunc:
            params["time_trunc"] = time_trunc
        url = f"{base_url}/{indicator_id}"

        # Modo de autenticación configurable: x-api-key | authorization | both (por defecto)
        preferred = (os.environ.get("ESIOS_AUTH_MODE", "both") or "both").lower()
        header_variants = []
        if preferred == "x-api-key":
            header_variants = ["x-api-key", "authorization", "both"]
        elif preferred == "authorization":
            header_variants = ["authorization", "x-api-key", "both"]
        else:
            header_variants = ["both", "x-api-key", "authorization"]

        last_err = None
        for mode in header_variants:
            headers = {
                "Accept": "application/json, application/vnd.esios-api-v1+json",
                "Content-Type": "application/json; charset=utf-8",
                "User-Agent": "tfm-energy-ingest/1.0",
                "Cache-Control": "no-cache",
            }
            if mode in ("x-api-key", "both"):
                headers["x-api-key"] = self.api_key
            if mode in ("authorization", "both"):
                headers["Authorization"] = f"Token token={self.api_key}"

            resp = requests.get(
                url, headers=headers, params=params, timeout=self.timeout
            )
            self._last_call = time.time()

            if resp.ok:
                return resp.json()
            # 403: probar siguiente variante
            last_err = resp
            if resp.status_code == 403:
                continue

            # Otros códigos -> fallo inmediato con detalle
            info = f"HTTP {resp.status_code} - {resp.reason}"
            try:
                body = resp.json()
            except Exception:
                body = resp.text[:500]
            raise requests.HTTPError(f"{info} | URL={resp.url} | Body={body}")

        # Si se agotaron variantes
        if last_err is not None:
            info = f"HTTP {last_err.status_code} - {last_err.reason}"
            try:
                body = last_err.json()
            except Exception:
                body = last_err.text[:500]
            raise requests.HTTPError(f"{info} | URL={last_err.url} | Body={body}")
        raise requests.HTTPError("Fallo de autenticación ESIOS desconocido")
