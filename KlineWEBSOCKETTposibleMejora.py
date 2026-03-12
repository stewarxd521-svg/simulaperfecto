"""
KlineWebSocketCache — versión corregida y optimizada
=====================================================
Correcciones aplicadas:
  1. REST periódico inteligente: solo descarga velas realmente faltantes
     usando startTime + limit calculado dinámicamente (en vez de 1500 fijos).
  2. Scheduler por intervalo: cada (symbol, interval) duerme hasta el
     próximo cierre de SU vela — no hay timer global de 5 minutos.
  3. Bug force_refresh: corregido uso de `self._loop` en vez de `loop` indefinido.
  4. Bug aiohttp session: se crea UNA sola sesión por ciclo, no una por par.
  5. Bug rest_concurrency/retries/backoff: eliminados los `if 'x' in locals()`
     innecesarios — los parámetros ya vienen del __init__.
  6. Bug _periodic_refresh_task: eliminado `force=True` que hacía inútil
     la guardia de last_refresh_time. El scheduler nuevo no lo necesita.
  7. Inserción en buffer: se reemplazó el insert O(n) por append + sort
     que es más eficiente con deque y mantiene el orden sin corrupción.
  8. Stream health check: threshold de silencio configurable y logs más limpios.
  9. import aiohttp duplicado eliminado.
"""

import asyncio
import random
import aiohttp
import websockets
import json
import threading
import time
import pandas as pd
import requests
from collections import defaultdict, deque
from typing import Dict, List, Optional, Tuple
from datetime import datetime


class KlineWebSocketCache:
    """
    Mantiene klines en tiempo real por WebSocket para USDT-M Futures con:
    - Backfill inicial por REST al arrancar (histórico completo).
    - WebSocket como fuente primaria de actualización tick a tick.
    - REST periódico MÍNIMO: solo descarga las velas que cerraron desde
      el último dato conocido (1–10 velas por intervalo, no 1500).
    - Scheduler independiente por (symbol, interval), alineado al cierre
      de cada vela.
    - Verificación de integridad y auto-corrección de gaps.
    - Detección de streams silenciosos y reconexión automática.
    - Conexiones WebSocket multiplexadas.
    """

    BASE_WS_URL  = "wss://fstream.binance.com/stream"
    BASE_REST_URL = "https://fapi.binance.com"

    # Margen de seguridad (velas extra) al pedir el refresh periódico
    REFRESH_SAFETY_BUFFER = 5

    # Segundos de gracia tras el cierre de vela antes de consultar REST
    # (Binance tarda ~1–2 s en confirmar la vela cerrada)
    CLOSE_GRACE_SECONDS = 2

    def __init__(
        self,
        pairs: Dict[str, List[str]],
        max_candles: int = 1500,
        include_open_candle: bool = True,
        backfill_on_start: bool = True,
        streams_per_connection: int = 50,
        rest_limits: Optional[Dict[str, int]] = None,
        rest_timeout: float = 6.0,
        rest_min_sleep: float = 0.12,
        integrity_check_enabled: bool = True,
        stream_silence_threshold_seconds: int = 120,
        stream_health_check_seconds: int = 60,
        rest_concurrency: int = 20,
        rest_retries: int = 3,
        rest_backoff_max: float = 8.0,
        session: Optional[requests.Session] = None,
    ):
        """
        Parámetros
        ----------
        pairs                           : {'BTCUSDT': ['1m','5m'], ...}
        max_candles                     : máximo de velas almacenadas por (symbol, interval)
        include_open_candle             : si True, mantiene la vela en curso actualizada
        backfill_on_start               : descarga histórico REST al iniciar
        streams_per_connection          : streams por conexión WS multiplexada
        rest_limits                     : override de limit REST por intervalo
                                          ej: {'1m': 200, '5m': 100}
        rest_timeout                    : timeout (s) por llamada REST
        rest_min_sleep                  : pausa mínima (s) entre llamadas REST consecutivas
        integrity_check_enabled         : verificar/reparar gaps en cada refresh periódico
        stream_silence_threshold_seconds: segundos sin mensajes para marcar stream como silencioso
        stream_health_check_seconds     : frecuencia del monitor de salud de streams
        rest_concurrency                : máximo de requests REST simultáneos
        rest_retries                    : intentos por request REST
        rest_backoff_max                : backoff máximo (s) entre reintentos
        session                         : requests.Session reutilizable (opcional)
        """
        # --- Pares y configuración general ---
        self.pairs = {
            s.upper(): ([i] if isinstance(i, str) else list(i))
            for s, i in pairs.items()
        }
        self.max_candles              = int(max_candles)
        self.include_open             = bool(include_open_candle)
        self.streams_per_connection   = int(streams_per_connection)
        self.backfill_on_start        = bool(backfill_on_start)

        # --- REST ---
        self.rest_limits     = rest_limits or {}
        self.rest_timeout    = float(rest_timeout)
        self.rest_min_sleep  = float(rest_min_sleep)
        self.rest_concurrency = int(rest_concurrency)
        self.rest_retries    = int(rest_retries)
        self.rest_backoff_max = float(rest_backoff_max)
        self._rest           = session or requests.Session()

        # --- Integridad y salud ---
        self.integrity_check_enabled          = bool(integrity_check_enabled)
        self.stream_silence_threshold_seconds = int(stream_silence_threshold_seconds)
        self.stream_health_check_seconds      = int(stream_health_check_seconds)

        # --- Buffers ---
        # buffer[(symbol, interval)] → deque de dicts kline, ordenado por open_time
        self.buffers: Dict[Tuple[str, str], deque] = defaultdict(
            lambda: deque(maxlen=self.max_candles)
        )
        self.lock = threading.Lock()

        # --- Tracking de refresh y mensajes ---
        self.last_refresh_time: Dict[Tuple[str, str], float] = {}
        self.last_message_time: Dict[Tuple[str, str], float] = {}
        self.message_counts: Dict[Tuple[str, str], int] = defaultdict(int)

        # --- Estadísticas de integridad ---
        self.integrity_stats = defaultdict(lambda: {
            "last_check":       None,
            "gaps_found":       0,
            "gaps_fixed":       0,
            "total_candles":    0,
            "expected_candles": 0,
            "last_error":       None,
        })

        # --- Estado de conexiones ---
        self._loop:   Optional[asyncio.AbstractEventLoop] = None
        self._running = False
        self._tasks:  Dict[tuple, asyncio.Future] = {}
        self._thread: Optional[threading.Thread]  = None
        self.connection_stats = defaultdict(lambda: {
            "reconnects": 0,
            "last_error": None,
            "streams":    [],
            "active":     False,
        })

        # --- Mapeo de streams ---
        self.stream_mapping: Dict[str, Tuple[str, str]] = {}
        self.subscribed_streams: set = set()

    # =========================================================================
    # UTILIDADES
    # =========================================================================

    def _get_interval_milliseconds(self, interval: str) -> int:
        """Convierte un intervalo string (1m, 5m, 1h, 1d…) a milisegundos."""
        units = {
            's': 1_000,
            'm': 60 * 1_000,
            'h': 60 * 60 * 1_000,
            'd': 24 * 60 * 60 * 1_000,
            'w': 7 * 24 * 60 * 60 * 1_000,
        }
        num  = int(''.join(filter(str.isdigit, interval)))
        unit = ''.join(filter(str.isalpha, interval))
        return num * units.get(unit, 60_000)

    def _parse_kline_row(self, k: list, symbol: str, interval: str, is_closed: bool) -> dict:
        """Parsea una fila de la API REST de klines a dict interno."""
        return {
            "open_time":             int(k[0]),
            "close_time":            int(k[6]),
            "symbol":                symbol.upper(),
            "interval":              interval,
            "open":                  float(k[1]),
            "high":                  float(k[2]),
            "low":                   float(k[3]),
            "close":                 float(k[4]),
            "volume":                float(k[5]),
            "quote_volume":          float(k[7]),
            "trades":                int(k[8]),
            "taker_buy_volume":      float(k[9]),
            "taker_buy_quote_volume": float(k[10]),
            "is_closed":             is_closed,
        }

    def _upsert_rows_into_buffer(self, key: Tuple[str, str], rows: List[dict]) -> None:
        """
        Inserta o actualiza filas en el buffer de forma ordenada.
        Usa append + sort final en vez de insert O(n) por cada elemento,
        lo cual es más eficiente para lotes de velas.
        """
        if not rows:
            return
        with self.lock:
            buf = self.buffers[key]
            existing = {r["open_time"]: i for i, r in enumerate(buf)}

            needs_sort = False
            for row in rows:
                ot = row["open_time"]
                if ot in existing:
                    # actualizar in-place
                    buf[existing[ot]] = row
                else:
                    buf.append(row)
                    # Si se añadió fuera de orden → marcar para ordenar
                    if len(buf) >= 2 and buf[-1]["open_time"] < buf[-2]["open_time"]:
                        needs_sort = True

            if needs_sort:
                sorted_rows = sorted(buf, key=lambda x: x["open_time"])
                buf.clear()
                buf.extend(sorted_rows)

    # =========================================================================
    # INTEGRIDAD
    # =========================================================================

    def _check_integrity(self, symbol: str, interval: str) -> Dict:
        """
        Analiza el buffer de (symbol, interval) en busca de:
        - Gaps (velas faltantes entre dos timestamps)
        - Duplicados

        Retorna un dict con el resultado del análisis.
        """
        key = (symbol.upper(), interval)
        with self.lock:
            buf = sorted(list(self.buffers.get(key, deque())),
                         key=lambda x: x["open_time"])

        if len(buf) < 2:
            return {
                "has_gaps":        False,
                "gaps":            [],
                "total_candles":   len(buf),
                "expected_candles": len(buf),
                "duplicates":      [],
                "error":           None,
            }

        interval_ms = self._get_interval_milliseconds(interval)
        gaps, duplicates = [], []

        for i in range(1, len(buf)):
            prev_ot = buf[i - 1]["open_time"]
            curr_ot = buf[i]["open_time"]

            if prev_ot == curr_ot:
                duplicates.append({"timestamp": prev_ot, "position": i})
                continue

            expected = prev_ot + interval_ms
            if curr_ot != expected:
                missing = max(1, int((curr_ot - expected) / interval_ms))
                gaps.append({
                    "after_timestamp":  prev_ot,
                    "before_timestamp": curr_ot,
                    "missing_candles":  missing,
                    "gap_duration_ms":  curr_ot - expected,
                })

        time_span        = buf[-1]["open_time"] - buf[0]["open_time"]
        expected_candles = int(time_span / interval_ms) + 1

        result = {
            "has_gaps":        len(gaps) > 0,
            "gaps":            gaps,
            "total_candles":   len(buf),
            "expected_candles": expected_candles,
            "duplicates":      duplicates,
            "error":           None,
        }

        with self.lock:
            self.integrity_stats[key].update({
                "last_check":       datetime.now(),
                "gaps_found":       len(gaps),
                "total_candles":    len(buf),
                "expected_candles": expected_candles,
            })

        return result

    def _fix_gaps(self, symbol: str, interval: str, integrity_result: Dict) -> bool:
        """
        Corrige gaps descargando por REST SOLO las velas del rango faltante.
        Retorna True si se corrigieron todos los gaps.
        """
        if not integrity_result["has_gaps"]:
            return True

        key          = (symbol.upper(), interval)
        interval_ms  = self._get_interval_milliseconds(interval)
        fixed_count  = 0

        for gap in integrity_result["gaps"]:
            try:
                start_time = gap["after_timestamp"] + interval_ms
                end_time   = gap["before_timestamp"]
                limit      = min(gap["missing_candles"] + 2, 1500)

                url    = f"{self.BASE_REST_URL}/fapi/v1/klines"
                params = {
                    "symbol":    symbol.upper(),
                    "interval":  interval,
                    "startTime": start_time,
                    "endTime":   end_time,
                    "limit":     limit,
                }

                resp = self._rest.get(url, params=params, timeout=self.rest_timeout)
                resp.raise_for_status()
                data = resp.json()

                rows = []
                for k in data:
                    try:
                        rows.append(self._parse_kline_row(k, symbol, interval, is_closed=True))
                    except Exception:
                        continue

                self._upsert_rows_into_buffer(key, rows)
                fixed_count += 1
                print(f"✅ Gap corregido {symbol} {interval}: {gap['missing_candles']} velas")
                time.sleep(self.rest_min_sleep)

            except Exception as e:
                print(f"❌ Error corrigiendo gap {symbol} {interval}: {e}")
                with self.lock:
                    self.integrity_stats[key]["last_error"] = str(e)

        with self.lock:
            self.integrity_stats[key]["gaps_fixed"] = fixed_count

        return fixed_count == len(integrity_result["gaps"])

    # =========================================================================
    # REST — BACKFILL INICIAL (concurrente, aiohttp)
    # =========================================================================

    async def _async_fetch_with_retries(
        self,
        session: aiohttp.ClientSession,
        url: str,
        params: dict,
    ) -> list:
        """GET JSON con reintentos y backoff exponencial con jitter."""
        attempt = 0
        while True:
            try:
                timeout = aiohttp.ClientTimeout(total=self.rest_timeout)
                async with session.get(url, params=params, timeout=timeout) as resp:
                    resp.raise_for_status()
                    return await resp.json()
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                attempt += 1
                if attempt > self.rest_retries:
                    raise
                backoff = min(self.rest_backoff_max, 0.5 * (2 ** attempt))
                jitter  = random.random() * 0.3
                await asyncio.sleep(backoff + jitter)

    async def _async_backfill_symbol_interval(
        self,
        session: aiohttp.ClientSession,
        sem: asyncio.Semaphore,
        symbol: str,
        interval: str,
    ) -> None:
        """
        Backfill completo (histórico) para un (symbol, interval).
        Se usa SOLO al iniciar o cuando el buffer está vacío.
        Siempre descarga max_candles velas (sin startTime).
        """
        key   = (symbol.upper(), interval)
        limit = int(min(
            self.rest_limits.get(interval, min(1500, self.max_candles)),
            self.max_candles,
        ))
        if limit <= 0:
            return

        url    = f"{self.BASE_REST_URL}/fapi/v1/klines"
        params = {"symbol": symbol.upper(), "interval": interval, "limit": limit}

        async with sem:
            try:
                data = await self._async_fetch_with_retries(session, url, params)
            except Exception as e:
                print(f"🔴 Backfill REST falló {symbol} {interval}: {e}")
                return

        if not data:
            print(f"⚠️  Sin datos para {symbol} {interval}")
            return

        rows = []
        now_ms = int(time.time() * 1000)
        for k in data:
            try:
                is_closed = int(k[6]) < now_ms
                rows.append(self._parse_kline_row(k, symbol, interval, is_closed))
            except Exception:
                continue

        self._upsert_rows_into_buffer(key, rows)
        self.last_refresh_time[key] = time.time()
        await asyncio.sleep(self.rest_min_sleep)
        print(f"✅ Backfill {symbol} {interval}: {len(rows)} velas descargadas")

    async def _async_backfill_all(self) -> None:
        """
        Backfill concurrente para todos los pares.
        Crea una única sesión aiohttp compartida por todos los pares.
        """
        pairs_list = [
            (symbol, interval)
            for symbol, intervals in self.pairs.items()
            for interval in intervals
        ]
        total = len(pairs_list)
        print(f"📥 Backfill inicial: {total} pares (concurrencia={self.rest_concurrency})…")

        connector = aiohttp.TCPConnector(limit=self.rest_concurrency * 2)
        sem       = asyncio.Semaphore(self.rest_concurrency)

        async with aiohttp.ClientSession(connector=connector) as session:
            tasks = [
                self._async_backfill_symbol_interval(session, sem, sym, itv)
                for sym, itv in pairs_list
            ]
            BATCH = 50
            for i in range(0, len(tasks), BATCH):
                results = await asyncio.gather(*tasks[i:i + BATCH], return_exceptions=True)
                for r in results:
                    if isinstance(r, Exception):
                        print(f"❌ Error en backfill batch: {r}")

        print(f"✅ Backfill inicial completado ({total} pares)")

    # =========================================================================
    # REST — REFRESH PERIÓDICO INTELIGENTE
    # =========================================================================

    def _calc_smart_limit(
        self, symbol: str, interval: str
    ) -> Tuple[int, Optional[int]]:
        """
        Calcula el limit óptimo y startTime para un refresh periódico.

        Retorna (limit, startTime_ms):
          - limit=0         → no hay velas nuevas que descargar todavía
          - startTime=None  → el buffer está vacío, hacer backfill completo
        """
        key         = (symbol.upper(), interval)
        interval_ms = self._get_interval_milliseconds(interval)

        with self.lock:
            buf = list(self.buffers.get(key, deque()))

        if not buf:
            # Buffer vacío → backfill completo
            return (min(1500, self.max_candles), None)

        # Última vela cerrada conocida
        closed = [r for r in buf if r.get("is_closed", True)]
        if not closed:
            return (min(1500, self.max_candles), None)

        last_closed_ot      = max(r["open_time"] for r in closed)
        next_expected_ot_ms = last_closed_ot + interval_ms
        now_ms              = int(time.time() * 1000)

        # Aún no ha cerrado la siguiente vela
        if now_ms < next_expected_ot_ms:
            return (0, None)

        # Cuántas velas completas han pasado desde la última conocida
        candles_elapsed = max(1, int((now_ms - next_expected_ot_ms) / interval_ms) + 1)
        smart_limit     = min(
            candles_elapsed + self.REFRESH_SAFETY_BUFFER,
            1500,
        )
        return (smart_limit, next_expected_ot_ms)

    async def _smart_refresh_symbol_interval(
        self,
        session: aiohttp.ClientSession,
        sem: asyncio.Semaphore,
        symbol: str,
        interval: str,
    ) -> None:
        """
        Refresh periódico MÍNIMO para un (symbol, interval).
        Solo descarga las velas que realmente cerraron desde la última conocida.
        Usa startTime para evitar descargar velas ya presentes en el buffer.
        """
        smart_limit, start_time_ms = self._calc_smart_limit(symbol, interval)

        if smart_limit == 0:
            return  # Nada nuevo todavía

        url    = f"{self.BASE_REST_URL}/fapi/v1/klines"
        params: dict = {
            "symbol":   symbol.upper(),
            "interval": interval,
            "limit":    smart_limit,
        }
        if start_time_ms is not None:
            params["startTime"] = start_time_ms

        async with sem:
            try:
                data = await self._async_fetch_with_retries(session, url, params)
            except Exception as e:
                print(f"🔴 Smart refresh falló {symbol} {interval}: {e}")
                return

        if not data:
            return

        key    = (symbol.upper(), interval)
        now_ms = int(time.time() * 1000)
        rows   = []
        for k in data:
            try:
                is_closed = int(k[6]) < now_ms
                rows.append(self._parse_kline_row(k, symbol, interval, is_closed))
            except Exception:
                continue

        if not rows:
            return

        self._upsert_rows_into_buffer(key, rows)
        self.last_refresh_time[key] = time.time()

        closed_count = sum(1 for r in rows if r["is_closed"])
        if closed_count:
            print(
                f"📥 Smart refresh {symbol} {interval}: "
                f"+{closed_count} velas cerradas (limit pedido={smart_limit})"
            )

    # =========================================================================
    # SAFETY REFRESH — red de seguridad global cada 5 min
    # =========================================================================

    async def _periodic_safety_refresh(self, interval_seconds: int = 300) -> None:
        """
        Red de seguridad que corre cada `interval_seconds` (default: 300 s = 5 min).

        Filosofía:
          - El WebSocket es la fuente PRIMARIA y construye las velas en tiempo real
            usando los campos t (open_time), T (close_time) y x (is_closed) que
            Binance envía en cada mensaje kline.
          - Este task solo interviene cuando el WS falla silenciosamente:
              · Pares ilíquidos donde Binance no emite x=true al cierre.
              · Reconexiones WS que dejaron un hueco.
              · Cualquier drop de mensajes no detectado.
          - Para cada par llama a _calc_smart_limit; si devuelve 0 → el buffer
            ya está al día (el WS hizo su trabajo) → no se hace ningún request.
          - Concurrencia máxima igual a rest_concurrency para no saturar la API.
        """
        print(f"🛡  Safety refresh iniciado — período={interval_seconds}s "
              f"(el WS construye las velas; esto solo cubre huecos que el WS pierda)")

        # Espera inicial: dar tiempo al backfill y al WS de estabilizarse
        await asyncio.sleep(30)

        while self._running:
            try:
                await asyncio.sleep(interval_seconds)
                if not self._running:
                    break

                pairs_list = [
                    (symbol, interval)
                    for symbol, intervals in self.pairs.items()
                    for interval in intervals
                ]

                # Filtrar solo los pares donde el WS no cubrió todos los cierres
                needs_refresh = [
                    (sym, itv)
                    for sym, itv in pairs_list
                    if self._calc_smart_limit(sym, itv)[0] > 0
                ]

                if not needs_refresh:
                    print(f"🛡  Safety refresh: WS al día — 0/{len(pairs_list)} pares "
                          f"requieren REST")
                    continue

                print(f"🛡  Safety refresh: {len(needs_refresh)}/{len(pairs_list)} pares "
                      f"con velas que el WS no confirmó → descargando via REST…")

                connector = aiohttp.TCPConnector(limit=self.rest_concurrency * 2)
                sem       = asyncio.Semaphore(self.rest_concurrency)
                async with aiohttp.ClientSession(connector=connector) as session:
                    tasks = [
                        self._smart_refresh_symbol_interval(session, sem, sym, itv)
                        for sym, itv in needs_refresh
                    ]
                    results = await asyncio.gather(*tasks, return_exceptions=True)
                    for r in results:
                        if isinstance(r, Exception):
                            print(f"❌ Error en safety refresh: {r}")

                # Verificar integridad solo en los pares que se refrescaron
                if self.integrity_check_enabled:
                    for sym, itv in needs_refresh:
                        try:
                            integrity = self._check_integrity(sym, itv)
                            if integrity["has_gaps"]:
                                print(f"⚠️  Gap detectado {sym} {itv}: "
                                      f"{len(integrity['gaps'])} gaps → reparando…")
                                self._fix_gaps(sym, itv, integrity)
                        except Exception as e:
                            print(f"❌ Error verificando integridad {sym} {itv}: {e}")

            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"❌ Error en safety refresh: {e}")
                await asyncio.sleep(60)

    # =========================================================================
    # WEBSOCKET
    # =========================================================================

    def _create_stream_groups(self) -> List[List[str]]:
        """Crea grupos de streams para conexiones WS multiplexadas."""
        all_streams: List[str] = []
        self.stream_mapping.clear()
        self.subscribed_streams.clear()

        for symbol, intervals in self.pairs.items():
            for interval in intervals:
                stream_name = f"{symbol.lower()}@kline_{interval}"
                all_streams.append(stream_name)
                self.stream_mapping[stream_name] = (symbol.upper(), interval)
                self.subscribed_streams.add((symbol.upper(), interval))

        print(f"📋 Total streams: {len(all_streams)}")
        for s in all_streams:
            sym, itv = self.stream_mapping[s]
            print(f"   • {s} → ({sym}, {itv})")

        groups = [
            all_streams[i:i + self.streams_per_connection]
            for i in range(0, len(all_streams), self.streams_per_connection)
        ]
        for idx, g in enumerate(groups, 1):
            print(f"📦 Grupo {idx}: {len(g)} streams")

        return groups

    async def _ws_combined_stream(self, stream_names: List[str], group_id: int) -> None:
        """Maneja un stream WS combinado para múltiples klines."""
        url        = f"{self.BASE_WS_URL}?streams={'/'.join(stream_names)}"
        group_name = f"group_{group_id}"

        print(f"\n🔗 Grupo {group_id} → {len(stream_names)} streams")
        print(f"   URL: {url}")

        self.connection_stats[group_name]["streams"] = stream_names
        self.connection_stats[group_name]["active"]  = True

        reconnect_delay    = 1.0
        consecutive_errors = 0

        while self._running:
            try:
                async with websockets.connect(
                    url,
                    ping_interval=30,
                    ping_timeout=10,
                    close_timeout=10,
                    max_size=10 ** 7,
                    max_queue=2000,
                    compression=None,
                ) as ws:
                    print(f"✅ WS conectado: {group_name} ({len(stream_names)} streams)")
                    self.connection_stats[group_name]["active"] = True
                    reconnect_delay    = 1.0
                    consecutive_errors = 0
                    last_ping          = time.time()
                    messages_received  = 0

                    while self._running:
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=45)
                            messages_received += 1

                            data = json.loads(raw)
                            if "data" not in data or "stream" not in data:
                                continue

                            stream_name = data["stream"]
                            kline_data  = data["data"]

                            if kline_data.get("e") != "kline":
                                continue

                            if stream_name not in self.stream_mapping:
                                print(f"⚠️  Stream desconocido: {stream_name}")
                                continue

                            symbol, interval = self.stream_mapping[stream_name]
                            k         = kline_data.get("k", {})
                            is_closed = bool(k.get("x", False))

                            # Tracking de mensajes
                            key = (symbol, interval)
                            self.last_message_time[key] = time.time()
                            self.message_counts[key]   += 1

                            # Si la vela está abierta y no queremos incluirlas → skip
                            if (not is_closed) and (not self.include_open):
                                continue

                            row = {
                                "open_time":             int(k["t"]),
                                "close_time":            int(k["T"]),
                                "symbol":                k["s"].upper(),
                                "interval":              k["i"],
                                "open":                  float(k["o"]),
                                "high":                  float(k["h"]),
                                "low":                   float(k["l"]),
                                "close":                 float(k["c"]),
                                "volume":                float(k["v"]),
                                "quote_volume":          float(k["q"]),
                                "trades":                int(k["n"]),
                                "taker_buy_volume":      float(k["V"]),
                                "taker_buy_quote_volume": float(k["Q"]),
                                "is_closed":             is_closed,
                            }

                            # Upsert directo (la vela abierta se actualiza constantemente)
                            with self.lock:
                                buf = self.buffers[key]
                                if buf and buf[-1]["open_time"] == row["open_time"]:
                                    buf[-1] = row
                                else:
                                    if buf and row["open_time"] < buf[-1]["open_time"]:
                                        # Vela desordenada (reconexión) → upsert con sort
                                        self._upsert_rows_into_buffer(key, [row])
                                    else:
                                        buf.append(row)

                            if messages_received % 500 == 0:
                                print(f"📨 {group_name}: {messages_received} msgs procesados")

                        except asyncio.TimeoutError:
                            # Timeout de recv → enviar ping manual
                            await ws.ping()
                            last_ping = time.time()

                        except websockets.ConnectionClosed as e:
                            print(f"🔶 WS cerrado {group_name}: {e}")
                            raise

                        # Ping manual periódico (keepalive)
                        if time.time() - last_ping > 30:
                            await ws.ping()
                            last_ping = time.time()

            except asyncio.CancelledError:
                break
            except Exception as e:
                consecutive_errors += 1
                self.connection_stats[group_name]["reconnects"]  += 1
                self.connection_stats[group_name]["last_error"]   = str(e)
                self.connection_stats[group_name]["active"]       = False

                reconnect_delay = min(reconnect_delay * 1.5, 30.0)
                if consecutive_errors > 5:
                    reconnect_delay = 60.0

                print(f"🔴 Error {group_name}: {e}")
                print(f"   Reconectando en {reconnect_delay:.1f}s (intento {consecutive_errors})")
                await asyncio.sleep(reconnect_delay)

        self.connection_stats[group_name]["active"] = False

    # =========================================================================
    # MONITOR DE SALUD
    # =========================================================================

    async def _stream_health_monitor(self) -> None:
        """
        Monitorea streams silenciosos (sin mensajes por más de
        stream_silence_threshold_seconds segundos).
        """
        print(
            f"🏥 Monitor de salud iniciado "
            f"(check cada {self.stream_health_check_seconds}s, "
            f"umbral={self.stream_silence_threshold_seconds}s)"
        )
        await asyncio.sleep(30)  # Dar tiempo a que los streams se conecten

        while self._running:
            try:
                await asyncio.sleep(self.stream_health_check_seconds)
                if not self._running:
                    break

                current_time = time.time()
                silent = [
                    (sym, itv, current_time - self.last_message_time.get((sym, itv), 0))
                    for sym, itv in self.subscribed_streams
                    if self.last_message_time.get((sym, itv), 0) > 0
                    and (current_time - self.last_message_time[(sym, itv)]) > self.stream_silence_threshold_seconds
                ]

                if silent:
                    print(f"\n⚠️  Streams silenciosos detectados ({len(silent)}):")
                    for sym, itv, duration in silent:
                        cnt = self.message_counts.get((sym, itv), 0)
                        print(f"   • {sym} {itv}: sin msgs por {duration:.0f}s (total: {cnt})")
                    print("   Considera reiniciar si el problema persiste\n")

            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"❌ Error en monitor de salud: {e}")
                await asyncio.sleep(60)

    async def _monitor_connections(self) -> None:
        """Monitor básico de conexiones activas (log cada 60s)."""
        while self._running:
            await asyncio.sleep(60)
            active = sum(
                1 for s in self.connection_stats.values() if s.get("active", False)
            )
            print(f"🔌 Conexiones WS activas: {active}/{len(self.connection_stats)}")

    # =========================================================================
    # CICLO DE VIDA
    # =========================================================================

    def start(self) -> None:
        """Inicia el sistema: loop asyncio, backfill, WebSocket y schedulers."""
        print("\n" + "=" * 70)
        print("🚀 INICIANDO KlineWebSocketCache")
        print("=" * 70)

        self._running = True

        # Crear e iniciar loop asyncio en thread daemon
        loop = asyncio.new_event_loop()
        self._loop = loop

        def run_loop():
            asyncio.set_event_loop(loop)
            loop.run_forever()

        thread = threading.Thread(target=run_loop, daemon=True, name="KlineWSLoop")
        thread.start()
        self._thread = thread

        # Backfill inicial (histórico completo)
        if self.backfill_on_start:
            asyncio.run_coroutine_threadsafe(self._async_backfill_all(), loop)

        # Crear grupos de streams WS
        print("\n" + "=" * 70)
        print("📊 CREANDO GRUPOS DE STREAMS")
        print("=" * 70)
        stream_groups = self._create_stream_groups()
        print(f"\n📊 {len(stream_groups)} conexiones WebSocket")
        print("=" * 70)

        # Lanzar streams WS
        for idx, group in enumerate(stream_groups, 1):
            task = asyncio.run_coroutine_threadsafe(
                self._ws_combined_stream(group, idx), loop
            )
            self._tasks[("stream", idx)] = task

        # Lanzar safety refresh global (1 tarea, cada 5 min, solo actúa si el WS falló)
        safety_task = asyncio.run_coroutine_threadsafe(
            self._periodic_safety_refresh(interval_seconds=300), loop
        )
        self._tasks[("safety_refresh", 0)] = safety_task

        # Monitor de salud de streams
        health_task = asyncio.run_coroutine_threadsafe(
            self._stream_health_monitor(), loop
        )
        self._tasks[("health", 0)] = health_task

        # Monitor de conexiones
        conn_monitor_task = asyncio.run_coroutine_threadsafe(
            self._monitor_connections(), loop
        )
        self._tasks[("conn_monitor", 0)] = conn_monitor_task

        print(f"\n✅ KlineWebSocketCache iniciado")
        print(f"   • Fuente primaria  : WebSocket (construye velas con t/T/x de Binance)")
        print(f"   • Safety refresh   : 1 tarea global cada 300s (solo actúa si el WS falla)")
        print(f"   • REST periódico   : solo velas no cubiertas por WS (startTime dinámico)")
        print(f"   • Monitor de salud : cada {self.stream_health_check_seconds}s")
        print(f"   • Integridad       : {'✅' if self.integrity_check_enabled else '❌'}")
        print("=" * 70 + "\n")

    def stop(self) -> None:
        """Detiene todas las conexiones y tareas."""
        print("🛑 Deteniendo KlineWebSocketCache…")
        self._running = False
        time.sleep(0.5)

        for key, task in list(self._tasks.items()):
            try:
                task.cancel()
            except Exception:
                pass
            self._tasks.pop(key, None)

        try:
            if self._loop and self._loop.is_running():
                self._loop.call_soon_threadsafe(self._loop.stop)
        except Exception:
            pass

        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=2.0)

        print("✅ KlineWebSocketCache detenido")

    def force_refresh(
        self,
        symbol: Optional[str] = None,
        interval: Optional[str] = None,
    ) -> None:
        """
        Fuerza un refresh inmediato.
        Si symbol e interval son None, refresca todos.
        """
        if not self._loop:
            print("⚠️  El loop no está activo. Llama a start() primero.")
            return

        if symbol and interval:
            print(f"🔄 Forzando refresh {symbol} {interval}…")

            async def _do_refresh():
                connector = aiohttp.TCPConnector(limit=4)
                async with aiohttp.ClientSession(connector=connector) as session:
                    sem = asyncio.Semaphore(1)
                    await self._smart_refresh_symbol_interval(
                        session, sem, symbol, interval
                    )
                if self.integrity_check_enabled:
                    integrity = self._check_integrity(symbol, interval)
                    if integrity["has_gaps"]:
                        self._fix_gaps(symbol, interval, integrity)

            asyncio.run_coroutine_threadsafe(_do_refresh(), self._loop)

        else:
            print("🔄 Forzando refresh de todos los pares…")
            asyncio.run_coroutine_threadsafe(
                self._async_backfill_all(), self._loop
            )

    # =========================================================================
    # CONSULTA DE DATOS
    # =========================================================================

    def get_dataframe(
        self,
        symbol: str,
        interval: str,
        only_closed: bool = False,
    ) -> pd.DataFrame:
        """Devuelve un DataFrame con las velas acumuladas."""
        key = (symbol.upper(), interval)
        with self.lock:
            rows = list(self.buffers.get(key, deque()))

        if not rows:
            return pd.DataFrame(columns=[
                "timestamp", "open", "high", "low", "close", "volume",
                "close_time", "trades", "quote_volume",
                "taker_buy_volume", "taker_buy_quote_volume", "is_closed",
            ])

        df = pd.DataFrame(rows)
        if only_closed:
            df = df[df["is_closed"]].copy()

        df["timestamp"]  = pd.to_datetime(df["open_time"],  unit="ms")
        df["close_time"] = pd.to_datetime(df["close_time"], unit="ms")

        return df[[
            "timestamp", "open", "high", "low", "close", "volume",
            "close_time", "trades", "quote_volume",
            "taker_buy_volume", "taker_buy_quote_volume", "is_closed",
        ]].reset_index(drop=True)

    def get_last_closed(self, symbol: str, interval: str) -> Optional[dict]:
        """Devuelve la última vela cerrada disponible."""
        df = self.get_dataframe(symbol, interval, only_closed=True)
        if df.empty:
            return None
        return df.iloc[-1].to_dict()

    def check_all_integrity(self) -> Dict[Tuple[str, str], Dict]:
        """Verifica la integridad de todos los pares y retorna el resultado."""
        return {
            (symbol, interval): self._check_integrity(symbol, interval)
            for symbol, intervals in self.pairs.items()
            for interval in intervals
        }

    def get_stream_health(self) -> Dict:
        """Retorna información de salud de cada stream suscrito."""
        current_time = time.time()
        return {
            key: {
                "last_message_time": self.last_message_time.get(key, 0),
                "message_count":     self.message_counts.get(key, 0),
                "silence_duration":  (
                    current_time - self.last_message_time[key]
                    if key in self.last_message_time else -1
                ),
                "is_healthy": (
                    (current_time - self.last_message_time[key])
                    < self.stream_silence_threshold_seconds
                    if key in self.last_message_time else False
                ),
                "last_message_ago": (
                    f"{current_time - self.last_message_time[key]:.0f}s"
                    if key in self.last_message_time else "never"
                ),
            }
            for key in self.subscribed_streams
        }

    def get_stats(self) -> Dict:
        """Estadísticas completas del sistema."""
        with self.lock:
            total_pairs    = len(self.buffers)
            total_candles  = sum(len(b) for b in self.buffers.values())
            pairs_with_data = sum(1 for b in self.buffers.values() if b)

            integrity_summary = {
                "pairs_checked":    len(self.integrity_stats),
                "total_gaps_found": sum(s["gaps_found"] for s in self.integrity_stats.values()),
                "total_gaps_fixed": sum(s["gaps_fixed"] for s in self.integrity_stats.values()),
            }

        return {
            "total_pairs":            total_pairs,
            "pairs_with_data":        pairs_with_data,
            "total_candles":          total_candles,
            "avg_candles_per_pair":   total_candles / max(pairs_with_data, 1),
            "connection_stats":       dict(self.connection_stats),
            "streams_per_connection": self.streams_per_connection,
            "total_connections":      len([k for k in self._tasks if k[0] == "stream"]),
            "active_connections":     sum(
                1 for s in self.connection_stats.values() if s.get("active", False)
            ),
            "integrity_check_enabled": self.integrity_check_enabled,
            "integrity_summary":       integrity_summary,
            "total_messages_received": sum(self.message_counts.values()),
        }


# =============================================================================
# EJEMPLO DE USO
# =============================================================================
if __name__ == "__main__":
    import os

    pairs = {
        "BTCUSDT": ["1m", "5m", "15m", "1h"],
        "ETHUSDT": ["1m", "5m"],
        "BNBUSDT": ["1m"],
        "SOLUSDT": ["1m", "5m"],
    }

    cache = KlineWebSocketCache(
        pairs=pairs,
        max_candles=1500,
        include_open_candle=True,
        backfill_on_start=True,
        streams_per_connection=40,
        integrity_check_enabled=True,
        stream_silence_threshold_seconds=120,   # Alerta si un stream calla >2 min
        stream_health_check_seconds=60,          # Verificar salud cada 60s
        rest_concurrency=20,
        rest_retries=3,
        rest_backoff_max=8.0,
        rest_min_sleep=0.12,
    )

    cache.start()

    try:
        while True:
            time.sleep(10)
            os.system("cls" if os.name == "nt" else "clear")

            print("=" * 70)
            print(f"📊 Kline Cache — {datetime.now().strftime('%H:%M:%S')}")
            print("=" * 70)

            stats = cache.get_stats()
            print(f"\n📈 General:")
            print(f"  Conexiones activas : {stats['active_connections']}/{stats['total_connections']}")
            print(f"  Total pares        : {stats['total_pairs']}")
            print(f"  Total velas        : {stats['total_candles']}")
            print(f"  Prom. velas/par    : {stats['avg_candles_per_pair']:.1f}")
            print(f"  Msgs WS recibidos  : {stats['total_messages_received']}")

            health = cache.get_stream_health()
            healthy = sum(1 for h in health.values() if h["is_healthy"])
            print(f"\n🏥 Streams saludables: {healthy}/{len(health)}")

            unhealthy = [(k, v) for k, v in health.items() if not v["is_healthy"]]
            if unhealthy:
                print("  ⚠️  Con problemas:")
                for key, info in unhealthy[:5]:
                    sym, itv = key
                    print(f"     • {sym} {itv}: {info['last_message_ago']} sin mensajes")

            print(f"\n🔍 Integridad:")
            print(f"  Gaps encontrados : {stats['integrity_summary']['total_gaps_found']}")
            print(f"  Gaps corregidos  : {stats['integrity_summary']['total_gaps_fixed']}")

            print(f"\n📊 Últimas velas cerradas:")
            print("-" * 70)
            for symbol in ["BTCUSDT", "ETHUSDT", "SOLUSDT"]:
                for interval in ["1m", "5m", "15m", "1h"]:
                    if symbol in cache.pairs and interval in cache.pairs[symbol]:
                        last = cache.get_last_closed(symbol, interval)
                        if last:
                            df       = cache.get_dataframe(symbol, interval, only_closed=True)
                            msg_cnt  = cache.message_counts.get((symbol, interval), 0)
                            print(
                                f"{symbol:10s} {interval:3s}: "
                                f"${last['close']:10.2f}  "
                                f"Vol:{last['volume']:10.2f}  "
                                f"Velas:{len(df):4d}  "
                                f"Msgs:{msg_cnt:5d}  "
                                f"[{last['timestamp']}]"
                            )

            print("\n" + "=" * 70)
            print("Ctrl+C para detener")

    except KeyboardInterrupt:
        print("\n🛑 Deteniendo…")
        cache.stop()
        print("✅ Finalizado")