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
    - Monitor de reloj (_candle_close_monitor): cierra velas por close_time,
      independientemente de si Binance envió x=true. Resuelve el problema
      de pares ilíquidos donde Binance omite el mensaje de cierre.
    - REST periódico MÍNIMO: solo descarga las velas que cerraron desde
      el último dato conocido (1–10 velas por intervalo, no 1500).
    - Scheduler independiente por (symbol, interval), alineado al cierre
      de cada vela.
    - Verificación de integridad y auto-corrección de gaps.
    - Detección de streams silenciosos y reconexión automática.
    - Conexiones WebSocket multiplexadas.
    """

    BASE_WS_URL   = "wss://fstream.binance.com/stream"
    BASE_REST_URL = "https://fapi.binance.com"

    # Margen de seguridad (velas extra) al pedir el refresh periódico
    REFRESH_SAFETY_BUFFER = 5

    # Segundos de gracia tras el cierre de vela antes de consultar REST
    CLOSE_GRACE_SECONDS = 2

    # Frecuencia del monitor de reloj (segundos)
    CLOCK_MONITOR_INTERVAL = 1

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
        # --- Pares y configuración general ---
        self.pairs = {
            s.upper(): ([i] if isinstance(i, str) else list(i))
            for s, i in pairs.items()
        }
        self.max_candles            = int(max_candles)
        self.include_open           = bool(include_open_candle)
        self.streams_per_connection = int(streams_per_connection)
        self.backfill_on_start      = bool(backfill_on_start)

        # --- REST ---
        self.rest_limits      = rest_limits or {}
        self.rest_timeout     = float(rest_timeout)
        self.rest_min_sleep   = float(rest_min_sleep)
        self.rest_concurrency = int(rest_concurrency)
        self.rest_retries     = int(rest_retries)
        self.rest_backoff_max = float(rest_backoff_max)
        self._rest            = session or requests.Session()

        # --- Integridad y salud ---
        self.integrity_check_enabled          = bool(integrity_check_enabled)
        self.stream_silence_threshold_seconds = int(stream_silence_threshold_seconds)
        self.stream_health_check_seconds      = int(stream_health_check_seconds)

        # --- Buffers ---
        # buffer[(symbol, interval)] → deque de dicts kline, ordenado por open_time.
        #
        # VENTANA DESLIZANTE FIFO con maxlen=max_candles:
        #   Al hacer buf.append(new_candle) cuando len(buf)==max_candles, Python
        #   descarta automáticamente buf[0] (la más antigua). No hay corrupción
        #   siempre que se opere por open_time (nunca por índice numérico en una
        #   deque que puede mutar).
        self.buffers: Dict[Tuple[str, str], deque] = defaultdict(
            lambda: deque(maxlen=self.max_candles)
        )
        self.lock = threading.Lock()

        # --- Tracking de refresh y mensajes ---
        self.last_refresh_time: Dict[Tuple[str, str], float] = {}
        self.last_message_time: Dict[Tuple[str, str], float] = {}
        self.message_counts: Dict[Tuple[str, str], int]      = defaultdict(int)

        # Estadísticas del monitor de reloj
        self.clock_closes: Dict[Tuple[str, str], int] = defaultdict(int)

        # --- Estadísticas de integridad ---
        self.integrity_stats = defaultdict(lambda: {
            "last_check":        None,
            "gaps_found":        0,
            "gaps_fixed":        0,
            "total_candles":     0,
            "expected_candles":  0,
            "last_error":        None,
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
        self.stream_mapping:    Dict[str, Tuple[str, str]] = {}
        self.subscribed_streams: set = set()
        self._refresh_groups:   List[List[Tuple[str, str]]] = []

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

    def _parse_kline_row(
        self,
        k: list,
        symbol: str,
        interval: str,
        is_closed: bool,
    ) -> dict:
        """Parsea una fila de la API REST de klines a dict interno."""
        return {
            "open_time":              int(k[0]),
            "close_time":             int(k[6]),
            "symbol":                 symbol.upper(),
            "interval":               interval,
            "open":                   float(k[1]),
            "high":                   float(k[2]),
            "low":                    float(k[3]),
            "close":                  float(k[4]),
            "volume":                 float(k[5]),
            "quote_volume":           float(k[7]),
            "trades":                 int(k[8]),
            "taker_buy_volume":       float(k[9]),
            "taker_buy_quote_volume": float(k[10]),
            "is_closed":              is_closed,
        }

    def _upsert_rows_into_buffer(
        self,
        key: Tuple[str, str],
        rows: List[dict],
    ) -> None:
        """
        Inserta o actualiza filas en el buffer de forma ordenada.

        Opera sobre un dict {open_time: row} en memoria para el merge,
        luego vuelca al buffer de una sola vez ya ordenado. Esto evita
        cualquier uso de índices numéricos sobre una deque que puede mutar
        durante el bucle (lo que causaría corrupción silenciosa).

        Uso: SOLO para datos REST (backfill, smart-refresh, fix-gaps).
        El WebSocket usa upsert directo (buf[-1] = row / buf.append(row))
        para mayor eficiencia.
        """
        if not rows:
            return
        with self.lock:
            buf = self.buffers[key]

            # Merge por open_time
            merged: dict = {r["open_time"]: r for r in buf}
            for row in rows:
                merged[row["open_time"]] = row

            sorted_rows = sorted(merged.values(), key=lambda x: x["open_time"])
            if len(sorted_rows) > self.max_candles:
                sorted_rows = sorted_rows[-self.max_candles:]

            buf.clear()
            buf.extend(sorted_rows)

    # =========================================================================
    # MONITOR DE RELOJ — cierre duro de velas por close_time
    # =========================================================================

    async def _candle_close_monitor(self) -> None:
        """
        Cierra velas por RELOJ cada CLOCK_MONITOR_INTERVAL segundos.

        Filosofía:
          El WebSocket envía x=true cuando Binance decide cerrar la vela.
          Pero para pares ilíquidos (poco volumen, pocos trades) Binance
          puede tardar o directamente omitir ese mensaje.

          Este monitor es la fuente AUTORITATIVA de cierre:
            · Si close_time < now_ms → la vela DEBE estar cerrada.
            · No importa si Binance envió x=true o no.
            · Se ejecuta cada 1 segundo, por lo que el cierre ocurre con
              un retraso máximo de ~1 s respecto al tiempo real.

        Por qué no bastan los checks en _ws_combined_stream:
          El check dentro del WS solo corre cuando llega un nuevo mensaje.
          Un par ilíquido puede no recibir mensajes durante minutos, por lo
          que la vela permanecería marcada como abierta indefinidamente.
          Este monitor corre aunque el WS esté en silencio.
        """
        print(
            f"⏰ Monitor de reloj iniciado "
            f"(cierre duro cada {self.CLOCK_MONITOR_INTERVAL}s)"
        )

        while self._running:
            try:
                await asyncio.sleep(self.CLOCK_MONITOR_INTERVAL)
                if not self._running:
                    break

                now_ms = int(time.time() * 1000)

                with self.lock:
                    for key, buf in self.buffers.items():
                        if not buf:
                            continue

                        # Solo la última vela puede estar abierta.
                        # Las anteriores ya fueron cerradas en iteraciones previas
                        # o vinieron cerradas del REST.
                        last = buf[-1]
                        if last.get("is_closed", False):
                            continue

                        close_time = last.get("close_time", 0)
                        if close_time > 0 and close_time < now_ms:
                            # Cierre duro: el período de esta vela ya terminó
                            closed = dict(last)
                            closed["is_closed"] = True
                            buf[-1] = closed
                            self.clock_closes[key] += 1

            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"❌ Error en monitor de reloj: {e}")
                await asyncio.sleep(5)

    # =========================================================================
    # INTEGRIDAD
    # =========================================================================

    def _check_integrity(self, symbol: str, interval: str) -> dict:
        """
        Analiza el buffer de (symbol, interval) en busca de gaps y duplicados.
        Retorna un dict con el resultado del análisis.
        """
        key = (symbol.upper(), interval)
        with self.lock:
            buf = sorted(
                list(self.buffers.get(key, deque())),
                key=lambda x: x["open_time"],
            )

        if len(buf) < 2:
            return {
                "has_gaps":         False,
                "gaps":             [],
                "total_candles":    len(buf),
                "expected_candles": len(buf),
                "duplicates":       [],
                "error":            None,
            }

        interval_ms   = self._get_interval_milliseconds(interval)
        gaps: list    = []
        duplicates: list = []

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
            "has_gaps":         len(gaps) > 0,
            "gaps":             gaps,
            "total_candles":    len(buf),
            "expected_candles": expected_candles,
            "duplicates":       duplicates,
            "error":            None,
        }

        with self.lock:
            self.integrity_stats[key].update({
                "last_check":        datetime.now(),
                "gaps_found":        len(gaps),
                "total_candles":     len(buf),
                "expected_candles":  expected_candles,
            })

        return result

    def _fix_gaps(
        self,
        symbol: str,
        interval: str,
        integrity_result: dict,
    ) -> bool:
        """
        Corrige gaps descargando por REST SOLO las velas del rango faltante.
        Retorna True si se corrigieron todos los gaps.
        """
        if not integrity_result["has_gaps"]:
            return True

        key         = (symbol.upper(), interval)
        interval_ms = self._get_interval_milliseconds(interval)
        fixed_count = 0

        for gap in integrity_result["gaps"]:
            try:
                start_time = gap["after_timestamp"] + interval_ms
                end_time   = gap["before_timestamp"]
                limit      = min(gap["missing_candles"] + 2, 1500)

                params = {
                    "symbol":    symbol.upper(),
                    "interval":  interval,
                    "startTime": start_time,
                    "endTime":   end_time,
                    "limit":     limit,
                }
                resp = self._rest.get(
                    f"{self.BASE_REST_URL}/fapi/v1/klines",
                    params=params,
                    timeout=self.rest_timeout,
                )
                resp.raise_for_status()
                data = resp.json()

                rows = []
                for k in data:
                    try:
                        rows.append(
                            self._parse_kline_row(k, symbol, interval, is_closed=True)
                        )
                    except Exception:
                        continue

                self._upsert_rows_into_buffer(key, rows)

                if rows:
                    fixed_count += 1
                    print(
                        f"✅ Gap corregido {symbol} {interval}: "
                        f"{len(rows)} velas insertadas"
                    )
                else:
                    print(
                        f"⚠️  Gap sin datos {symbol} {interval}: "
                        f"REST no devolvió velas"
                    )
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
        """
        key   = (symbol.upper(), interval)
        limit = int(min(
            self.rest_limits.get(interval, min(1500, self.max_candles)),
            self.max_candles,
        ))
        if limit <= 0:
            return

        params = {
            "symbol":   symbol.upper(),
            "interval": interval,
            "limit":    limit,
        }

        async with sem:
            try:
                data = await self._async_fetch_with_retries(
                    session, f"{self.BASE_REST_URL}/fapi/v1/klines", params
                )
            except Exception as e:
                print(f"🔴 Backfill REST falló {symbol} {interval}: {e}")
                return

        if not data:
            print(f"⚠️  Sin datos para {symbol} {interval}")
            return

        now_ms = int(time.time() * 1000)
        rows   = []
        for k in data:
            try:
                # Una vela cuyo close_time ya pasó está cerrada,
                # independientemente de lo que Binance indique.
                is_closed = int(k[6]) < now_ms
                rows.append(self._parse_kline_row(k, symbol, interval, is_closed))
            except Exception:
                continue

        self._upsert_rows_into_buffer(key, rows)
        self.last_refresh_time[key] = time.time()
        await asyncio.sleep(self.rest_min_sleep)
        print(f"✅ Backfill {symbol} {interval}: {len(rows)} velas descargadas")

    async def _async_backfill_all(self) -> None:
        """Backfill concurrente para todos los pares."""
        pairs_list = [
            (symbol, interval)
            for symbol, intervals in self.pairs.items()
            for interval in intervals
        ]
        total = len(pairs_list)
        print(
            f"📥 Backfill inicial: {total} pares "
            f"(concurrencia={self.rest_concurrency})…"
        )

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
        self,
        symbol: str,
        interval: str,
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
            return (min(1500, self.max_candles), None)

        now_ms = int(time.time() * 1000)

        # Una vela está cerrada si:
        #   a) is_closed=True (marcada por WS x=true o por el monitor de reloj), o
        #   b) close_time < now_ms (el período ya terminó aunque ninguno lo marcó)
        closed = [
            r for r in buf
            if r.get("is_closed", False)
            or r.get("close_time", now_ms + 1) < now_ms
        ]
        if not closed:
            return (min(1500, self.max_candles), None)

        last_closed_ot      = max(r["open_time"] for r in closed)
        next_expected_ot_ms = last_closed_ot + interval_ms

        if now_ms < next_expected_ot_ms:
            return (0, None)

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
        """
        smart_limit, start_time_ms = self._calc_smart_limit(symbol, interval)

        if smart_limit == 0:
            return

        params: dict = {
            "symbol":   symbol.upper(),
            "interval": interval,
            "limit":    smart_limit,
        }
        if start_time_ms is not None:
            params["startTime"] = start_time_ms

        async with sem:
            try:
                data = await self._async_fetch_with_retries(
                    session, f"{self.BASE_REST_URL}/fapi/v1/klines", params
                )
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

    async def _periodic_safety_refresh(
        self,
        interval_seconds: int = 300,
        inter_group_delay_seconds: int = 60,
    ) -> None:
        """
        Red de seguridad que corre cada `interval_seconds` (default: 300 s).

        · El WS + monitor de reloj cubren el 99 % de los casos.
        · Este task cubre: reconexiones WS que dejaron un hueco, o
          fallos completos del WS durante varios minutos.
        · Para cada par llama a _calc_smart_limit; si devuelve 0 → nada que hacer.
        · TURNOS: los pares se procesan grupo a grupo con pausa entre grupos
          para no superar los rate-limits de Binance.
        """
        n_groups = len(self._refresh_groups)
        total    = sum(len(g) for g in self._refresh_groups)
        print(
            f"🛡  Safety refresh iniciado — período={interval_seconds}s, "
            f"{n_groups} grupos ({total} pares), "
            f"pausa entre grupos={inter_group_delay_seconds}s"
        )

        await asyncio.sleep(30)  # Dar tiempo al backfill y al WS de estabilizarse

        while self._running:
            try:
                await asyncio.sleep(interval_seconds)
                if not self._running:
                    break

                groups = self._refresh_groups
                if not groups:
                    continue

                print(
                    f"🛡  Safety refresh — {len(groups)} grupos, "
                    f"{sum(len(g) for g in groups)} pares totales"
                )

                for g_idx, group in enumerate(groups, 1):
                    if not self._running:
                        break

                    if g_idx > 1:
                        print(
                            f"⏳ Safety refresh: esperando {inter_group_delay_seconds}s "
                            f"antes del grupo {g_idx}/{len(groups)}…"
                        )
                        await asyncio.sleep(inter_group_delay_seconds)
                        if not self._running:
                            break

                    needs_refresh = [
                        (sym, itv)
                        for sym, itv in group
                        if self._calc_smart_limit(sym, itv)[0] > 0
                    ]

                    if not needs_refresh:
                        print(
                            f"🛡  Grupo {g_idx}/{len(groups)}: "
                            f"WS+reloj al día — 0/{len(group)} pares necesitan REST"
                        )
                        continue

                    print(
                        f"🛡  Grupo {g_idx}/{len(groups)}: "
                        f"{len(needs_refresh)}/{len(group)} pares con velas "
                        f"no confirmadas → descargando via REST…"
                    )

                    connector = aiohttp.TCPConnector(limit=self.rest_concurrency * 2)
                    sem       = asyncio.Semaphore(self.rest_concurrency)
                    async with aiohttp.ClientSession(connector=connector) as session:
                        tasks   = [
                            self._smart_refresh_symbol_interval(session, sem, sym, itv)
                            for sym, itv in needs_refresh
                        ]
                        results = await asyncio.gather(*tasks, return_exceptions=True)
                        for r in results:
                            if isinstance(r, Exception):
                                print(f"❌ Error en safety refresh grupo {g_idx}: {r}")

                    if self.integrity_check_enabled:
                        for sym, itv in needs_refresh:
                            try:
                                integrity = self._check_integrity(sym, itv)
                                if integrity["has_gaps"]:
                                    print(
                                        f"⚠️  Gap detectado {sym} {itv}: "
                                        f"{len(integrity['gaps'])} gaps → reparando…"
                                    )
                                    self._fix_gaps(sym, itv, integrity)
                            except Exception as e:
                                print(
                                    f"❌ Error verificando integridad {sym} {itv}: {e}"
                                )

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

        self._refresh_groups = [
            [self.stream_mapping[s] for s in group]
            for group in groups
        ]
        return groups

    def _build_ws_row(self, k: dict, is_closed: bool) -> dict:
        """
        Construye el dict interno de una vela a partir del payload WS de Binance.

        Campos del payload k:
          t → open_time (ms)    T → close_time (ms)
          s → symbol            i → interval
          o / h / l / c → OHLC  v → volume
          q → quote_volume      n → trades
          V → taker_buy_volume  Q → taker_buy_quote_volume
          x → is_closed (x=true: Binance confirma cierre)
        """
        return {
            "open_time":              int(k["t"]),
            "close_time":             int(k["T"]),
            "symbol":                 str(k["s"]).upper(),
            "interval":               str(k["i"]),
            "open":                   float(k["o"]),
            "high":                   float(k["h"]),
            "low":                    float(k["l"]),
            "close":                  float(k["c"]),
            "volume":                 float(k["v"]),
            "quote_volume":           float(k["q"]),
            "trades":                 int(k["n"]),
            "taker_buy_volume":       float(k["V"]),
            "taker_buy_quote_volume": float(k["Q"]),
            "is_closed":              is_closed,
        }

    def _handle_ws_kline(self, symbol: str, interval: str, k: dict) -> None:
        """
        Aplica un tick de WebSocket al buffer de (symbol, interval).

        Ciclo de vida de una vela via WS:
        ──────────────────────────────────
          Tick 1..N  (x=false): actualización de la vela ABIERTA.
                                Se sobreescribe buf[-1] in-place si el
                                open_time coincide. El deque NO crece.

          Tick final (x=true) : Binance confirma el cierre.
                                Se sobreescribe buf[-1] con is_closed=True.
                                El deque NO crece aún.

          Primer tick de la    : Llega un row con open_time DISTINTO al de
          siguiente vela         buf[-1]. Aquí el deque crece en 1 (y si
                                 estaba en maxlen, el elemento más antiguo
                                 cae automáticamente — ventana deslizante).

        Nota: el _candle_close_monitor es la fuente autoritativa de cierre.
              x=true del WS es una señal adicional bienvenida, pero no
              necesaria. Si nunca llega (par ilíquido), el monitor cerrará
              la vela por reloj a los ≤1 s de que close_time pase.
        """
        now_ms    = int(time.time() * 1000)
        is_closed = bool(k.get("x", False))

        # Cierre duro por reloj: si close_time ya pasó, la vela es cerrada
        # independientemente de lo que diga el flag x.
        close_time = int(k.get("T", 0))
        if close_time > 0 and close_time < now_ms:
            is_closed = True

        row = self._build_ws_row(k, is_closed)

        # Si la vela está abierta y no queremos incluir velas abiertas → ignorar.
        # Pero si is_closed=True siempre la procesamos (para actualizar el buffer).
        if not is_closed and not self.include_open:
            return

        key          = (symbol, interval)
        needs_upsert = False   # inicializar siempre; solo Caso B-desordenado lo pone True

        with self.lock:
            buf = self.buffers[key]

            if buf and buf[-1]["open_time"] == row["open_time"]:
                # ─────────────────────────────────────────────────────────
                # CASO A — Mismo período: actualización in-place.
                #
                # La vela en curso recibe un nuevo tick (precio, volumen, etc.)
                # o Binance confirma su cierre (x=true).
                # No se añade un elemento nuevo: el deque no crece.
                # ─────────────────────────────────────────────────────────
                buf[-1] = row

            else:
                # ─────────────────────────────────────────────────────────
                # CASO B — Período nuevo: la vela anterior terminó y empieza
                #          una nueva.
                #
                # 1. Cerrar la vela anterior por reloj si no estaba cerrada.
                #    Esto es el "cierre forzado" para pares ilíquidos donde
                #    Binance nunca envió x=true para esa vela.
                # ─────────────────────────────────────────────────────────
                if buf:
                    prev = buf[-1]
                    if not prev.get("is_closed", False):
                        prev_close = prev.get("close_time", now_ms + 1)
                        if prev_close < now_ms:
                            # El período de la vela anterior ya terminó → cerrar.
                            closed_prev = dict(prev)
                            closed_prev["is_closed"] = True
                            buf[-1] = closed_prev

                # 2. Manejar velas desordenadas (reconexión WS).
                if buf and row["open_time"] < buf[-1]["open_time"]:
                    # Vela del pasado que llega tarde → upsert con reordenamiento.
                    # Liberar el lock antes de llamar a _upsert_rows_into_buffer
                    # para evitar re-entrancy (ese método adquiere self.lock).
                    needs_upsert = True
                else:
                    needs_upsert = False
                    # ─────────────────────────────────────────────────────
                    # CASO B normal: append de la nueva vela.
                    # Si buf.maxlen == len(buf), Python descarta buf[0]
                    # automáticamente (ventana deslizante FIFO).
                    # ─────────────────────────────────────────────────────
                    buf.append(row)

        # Upsert fuera del lock (para evitar deadlock)
        if needs_upsert:
            self._upsert_rows_into_buffer(key, [row])

    async def _ws_combined_stream(
        self,
        stream_names: List[str],
        group_id: int,
    ) -> None:
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
                    print(
                        f"✅ WS conectado: {group_name} "
                        f"({len(stream_names)} streams)"
                    )
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
                            k = kline_data.get("k", {})

                            # Tracking de mensajes
                            key = (symbol, interval)
                            self.last_message_time[key] = time.time()
                            self.message_counts[key]   += 1

                            # Delegar todo el manejo al método dedicado.
                            # _handle_ws_kline encapsula la lógica de ciclo de
                            # vida de la vela (update in-place, cierre por reloj,
                            # append de nueva vela, reordenamiento).
                            self._handle_ws_kline(symbol, interval, k)

                            if messages_received % 500 == 0:
                                print(
                                    f"📨 {group_name}: "
                                    f"{messages_received} msgs procesados"
                                )

                        except asyncio.TimeoutError:
                            await ws.ping()
                            last_ping = time.time()

                        except websockets.ConnectionClosed as e:
                            print(f"🔶 WS cerrado {group_name}: {e}")
                            raise

                        if time.time() - last_ping > 30:
                            await ws.ping()
                            last_ping = time.time()

            except asyncio.CancelledError:
                break
            except Exception as e:
                consecutive_errors += 1
                self.connection_stats[group_name]["reconnects"] += 1
                self.connection_stats[group_name]["last_error"]  = str(e)
                self.connection_stats[group_name]["active"]      = False

                reconnect_delay = min(reconnect_delay * 1.5, 30.0)
                if consecutive_errors > 5:
                    reconnect_delay = 60.0

                print(f"🔴 Error {group_name}: {e}")
                print(
                    f"   Reconectando en {reconnect_delay:.1f}s "
                    f"(intento {consecutive_errors})"
                )
                await asyncio.sleep(reconnect_delay)

        self.connection_stats[group_name]["active"] = False

    # =========================================================================
    # MONITOR DE SALUD DE STREAMS
    # =========================================================================

    async def _stream_health_monitor(self) -> None:
        """
        Monitorea streams silenciosos y cancela la tarea WS del grupo afectado
        para forzar reconexión.
        """
        print(
            f"🏥 Monitor de salud iniciado "
            f"(check cada {self.stream_health_check_seconds}s, "
            f"umbral={self.stream_silence_threshold_seconds}s)"
        )
        await asyncio.sleep(30)

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
                    and (current_time - self.last_message_time[(sym, itv)])
                    > self.stream_silence_threshold_seconds
                ]

                if not silent:
                    continue

                print(f"\n⚠️  Streams silenciosos detectados ({len(silent)}):")
                for sym, itv, duration in silent:
                    cnt = self.message_counts.get((sym, itv), 0)
                    print(
                        f"   • {sym} {itv}: sin msgs por {duration:.0f}s "
                        f"(total: {cnt})"
                    )

                stream_names_silent = {
                    name
                    for name, (sym, itv) in self.stream_mapping.items()
                    if any(sym == s and itv == i for s, i, _ in silent)
                }

                groups_to_restart: set = set()
                for group_name, stats in self.connection_stats.items():
                    if any(s in stats.get("streams", []) for s in stream_names_silent):
                        try:
                            gid = int(group_name.split("_")[1])
                            groups_to_restart.add(gid)
                        except (IndexError, ValueError):
                            pass

                for gid in groups_to_restart:
                    old_task        = self._tasks.get(("stream", gid))
                    group_name_gid  = f"group_{gid}"
                    stream_names_gid = self.connection_stats[group_name_gid].get("streams", [])

                    if not stream_names_gid:
                        print(f"   ⚠️  Grupo {gid}: sin streams registrados, skip")
                        continue

                    if old_task and not old_task.done():
                        print(f"   🔄 Cancelando grupo {gid} para forzar reconexión…")
                        old_task.cancel()
                        await asyncio.sleep(1.0)

                    # Crear un task nuevo con los mismos streams.
                    # Sin esto el stream muere permanentemente: CancelledError
                    # es capturado por el 'break' del outer loop en _ws_combined_stream.
                    if self._running:
                        print(f"   🆕 Relanzando grupo {gid} ({len(stream_names_gid)} streams)…")
                        new_task = asyncio.ensure_future(
                            self._ws_combined_stream(stream_names_gid, gid)
                        )
                        self._tasks[("stream", gid)] = new_task

            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"❌ Error en monitor de salud: {e}")
                await asyncio.sleep(60)

    async def _monitor_connections(self) -> None:
        """Monitor básico de conexiones activas (log cada 60 s)."""
        while self._running:
            await asyncio.sleep(60)
            active = sum(
                1 for s in self.connection_stats.values() if s.get("active", False)
            )
            total_clock_closes = sum(self.clock_closes.values())
            print(
                f"🔌 Conexiones WS activas: {active}/{len(self.connection_stats)} "
                f"| Cierres por reloj acumulados: {total_clock_closes}"
            )

    # =========================================================================
    # CICLO DE VIDA
    # =========================================================================

    def start(self) -> None:
        """Inicia el sistema: loop asyncio, backfill, WebSocket y monitores."""
        print("\n" + "=" * 70)
        print("🚀 INICIANDO KlineWebSocketCache")
        print("=" * 70)

        self._running = True

        loop = asyncio.new_event_loop()
        self._loop = loop

        def run_loop():
            asyncio.set_event_loop(loop)
            loop.run_forever()

        thread = threading.Thread(
            target=run_loop, daemon=True, name="KlineWSLoop"
        )
        thread.start()
        self._thread = thread

        async def _startup_sequence():
            if self.backfill_on_start:
                await self._async_backfill_all()

            print("\n" + "=" * 70)
            print("📊 CREANDO GRUPOS DE STREAMS")
            print("=" * 70)
            stream_groups_local = self._create_stream_groups()
            print(f"\n📊 {len(stream_groups_local)} conexiones WebSocket")
            print("=" * 70)

            # ── WebSocket por grupo ──────────────────────────────────────
            for idx, group in enumerate(stream_groups_local, 1):
                task = asyncio.ensure_future(self._ws_combined_stream(group, idx))
                self._tasks[("stream", idx)] = task

            # ── Monitor de reloj (cierre duro por close_time) ────────────
            asyncio.ensure_future(self._candle_close_monitor())

            # ── Safety refresh REST ──────────────────────────────────────
            asyncio.ensure_future(
                self._periodic_safety_refresh(
                    interval_seconds=300,
                    inter_group_delay_seconds=60,
                )
            )

            # ── Monitores auxiliares ─────────────────────────────────────
            asyncio.ensure_future(self._stream_health_monitor())
            asyncio.ensure_future(self._monitor_connections())

            n_groups = len(stream_groups_local)
            print(f"\n✅ KlineWebSocketCache iniciado")
            print(f"   • Fuente primaria   : WebSocket tick-a-tick")
            print(f"   • Cierre de velas   : Monitor de reloj (close_time, cada 1s)")
            print(f"   • Safety refresh    : cada 300s, {n_groups} grupos (60s entre grupos)")
            print(f"   • REST periódico    : solo velas no cubiertas por WS/reloj")
            print(f"   • Monitor de salud  : cada {self.stream_health_check_seconds}s")
            print(f"   • Integridad        : {'✅' if self.integrity_check_enabled else '❌'}")
            print(f"   • Buffer            : ventana deslizante {self.max_candles} velas/par")
            print("=" * 70 + "\n")

        print("⏳ Arrancando secuencia de inicio (backfill → WS → monitores)…")
        asyncio.run_coroutine_threadsafe(_startup_sequence(), loop)

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
        """Fuerza un refresh inmediato. Si symbol/interval son None, refresca todos."""
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
        """
        Devuelve un DataFrame con las velas acumuladas.

        Nota sobre el conteo de filas:
          Con include_open=True el buffer contiene hasta max_candles velas
          (1499 cerradas + 1 abierta = 1500 con max_candles=1500).
          only_closed=True devuelve 1499 filas. Esto es correcto: la vela
          abierta está en el buffer pero no se incluye en el filtro.
        """
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

    def get_last_closed(
        self,
        symbol: str,
        interval: str,
    ) -> Optional[dict]:
        """Devuelve la última vela cerrada disponible."""
        df = self.get_dataframe(symbol, interval, only_closed=True)
        if df.empty:
            return None
        return df.iloc[-1].to_dict()

    def check_all_integrity(self) -> Dict[Tuple[str, str], dict]:
        """Verifica la integridad de todos los pares y retorna el resultado."""
        return {
            (symbol, interval): self._check_integrity(symbol, interval)
            for symbol, intervals in self.pairs.items()
            for interval in intervals
        }

    def get_stream_health(self) -> dict:
        """Retorna información de salud de cada stream suscrito."""
        current_time = time.time()
        return {
            key: {
                "last_message_time": self.last_message_time.get(key, 0),
                "message_count":     self.message_counts.get(key, 0),
                "clock_closes":      self.clock_closes.get(key, 0),
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

    def get_stats(self) -> dict:
        """Estadísticas completas del sistema."""
        with self.lock:
            total_pairs     = len(self.buffers)
            total_candles   = sum(len(b) for b in self.buffers.values())
            pairs_with_data = sum(1 for b in self.buffers.values() if b)

            integrity_summary = {
                "pairs_checked":    len(self.integrity_stats),
                "total_gaps_found": sum(
                    s["gaps_found"] for s in self.integrity_stats.values()
                ),
                "total_gaps_fixed": sum(
                    s["gaps_fixed"] for s in self.integrity_stats.values()
                ),
            }

        return {
            "total_pairs":             total_pairs,
            "pairs_with_data":         pairs_with_data,
            "total_candles":           total_candles,
            "avg_candles_per_pair":    total_candles / max(pairs_with_data, 1),
            "connection_stats":        dict(self.connection_stats),
            "streams_per_connection":  self.streams_per_connection,
            "total_connections":       len([k for k in self._tasks if k[0] == "stream"]),
            "active_connections":      sum(
                1 for s in self.connection_stats.values() if s.get("active", False)
            ),
            "integrity_check_enabled": self.integrity_check_enabled,
            "integrity_summary":       integrity_summary,
            "total_messages_received": sum(self.message_counts.values()),
            "total_clock_closes":      sum(self.clock_closes.values()),
        }


