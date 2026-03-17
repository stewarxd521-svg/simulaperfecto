import requests
import threading
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import time
import urllib.parse
import hmac
import hashlib
import logging
from typing import Optional, Dict, List, Tuple, Set
import threading
from collections import defaultdict, OrderedDict, deque
from queue import Queue, Empty, Full
from dataclasses import dataclass, field
from enum import Enum
import json
import asyncio
import aiohttp
import websockets
import warnings
from pathlib import Path
import os
import tempfile
import threading
import time
import logging
import itertools
from queue import PriorityQueue, Full, Empty
from concurrent.futures import ThreadPoolExecutor
from typing import Optional, Dict
from datetime import datetime

logger = logging.getLogger(__name__)


# Import the Binance API wrapper
from binance_api_mejorado import BinanceAPI

# 🆕 IMPORTAR EL WEBSOCKET DE PRECIOS
from WS import SymbolWebSocketPriceCache
from KlineWEBSOCKETTposibleMejora import KlineWebSocketCache  # tu archivo

# Suppress warnings
warnings.filterwarnings('ignore')

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ==================== PARÁMETROS DE ESTRATEGIA HEIKIN ASHI ====================
EMA_PERIOD = 20  # Período de la EMA
EMA_PERIOD_BTC= 200
FEE_RATE = 0.0005
MAX_CONCURRENT_TRADES = 50  # Máximo de operaciones simultáneas
BB_LOOKBACK_CANDLES = 0   # 2 = más señales, 3 = más filtrado
MOMENTUM_EXIT_ENABLED  = True
MOMENTUM_EXIT_ROI_MAX  = -0.5    # Solo actúa si ROI < -0.5%

# 🆕 SISTEMA DE RECUPERACIÓN MEJORADO
ROI_CRITICAL_LOSS = -2.5  # ROI crítico para activar recuperación (más temprano)
ROI_CRITICAL_PROFIT = 18.0  # ROI positivo para considerar "recuperado" (más bajo)
ROI_RECOVERY_TARGET_1 = -3.0  # Primera meta de recuperación
ROI_RECOVERY_TARGET_2 = -1.0  # Segunda meta (opcional)
ROI_RECOVERY_TARGET_FINAL = 0.5  # Meta final (break-even + algo)
RECOVERY_MODE_ENABLED = True  # Activar/desactivar recuperación
MAX_RECOVERY_ATTEMPTS = 2  # Máximo intentos de recuperación por trade
RECOVERY_POSITION_SIZE_MULTIPLIER = 0.6  # 60% del tamaño original

PROTECT_ROI_THRESHOLD = 5.0   # umbral para empezar a proteger con ROI
PROTECT_DISTANCE = 4.0        # distancia en puntos porcentuales que queremos mantener

# 🔥 NUEVOS PARÁMETROS DE PYRAMIDING (ESCALADO DE POSICIONES)
PYRAMIDING_ENABLED = True  # Activar/desactivar pyramiding
PYRAMIDING_ROI_LEVELS = [3.0, 5.0, 7.0, 10.0, 12.0, 15.0, 20.0, 25.0]  # Niveles de ROI para escalar
PYRAMIDING_MULTIPLIER = 1.0  # Multiplicador de tamaño (1.0 = mismo tamaño, 0.5 = mitad)
# ---- Parámetros martingala / add-on ----
MARTINGALE_ENABLED = True
MARTINGALE_MULTIPLIER = 1.5        # cuánto multiplicar la cantidad original (1.5 = +50%)
MARTINGALE_MAX_ADDS = 2           # cuántos adds permitidos (1 = solo 1 add, 2 = hasta 2 adds)
MARTINGALE_MAX_TOTAL_MULT = 4.0   # tope absoluto de multiplicador con respecto a original (ej: 4x)
EMATOUCH_TOLERANCE = 0.002        # tolerancia para considerar "touch" a EMA200 (0.2%)

# ----------------- Parámetros para bloqueo por historial de pérdidas -----------------
LOSS_HISTORY_LEN = 10           # cuántos trades cerrados mirar hacia atrás
LOSS_ROI_THRESHOLD = ROI_CRITICAL_LOSS      # umbral (ej: -8.0 significa pérdidas >= 8%)
LOSS_MIN_COUNT = 1             # cuántas pérdidas que cumplan el umbral
SYMBOL_INVERT_TTL_SECONDS = 1800  # (opcional) tiempo que mantenemos la inversión por símbolo (30min)
# ------------------------------------------------------------------------------------



# ----------------- Cierre por ROI para PYRAMIDING -----------------
PYRAMIDING_CLOSE_ON_ROI_ENABLED = True   # True = cerrar automáticamente trades con pyramiding cuando alcanzan el ROI
PYRAMIDING_CLOSE_ROI = -0.0               # Umbral por defecto en % (ajústalo a lo que quieras)
# ------------------------------------------------------------------

# ==================== PYRAMIDING INVERSO (NEGATIVOS) ====================
INVERSE_PYRAMIDING_ENABLED = True            # activar pyramiding inverso
INVERSE_PYRAMIDING_NEG_ROI_LEVELS = [-3.0, -5.0, -7.0, -10.0, -12.0]  # niveles negativos (en %)
INVERSE_PYRAMIDING_MAX_ADDS = 1              # máximo de pyramidings efectivamente permitidos por trade (tu requisito)
INVERSE_PYRAMIDING_ABORT_LEVEL = 5           # si llega a este número (niveles totales), aceptar pérdida y cerrar
INVERSE_PYRAMIDING_MULTIPLIER = 1.0          # multiplicador de tamaño para cada add (puedes ajustar)
INVERSE_PYRAMIDING_MINIMUM_ROI_TO_TRIGGER = -0.5  # no disparar para micro-pérdidas (ejemplo)

# ---- Nuevas opciones para acelerar Adds a partir de un nivel ----
# A partir de este nivel (4 = el 4º add) empezamos a "acelerar" la cantidad.
INVERSE_PYRAMIDING_ACCELERATION_START_LEVEL = 4     # nivel desde el que aplicamos el x3
INVERSE_PYRAMIDING_ACCELERATION_MULTIPLIER = 3.0    # multiplicador base (x3)
INVERSE_PYRAMIDING_ACCELERATION_MAX_MULT = 10.0     # tope absoluto del multiplicador para seguridad


# ==================== PARÁMETROS DEL EXECUTOR DE ÓRDENES ====================
# Prioridades (menor = mayor prioridad)
_PRIORITY = {
    'CLOSE_POSITION': 0,      # máxima prioridad
    'UPDATE_TP_SL':   2,
    'OPEN_POSITION':  0,
    'OPEN_PYRAMIDING': 1,     # ← NUEVO — misma prioridad que OPEN_POSITION
}

# Configurables (ajusta aquí si lo necesitas)
DEFAULT_MAX_WORKERS =50  # solicitado: 15 workers en paralelo
COMMAND_QUEUE_MAX = 2000
MARK_CLOSING_RETRIES = 5
MARK_CLOSING_RETRY_DELAY = 1
API_CALL_TIMEOUT = 10  # referencia, algunas APIs manejan su propio timeout

# Intento de usar FEE_RATE si está definido en el módulo; si no, fallback.
try:
    FEE_RATE  # type: ignore
except Exception:
    FEE_RATE = 0.0007  # default si no existe en tu entorno

# 🆕 STOP-LOSS GLOBAL
MAX_DAILY_LOSS = -100.0  # Máximo de pérdida diaria en USD
EMERGENCY_STOP_ENABLED = True
# 🧮 ROI GLOBAL - NUEVA FUNCIÓN DE CONTROL DE REINICIO
ROI_RESET_THRESHOLD_POSITIVE = 15   # ROI promedio para reinicio por ganancia
ROI_RESET_THRESHOLD_NEGATIVE = -100 # ROI promedio para reinicio por pérdida
GLOBAL_ROI_CHECK_INTERVAL = 5        # segundos entre verificaciones

# ==================== FUNCIONES HEIKIN ASHI ====================
def calculate_williams_r(df: pd.DataFrame, period: int = 300) -> pd.Series:
        """
        Williams %R = (Highest High - Close) / (Highest High - Lowest Low) * -100
        Rango: -100 (sobreventa) a 0 (sobrecompra)
        """
        highest_high = df['high'].rolling(window=period, min_periods=period).max()
        lowest_low   = df['low'].rolling(window=period, min_periods=period).min()
        wr = ((highest_high - df['close']) / (highest_high - lowest_low)) * -100
        return wr

def calculate_heikin_ashi(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convierte velas normales a Heikin Ashi
    
    Fórmulas:
    - HA_Close = (Open + High + Low + Close) / 4
    - HA_Open = (HA_Open[prev] + HA_Close[prev]) / 2
    - HA_High = max(High, HA_Open, HA_Close)
    - HA_Low = min(Low, HA_Open, HA_Close)
    """
    if df is None or df.empty or len(df) < 2:
        return pd.DataFrame()
    
    df = df.copy()
    ha_df = pd.DataFrame()
    
    # Calcular HA_Close
    ha_df['ha_close'] = (df['open'] + df['high'] + df['low'] + df['close']) / 4
    
    # Inicializar HA_Open
    ha_df['ha_open'] = 0.0
    ha_df.loc[0, 'ha_open'] = (df.loc[0, 'open'] + df.loc[0, 'close']) / 2
    
    # Calcular HA_Open para el resto
    for i in range(1, len(df)):
        ha_df.loc[i, 'ha_open'] = (ha_df.loc[i-1, 'ha_open'] + ha_df.loc[i-1, 'ha_close']) / 2
    
    # Calcular HA_High y HA_Low
    ha_df['ha_high'] = df[['high']].join(ha_df[['ha_open', 'ha_close']]).max(axis=1)
    ha_df['ha_low'] = df[['low']].join(ha_df[['ha_open', 'ha_close']]).min(axis=1)
    
    # Mantener timestamp y volume
    ha_df['timestamp'] = df['timestamp']
    ha_df['volume'] = df['volume']
    
    return ha_df[['timestamp', 'ha_open', 'ha_high', 'ha_low', 'ha_close', 'volume']]

def calculate_ema(series: pd.Series, period: int) -> pd.Series:
    """Calcula EMA con período dado"""
    return series.ewm(span=period, adjust=False).mean()

def add_heikin_ashi_indicators(ha_df: pd.DataFrame, ema_period: int = EMA_PERIOD) -> pd.DataFrame:
    """
    Añade EMAs a las velas Heikin Ashi
    - EMA20_high: EMA del HA_High
    - EMA20_low: EMA del HA_Low
    """
    if ha_df is None or ha_df.empty:
        return ha_df
    
    df = ha_df.copy()
    
    # EMAs de High y Low
    df['ema_high'] = calculate_ema(df['ha_high'], ema_period)
    df['ema_low'] = calculate_ema(df['ha_low'], ema_period)
    
    return df

def calculate_momentum(series: pd.Series, period: int = 14) -> pd.Series:
        """
        Momentum clásico: diferencia entre el precio actual y el precio n periodos atrás.
        M = Close(t) - Close(t - n)
        
        También puedes usar la versión normalizada (Rate of Change):
        ROC = (Close(t) - Close(t-n)) / Close(t-n) * 100
        """
        try:
            return series.diff(period)          # versión absoluta
            # return series.pct_change(period) * 100  # versión ROC (%) — descomenta si prefieres
        except Exception as e:
            logger.debug(f"Error en calculate_momentum: {e}")
            return pd.Series(dtype=float)

def check_bearish_momentum(self, current_bar: pd.Series, prev_bar: pd.Series) -> bool:
    """
    BEARISH: momentum cruzó de positivo → negativo
    M = Close(t) - Close(t-n)
    """
    try:
        mom_now  = current_bar.get('momentum', None)
        mom_prev = prev_bar.get('momentum', None)

        if mom_now is None or mom_prev is None:
            return False

        return (mom_prev > 0) and (mom_now < 0)   # cruce por cero hacia abajo

    except Exception as e:
        logger.debug(f"Error en check_bearish_momentum: {e}")
        return False

def check_bullish_momentum(self, current_bar: pd.Series, prev_bar: pd.Series) -> bool:
    """
    BULLISH: momentum cruzó de negativo → positivo
    M = Close(t) - Close(t-n)
    """
    try:
        mom_now  = current_bar.get('momentum', None)
        mom_prev = prev_bar.get('momentum', None)

        if mom_now is None or mom_prev is None:
            return False

        return (mom_prev < 0) and (mom_now > 0)   # cruce por cero hacia arriba

    except Exception as e:
        logger.debug(f"Error en check_bullish_momentum: {e}")
        return False
    
    
# ==================== GESTOR DE ESTADO ====================

class TradeState(Enum):
    """Estados de un trade"""
    PENDING_OPEN = "pending_open"
    OPEN = "open"
    TRAILING = "trailing"  # Nuevo: seguimiento activo con TP/SL dinámico
    PENDING_CLOSE = "pending_close"
    CLOSED = "closed"

@dataclass
class HeikinAshiBar:
    """Barra Heikin Ashi individual"""
    timestamp: datetime
    ha_open: float
    ha_high: float
    ha_low: float
    ha_close: float
    volume: float
    
@dataclass
class PyramidLevel:
    """🔥 NUEVO: Información de cada nivel de pyramiding"""
    level: int  # Nivel del escalado (1, 2, 3, 4, 5)
    roi_threshold: float  # ROI en el que se activó (5%, 10%, etc)
    entry_price: float  # Precio de entrada de este nivel
    quantity: float  # Cantidad añadida en este nivel
    entry_time: datetime  # Momento del escalado
    
@dataclass
class TradeInfo:
    """Información completa de un trade con Heikin Ashi"""
    symbol: str
    trade_type: str  # "LONG" o "SHORT"
    entry_price: float
    entry_time: datetime
    current_tp: float
    current_sl: float
    quantity: float

      # --- NUEVO campo único por trade ---
    trade_id: str = field(default_factory=str)
    
    # Tracking dinámico
    state: TradeState = TradeState.PENDING_OPEN
    last_update: datetime = field(default_factory=datetime.now)
    tp_sl_history: List[Dict] = field(default_factory=list)  # Historial de ajustes
    bars_held: int = 0  # Velas mantenidas
    highest_price: float = 0.0  # Mayor precio alcanzado (para LONG)
    lowest_price: float = float('inf')  # Menor precio alcanzado (para SHORT)

    # 🆕 SISTEMA DE RECUPERACIÓN MEJORADO
    is_recovery_mode: bool = False  # Si está en modo recuperación
    original_entry_price: float = 0.0  # Precio de entrada original
    original_exit_price: float = 0.0  # 🆕 Precio de cierre del trade original
    recovery_triggered_at: Optional[datetime] = None  # Cuándo se activó recuperación
    recovery_attempts: int = 0  # 🆕 Contador de intentos de recuperación
    max_recovery_attempts: int = MAX_RECOVERY_ATTEMPTS  # 🆕 Límite de intentos
    recovery_reason: str = ""  # 🆕 Razón de activación
    last_protected_roi: float = 0.0  # último ROI usado para proteger SL (en %)
    
    # 🔥 NUEVO: Sistema de Pyramiding (Escalado de Posiciones)
    pyramiding_enabled: bool = PYRAMIDING_ENABLED
    pyramid_symbol_enabled: bool = False  # Símbolo del trade (para referencia)
    pyramid_levels: List[PyramidLevel] = field(default_factory=list)  # Niveles de escalado
    original_quantity: float = 0.0  # Cantidad original (primer entrada)
    total_quantity: float = 0.0  # Cantidad total acumulada
    weighted_avg_price: float = 0.0  # Precio promedio ponderado
    completed_pyramiding_levels: Set[float] = field(default_factory=set)  # ROIs ya escalados
    
    def __post_init__(self):
        if self.original_quantity == 0.0:
            self.original_quantity = self.quantity
        if self.total_quantity == 0.0:
            self.total_quantity = self.quantity
        if self.weighted_avg_price == 0.0:
            self.weighted_avg_price = self.entry_price

        # Si trade_id vacio, inicializar uno único por entry_time (ms)
        if not self.trade_id:
            try:
                ts_ms = int(self.entry_time.timestamp())
            except Exception:
                ts_ms = int(time.time())
            self.trade_id = f"{self.symbol}_{ts_ms}"

@dataclass
class OrderCommandData:
    """Comando para ejecutar órdenes"""
    command: str
    symbol: str
    data: Dict
    trade_id: Optional[str] = None        # <-- nuevo
    timestamp: datetime = field(default_factory=datetime.now)

class ProfitTargetManager:
    """
    Gestión de targets de ganancia automáticos.

    - base_amount: incrementos del target (ej. 200.0)
    - wait_hours: cooldown tras alcanzar target
    - consider_unrealized: si True, también considera PnL no realizado (combined balance)
    - use_net_estimate: si True usa combined_balance_net_est (estimado neto), si False usa gross
    """
    def __init__(self,
                 base_amount: float = 2.0,
                 wait_hours: float = 1.0,
                 bot=None,
                 consider_unrealized: bool = True,
                 use_net_estimate: bool = True):
        self.base_amount = float(base_amount)
        self.wait_hours = float(wait_hours)
        self.current_target = float(base_amount)
        self.lock = threading.RLock()
        self.target_history = []  # Para tracking
        self.bot = bot
        self.consider_unrealized = bool(consider_unrealized)
        self.use_net_estimate = bool(use_net_estimate)

    def attach_bot(self, bot_instance):
        """Permite adjuntar el bot después de crear el manager (si hace falta)."""
        with self.lock:
            self.bot = bot_instance

    def get_current_target(self) -> float:
        """Obtiene el target actual de ganancia"""
        with self.lock:
            return float(self.current_target)

    def set_next_target(self):
        """Incrementa al siguiente target (registra historial)"""
        with self.lock:
            self.target_history.append({
                'target': float(self.current_target),
                'timestamp': datetime.now()
            })
            self.current_target  = self.base_amount # QUITE UN + ESTE ES EL ORIGNAL += 
            if getattr(self.bot, 'inversion_posiciones_PROBABLE'):  #  inversión por historial de pérdidas")
                self.current_target *= 1.5  # Disminuir el target un 50% más si hay inversión por historial de pérdidas
            else:
                self.current_target *= 1.0  # Mantener el target base si no hay inversión por historial de pérdidas
                                    
            logger.info(f"✅ Target alcanzado: ${self.current_target - self.base_amount:.2f}")
            logger.info(f"🎯 Nuevo target: ${self.current_target:.2f}")

            # --- LÍNEA AÑADIDA (mínima): si ya hubo 2 ciclos, reiniciamos ---
            #if len(self.target_history) >= 1:
                #self.reset_for_new_cycle()
            # -----------------------------------------------------------------

    def reset_for_new_cycle(self):
        """Reinicia para un nuevo ciclo"""
        with self.lock:
            self.current_target = float(self.base_amount)
            logger.info(f"🔄 Ciclo reiniciado. Target: ${self.current_target:.2f}")

    def _get_unrealized_summary(self) -> Dict:
        """
        Wrapper seguro para llamar a bot.compute_unrealized_pnl_summary()
        Devuelve dict con claves esperadas ó None si no disponible.
        """
        try:
            if not self.bot:
                return None
            if getattr(self.bot, 'compute_unrealized_pnl_summary', None) is None:
                return None
            return self.bot.compute_unrealized_pnl_summary()
        except Exception as e:
            logger.debug(f"Error obteniendo PnL no realizado: {e}")
            return None

    def is_target_reached(self) -> bool:
        """
        Evalúa si se alcanzó el target. Dos checks:
         1) Ganancia realizada (balance - daily_start_balance) >= target
         2) (Opcional) Balance combinado (balance + unrealized_pnl) >= (daily_start_balance + target)
        """
        with self.lock:
            try:
                if not self.bot:
                    logger.debug("ProfitTargetManager: bot no adjuntado, usando solo current_target check por balance.")
                    return False

                # 1) Ganancia realizada
                balance = float(getattr(self.bot, 'balance', 0.0))
                daily_start = float(getattr(self.bot, 'daily_start_balance', 0.0))
                realized_gain = balance - daily_start
                target = float(self.current_target)

                # Log de diagnóstico (muy útil al debuguear)
                logger.debug(f"PTM: balance={balance:.2f}, daily_start={daily_start:.2f}, realized_gain={realized_gain:.2f}, target={target:.2f}")

                if realized_gain >= target or realized_gain <= -10.5*target:  # Considerar también pérdida extrema
                    logger.info(f"PTM: Target alcanzado por ganancia realizada: ${realized_gain:.2f} >= ${target:.2f}")
                    if realized_gain <= -10.5*target:
                       if getattr(self.bot, 'inversion_posiciones_PROBABLE'):
                                 
                            setattr(self.bot, 'inversion_posiciones_PROBABLE', False)  # Activar inversión por historial de pérdidas")
                       else: 
                            setattr(self.bot, 'inversion_posiciones_PROBABLE', True) 
                            
                    elif realized_gain >= target:
                        logger.debug("ProfitTargetManager: bot  adjuntado, usando solo current_target check por balance.")                     
                        
                    return True

                # 2) Considerar PnL no realizado -> balance combinado
                if self.consider_unrealized:
                    summary = self._get_unrealized_summary()
                    if summary:
                        if self.use_net_estimate:
                            combined = float(summary.get('combined_balance_net_est', summary.get('combined_balance_gross', None)))
                        else:
                            combined = float(summary.get('combined_balance_gross', None))

                        if combined is not None:
                            combined_gain = combined - daily_start
                            logger.debug(f"PTM: combined_balance={combined:.2f}, combined_gain={combined_gain:.2f}")
                            if combined_gain >= target or combined_gain <= -10.5*target:  # Considerar también pérdida extrema
                                logger.info(f"PTM: Target alcanzado por balance combinado (incluye PnL no realizado): ${combined_gain:.2f} >= ${target:.2f}")
                                if combined_gain <= -10.5*target:
                                    if getattr(self.bot, 'inversion_posiciones_PROBABLE'):
                                        setattr(self.bot, 'inversion_posiciones_PROBABLE', False)  # Desactivar inversión por historial de pérdidas
                                    else:
                                        setattr(self.bot, 'inversion_posiciones_PROBABLE', True)  # Activar inversión por historial de pérdidas
                                elif combined_gain >= target:
                                    
                                    logger.debug("ProfitTargetManager: bot adjuntado, usando solo current_target check por balance.")
                                    
                                return True

                return False

            except Exception as e:
                logger.exception(f"Error en is_target_reached: {e}")
                return False



# ==================== CACHE DE DATOS MEJORADO ====================

class DataCache:
    """Cache thread-safe para datos de mercado con WebSocket integration"""
    
    def __init__(self, max_symbols: int = 40, max_candles: int = 1500):
        self.max_symbols = int(max_symbols)
        self.max_candles = int(max_candles)
        
        self.cache_1m = OrderedDict()
        self.cache_5m = OrderedDict()
        self.price_cache = {}
        self.last_update = {}
        self.lock = threading.Lock()
        
        # 🆕 INTEGRACIÓN WEBSOCKET DE PRECIOS
        self.ws_price_cache = None
        self.ws_symbols = set()
    
    def _normalize(self, symbol: str) -> str:
        return symbol.upper() if symbol is not None else symbol
    
    def initialize_websocket(self, symbols: List[str]):
        """🆕 Inicializa el WebSocket de precios"""
        try:
            self.ws_symbols = set(symbols)
            self.ws_price_cache = SymbolWebSocketPriceCache(symbols, symbols_per_connection=40)
            self.ws_price_cache.start()
            logger.info(f"✅ WebSocket de precios inicializado para {len(symbols)} símbolos")
        except Exception as e:
            logger.error(f"❌ Error inicializando WebSocket: {e}")
    
    def get_current_price(self, symbol: str) -> Optional[float]:
        """🆕 Obtiene precio actual del WebSocket con fallback al cache normal"""
        sym = self._normalize(symbol)
        
        # 1. Intentar obtener del WebSocket primero
        if self.ws_price_cache and sym in self.ws_symbols:
            try:
                ws_price = self.ws_price_cache.get_price(sym)
                if ws_price is not None and ws_price > 0:
                    # Actualizar cache interno con precio del WebSocket
                    with self.lock:
                        self.price_cache[sym] = ws_price
                        self.last_update[sym] = time.time()
                    return float(ws_price)
            except Exception as e:
                logger.debug(f"Error obteniendo precio WS para {sym}: {e}")
        
        # 2. Fallback al cache normal
        with self.lock:
            p = self.price_cache.get(sym)
            return float(p) if p is not None else None
    
    def is_data_fresh(self, symbol: str, max_age_seconds: int = 60) -> bool:
        """🆕 Verifica si los datos son frescos usando WebSocket"""
        sym = self._normalize(symbol)
        
        # Para WebSocket, asumimos que los precios son siempre frescos
        if self.ws_price_cache and sym in self.ws_symbols:
            return True
            
        # Fallback a la lógica original
        with self.lock:
            ts = self.last_update.get(sym)
            if ts is None:
                return False
            age = time.time() - float(ts)
            return age < float(max_age_seconds)
    
    def update_data(self, symbol: str, data_1m: pd.DataFrame, data_5m: pd.DataFrame, ts: Optional[float] = None):
        """Actualiza el cache con DataFrames"""
        sym = self._normalize(symbol)
        if sym is None:
            return
        
        with self.lock:
            if isinstance(data_1m, pd.DataFrame) and len(data_1m) > self.max_candles:
                data_1m = data_1m.tail(self.max_candles).copy()
            if isinstance(data_5m, pd.DataFrame) and len(data_5m) > self.max_candles:
                data_5m = data_5m.tail(self.max_candles).copy()
            
            if isinstance(data_1m, pd.DataFrame):
                self.cache_1m[sym] = data_1m.copy()
                self.cache_1m.move_to_end(sym, last=True)
            if isinstance(data_5m, pd.DataFrame):
                self.cache_5m[sym] = data_5m.copy()
                self.cache_5m.move_to_end(sym, last=True)
            
            # 🆕 Actualizar precio solo si no tenemos WebSocket o está fallando
            if (not self.ws_price_cache or sym not in self.ws_symbols) and \
               isinstance(data_1m, pd.DataFrame) and not data_1m.empty and 'close' in data_1m.columns:
                try:
                    last_price = float(data_1m['close'].iloc[-1])
                    if np.isfinite(last_price) and last_price > 0:
                        self.price_cache[sym] = last_price
                except Exception:
                    pass
            
            self.last_update[sym] = float(ts if ts is not None else time.time())
    
    def get_data(self, symbol: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Devuelve (df_1m, df_5m)"""
        sym = self._normalize(symbol)
        empty = pd.DataFrame(columns=['timestamp','open','high','low','close','volume'])
        with self.lock:
            df1 = self.cache_1m.get(sym)
            df5 = self.cache_5m.get(sym)
            df1 = df1.copy() if isinstance(df1, pd.DataFrame) else empty.copy()
            df5 = df5.copy() if isinstance(df5, pd.DataFrame) else empty.copy()
        return df1.reset_index(drop=True), df5.reset_index(drop=True)
    
    def stop_websocket(self):
        """🆕 Detiene el WebSocket de precios"""
        if self.ws_price_cache:
            try:
                self.ws_price_cache.stop()
                logger.info("✅ WebSocket de precios detenido")
            except Exception as e:
                logger.error(f"Error deteniendo WebSocket: {e}")


# ==================== GESTOR DE ESTADO MEJORADO ====================

class TradeStateManager:
    """Gestor centralizado del estado de todos los trades - VERSIÓN CORREGIDA"""
    
    def __init__(self):
        self._trades: Dict[str, TradeInfo] = {}
        self._lock = threading.RLock()
        
        # 🔧 FIX 2: Un ÚNICO lock para operaciones de cierre (no doble)
        self._closing: Set[str] = set()
    
    def add_trade(self, symbol: str, trade: TradeInfo) -> bool:
        with self._lock:
            if symbol in self._trades:
                return False
            self._trades[symbol] = trade
            return True
    
    def get_trade(self, symbol: str) -> Optional[TradeInfo]:
        with self._lock:
            return self._trades.get(symbol)
    
    def remove_trade(self, symbol: str) -> Optional[TradeInfo]:
        with self._lock:
            return self._trades.pop(symbol, None)
    
    def update_trade_state(self, symbol: str, new_state: TradeState) -> bool:
        with self._lock:
            trade = self._trades.get(symbol)
            if not trade:
                return False
            trade.state = new_state
            trade.last_update = datetime.now()
            return True
    
    def mark_closing(self, symbol: str) -> bool:
        """🔧 FIX 2: Simplificado - un ÚNICO lock, lógica directa"""
        with self._lock:
            # Si ya está marcado, NO lo marcamos de nuevo
            if symbol in self._closing:
                logger.debug(f"Symbol {symbol} ya está en cierre")
                return False
            
            # Marcar como cierre
            self._closing.add(symbol)
            
            # Actualizar estado del trade
            trade = self._trades.get(symbol)
            if trade:
                trade.state = TradeState.PENDING_CLOSE
            
            logger.debug(f"✅ {symbol} marcado como cerrando (1er intento)")
            return True
    
    def unmark_closing(self, symbol: str):
        """Desmarca un símbolo de cierre"""
        with self._lock:
            self._closing.discard(symbol)
    
    def is_closing(self, symbol: str) -> bool:
        """Verifica si está marcado para cierre"""
        with self._lock:
            return symbol in self._closing
    
    def get_all_active_symbols(self) -> Set[str]:
        with self._lock:
            return set(self._trades.keys())
    
    def cleanup_symbol(self, symbol: str):
        """🔧 Limpia completamente un símbolo del sistema"""
        with self._lock:
            self._closing.discard(symbol)
            self._trades.pop(symbol, None)
            logger.debug(f"🧹 Símbolo {symbol} limpiado del state manager")
    
    def update_tp_sl(self, symbol: str, new_tp: float, new_sl: float, reason: str = ""):
        """Actualiza TP/SL y registra en historial"""
        with self._lock:
            trade = self._trades.get(symbol)
            if not trade:
                return False
            
            trade.tp_sl_history.append({
                'timestamp': datetime.now(),
                'tp': new_tp,
                'sl': new_sl,
                'reason': reason,
                'bars_held': trade.bars_held
            })
            
            trade.current_tp = new_tp
            trade.current_sl = new_sl
            trade.last_update = datetime.now()
            return True

# ==================== EXECUTOR DE ÓRDENES MEJORADO ====================

class OrderExecutor:
    """
    Versión lista para pegar en nuevo.py.

    Características:
     - Prioriza CLOSE sobre OPEN (PriorityQueue).
     - Pool de 15 workers (ThreadPoolExecutor) para throughput (IO-bound).
     - Locks por símbolo para permitir concurrencia entre símbolos y evitar race conditions en el mismo símbolo.
     - Previene duplicados de comandos y evita aperturas cuando hay cierre global o símbolo en 'closing'.
     - Minimiza tiempo con _api_lock solo alrededor de llamadas de red.
     - Registra siempre el cierre (aunque falle el exchange) y limpia el estado.
    """

    def __init__(self, api, state_manager, bot_instance, simulate: bool = False):
        self.api = api
        self.state_manager = state_manager
        self.bot = bot_instance
        self.simulate = simulate

        # Cola priorizada: (priority, seq, command)
        self._queue = PriorityQueue(maxsize=COMMAND_QUEUE_MAX)
        self._seq = itertools.count()

        # Prevención de duplicados (ids de comando)
        self._processing_commands = set()
        self._processing_lock = threading.Lock()
                # Prevención de cierres duplicados por trade_id
        self._closed_trade_ids: Set[str] = set()
        self._closed_trade_ids_lock = threading.Lock()


        # Locks por símbolo: permite concurrencia entre diferentes símbolos
        self._symbol_locks: Dict[str, threading.Lock] = {}
        self._symbol_locks_lock = threading.Lock()

        # Lock para llamadas al API (mantenerlo mínimo)
        self._api_lock = threading.Lock()

        # Thread pool para ejecutar comandos concurrentemente
        self._executor = ThreadPoolExecutor(max_workers=DEFAULT_MAX_WORKERS)
        self._running = False
        self._dispatcher_thread: Optional[threading.Thread] = None

        # Métricas simples
        self.metrics = {
            'submitted': 0,
            'ignored_duplicates': 0,
            'rejected_due_to_global_close': 0,
            'processed': 0,
            'errors': 0
        }

        logger.info("OrderExecutor creado (workers=%d, queue_max=%d)", DEFAULT_MAX_WORKERS, COMMAND_QUEUE_MAX)

    # -------------------------
    # Control de ciclo
    # -------------------------
    def start(self):
        if self._running:
            return
        self._running = True
        self._dispatcher_thread = threading.Thread(target=self._dispatch_loop, daemon=True)
        self._dispatcher_thread.start()
        logger.info("✅ OrderExecutor iniciado")

    def stop(self, wait_seconds: float = 5.0):
        self._running = False
        if self._dispatcher_thread:
            self._dispatcher_thread.join(timeout=wait_seconds)
        try:
            self._executor.shutdown(wait=False)
        except Exception:
            pass
        logger.info("🛑 OrderExecutor detenido")

    # -------------------------
    # Helpers
    # -------------------------
    def _get_symbol_lock(self, symbol: str) -> threading.Lock:
        with self._symbol_locks_lock:
            lock = self._symbol_locks.get(symbol)
            if lock is None:
                lock = threading.Lock()
                self._symbol_locks[symbol] = lock
            return lock

    def _is_global_closing(self) -> bool:
        """
        Detecta flags de cierre global en state_manager de forma tolerante.
        Busca atributos comunes o callables.
        """
        for attr in ('is_global_closing', 'global_closing', 'has_global_close', 'is_shutting_down'):
            flag = getattr(self.state_manager, attr, None)
            if isinstance(flag, bool):
                if flag:
                    return True
            elif callable(flag):
                try:
                    if flag():
                        return True
                except Exception:
                    continue
        return False

    def _command_id(self, command) -> str:
        ts = getattr(command, 'timestamp', None)
        try:
            ts_val = ts.timestamp() if ts is not None else None
        except Exception:
            ts_val = str(ts)
        trade_id = getattr(command, 'trade_id', None)
        # fallback si viene en data
        if not trade_id:
            trade_id = command.data.get('trade_id') if isinstance(command.data, dict) else None
        return f"{getattr(command, 'command', '?')}_{getattr(command, 'symbol', '?')}_{trade_id}_{ts_val}"

    # -------------------------
    # Envío de comandos
    # -------------------------
    def submit_command(self, command):
        """
        Inserta un comando en la cola priorizada.
        Previene duplicados y rechaza OPEN si hay cierre global o símbolo ya en proceso de cierre.
        """
        cmd_id = self._command_id(command)
        trade_id = getattr(command, 'trade_id', None) or (command.data.get('trade_id') if isinstance(command.data, dict) else None)
        with self._processing_lock:
            if cmd_id in self._processing_commands:
                self.metrics['ignored_duplicates'] += 1
                logger.debug("Comando duplicado ignorado: %s", cmd_id)
                return
            
             # Si close y trade_id ya fue cerrado, ignorar
            if getattr(command, 'command', None) == "CLOSE_POSITION" and trade_id:
                with self._closed_trade_ids_lock:
                    if trade_id in self._closed_trade_ids:
                        logger.debug("CLOSE_POSITION ignorado porque trade_id ya cerrado: %s", trade_id)
                        self.metrics['ignored_duplicates'] += 1
                        return

            # Fast-reject: si es OPEN y hay cierre global o símbolo en closing
            if getattr(command, 'command', None) == "OPEN_POSITION":
                if self._is_global_closing():
                    self.metrics['rejected_due_to_global_close'] += 1
                    logger.warning("OPEN rechazado por cierre global activo: %s", getattr(command, 'symbol', None))
                    return

                # Si state_manager ofrece is_closing(symbol)
                try:
                    is_closing_fn = getattr(self.state_manager, 'is_closing', None)
                    if callable(is_closing_fn) and is_closing_fn(getattr(command, 'symbol', None)):
                        logger.warning("OPEN rechazado: símbolo en proceso de cierre: %s", getattr(command, 'symbol', None))
                        return
                except Exception:
                    # no romper si state_manager falla
                    pass


            # dentro de submit_command, justo antes de `self._processing_commands.add(cmd_id)`
            if getattr(command, 'command', None) == "CLOSE_POSITION":
                try:
                    if callable(getattr(self.state_manager, 'is_closing', None)) and self.state_manager.is_closing(getattr(command, 'symbol', None)):
                        logger.debug("CLOSE rechazado: símbolo ya marcado como closing: %s", getattr(command, 'symbol', None))
                        return
                except Exception:
                    pass


            # marcar como en procesamiento para evitar reenvíos
            self._processing_commands.add(cmd_id)

        priority = _PRIORITY.get(getattr(command, 'command', 'OPEN_POSITION'), 2)
        seq = next(self._seq)
        try:
            self._queue.put_nowait((priority, seq, command))
            self.metrics['submitted'] += 1
        except Full:
            with self._processing_lock:
                self._processing_commands.discard(cmd_id)
            self.metrics['errors'] += 1
            logger.error("Cola de comandos llena. Comando descartado: %s", cmd_id)

    # -------------------------
    # Dispatcher: saca comandos y los envía al pool
    # -------------------------
    def _dispatch_loop(self):
        logger.debug("Dispatcher loop iniciado")
        while self._running:
            try:
                priority, seq, cmd = self._queue.get(timeout=1.0)
            except Empty:
                continue

            # enviar al pool inmediatamente
            try:
                self._executor.submit(self._process_command_worker, cmd)
            except Exception as e:
                logger.exception("Error submitiendo tarea al pool: %s", e)
                # limpiar marca para no bloquear reenvío futuro
                with self._processing_lock:
                    self._processing_commands.discard(self._command_id(cmd))

    # -------------------------
    # Worker: ejecuta comando protegido por lock de símbolo
    # -------------------------
    def _process_command_worker(self, cmd):
        cmd_type = getattr(cmd, 'command', None)
        symbol = getattr(cmd, 'symbol', None)
        cmd_id = self._command_id(cmd)

        symbol_lock = self._get_symbol_lock(symbol) if symbol else threading.Lock()
        acquired = False

        try:
            acquired = symbol_lock.acquire(timeout=12)
            if not acquired:
                logger.warning("No se pudo adquirir lock para %s - descartando temporalmente", symbol)
                return

            # Re-check: evitar abrir si se activó cierre global mientras estaba en cola
            if cmd_type == "OPEN_POSITION" and self._is_global_closing():
                logger.warning("OPEN rechazado en worker por cierre global: %s", symbol)
                return

            # Ejecutar el handler correspondiente
            try:
                if cmd_type == "OPEN_POSITION":
                    self._open_position(cmd)
                elif cmd_type == "OPEN_PYRAMIDING":          # ← NUEVO
                    self._open_pyramiding(cmd)               # ← NUEVO
                elif cmd_type == "CLOSE_POSITION":
                    # Reservar trade_id para evitar otra ejecución concurrente
                    trade_id = getattr(cmd, 'trade_id', None) or (cmd.data.get('trade_id') if isinstance(cmd.data, dict) else None)
                    if trade_id:
                        with self._closed_trade_ids_lock:
                            if trade_id in self._closed_trade_ids:
                                logger.debug("Worker: CLOSE ignorado, trade_id ya cerrado: %s", trade_id)
                                return
                            # marcar reservado inmediatamente
                            self._closed_trade_ids.add(trade_id)
                    self._close_position(cmd)

                elif cmd_type == "UPDATE_TP_SL":
                    self._update_tp_sl(cmd)
                else:
                    logger.error("Tipo de comando desconocido: %s", cmd_type)
            except Exception as e:
                self.metrics['errors'] += 1
                logger.exception("Error ejecutando comando %s para %s: %s", cmd_type, symbol, e)
            finally:
                self.metrics['processed'] += 1

        finally:
            if acquired:
                try:
                    symbol_lock.release()
                except Exception:
                    pass
            # limpiar set de procesados
            with self._processing_lock:
                self._processing_commands.discard(cmd_id)

    # -------------------------
    # Implementación de operaciones (adaptada de tu versión original)
    # -------------------------
    def _open_position(self, cmd):
        symbol = cmd.symbol
        logger.info("🔁 OPEN_POSITION worker: %s", symbol)

        # Doble chequeo
        if self._is_global_closing():
            logger.warning("Abortando OPEN por cierre global: %s", symbol)
            return

        try:
            if self.simulate:
                logger.info("🧪 (Simulado) Apertura %s %s", cmd.data.get('side'), symbol)
                try:
                    self.state_manager.update_trade_state(symbol, getattr(TradeState, 'OPEN', 'OPEN'))
                except Exception:
                    pass
                return

            # set leverage (si falla, continuamos)
            leverage = cmd.data.get('leverage', 20)
            try:
                with self._api_lock:
                    self.api.set_leverage(symbol, leverage)
                    logger.debug("Leverage set %s @ %sx", symbol, leverage)
            except Exception as e:
                logger.warning("No se pudo configurar leverage para %s: %s", symbol, e)

            # crear orden bracket (minimizar tiempo con api_lock)
            try:
                with self._api_lock:
                    result = self.api.bracket_batch(
                        symbol=symbol,
                        side=cmd.data['side'],
                        quantity=cmd.data['quantity'],
                        entry_type="MARKET",
                        take_profit=cmd.data.get('tp'),
                        stop_loss=cmd.data.get('sl')
                    )
                if result:
                    try:
                            
                        logger.info("✅ Posición : %s %s ",symbol, result)
                        self.state_manager.update_trade_state(symbol, getattr(TradeState, 'OPEN', 'OPEN'))
                    except Exception:
                        pass
                    logger.info("✅ Posición abierta: %s %s", symbol, cmd.data.get('side'))
                else:
                    logger.error("❌ Falló apertura de %s", symbol)
            except Exception as e:
                logger.exception("Error abriendo posición %s: %s", symbol, e)

        except Exception as e:
            logger.exception("ERROR crítico en _open_position para %s: %s", symbol, e)

    def _open_pyramiding(self, cmd):
            """
            Abre una posición con estrategia de pyramiding enviando hasta 5 entradas
            escalonadas en UN SOLO REQUEST usando api.pyramiding_batch().

            Campos esperados en cmd.data:
            ─────────────────────────────
            Obligatorios:
                side        (str)   : 'BUY' | 'SELL'
                entries     (list)  : Lista de 1-5 dicts, cada uno con:
                                        'price'    (float) – precio límite de entrada
                                        'quantity' (float) – cantidad de esa entrada
                                        'type'     (str)   – 'LIMIT' | 'MARKET' (default 'LIMIT')

            Opcionales:
                leverage      (int)   : Apalancamiento. Default 20.
                tp            (float) : Take profit – se guarda en el trade para referencia.
                sl            (float) : Stop loss   – ídem.
                position_side (str)   : 'LONG' | 'SHORT' | 'BOTH' (auto si no se pasa).
                time_in_force (str)   : 'GTC' | 'IOC' | 'FOK'. Default 'GTC'.

            Atributos que escribe en el trade (state_manager):
                weighted_avg_price  – precio promedio ponderado de los niveles aceptados.
                total_quantity      – suma de cantidades de los niveles aceptados.
                pyramid_levels      – lista con el detalle de cada nivel (orderId, precio, qty…).
                tp / sl             – si se pasan, para referencia del bot.

            Ejemplo de cmd.data:
            ─────────────────────
                {
                    'side': 'BUY',
                    'leverage': 20,
                    'tp': 97_000.0,
                    'sl': 85_000.0,
                    'entries': [
                        {'price': 94_000, 'quantity': 0.002},
                        {'price': 92_000, 'quantity': 0.003},
                        {'price': 90_000, 'quantity': 0.005},
                        {'price': 88_000, 'quantity': 0.008},
                    ]
                }
            """
            symbol = cmd.symbol
            logger.info("🔼 OPEN_PYRAMIDING worker: %s", symbol)

            # ── 1) Doble chequeo de cierre global ────────────────────────────────────
            if self._is_global_closing():
                logger.warning("Abortando OPEN_PYRAMIDING por cierre global: %s", symbol)
                return

            # ── 2) Extraer y validar datos ────────────────────────────────────────────
            data          = cmd.data or {}
            side          = data.get('side', '').upper()
            entries       = data.get('entries')
            leverage      = data.get('leverage', 20)
            tp            = data.get('tp')
            sl            = data.get('sl')
            position_side = data.get('position_side')
            time_in_force = data.get('time_in_force', 'GTC')

            if not side or side not in ('BUY', 'SELL'):
                logger.error(
                    "❌ OPEN_PYRAMIDING: 'side' inválido o ausente para %s: %r", symbol, side
                )
                return

            if not entries or not isinstance(entries, list) or len(entries) == 0:
                logger.error("❌ OPEN_PYRAMIDING: 'entries' vacío o inválido para %s", symbol)
                return

            if len(entries) > 5:
                logger.error(
                    "❌ OPEN_PYRAMIDING: Binance batchOrders acepta máximo 5 entradas. "
                    "Se recibieron %d para %s. Recorta la lista.", len(entries), symbol
                )
                return

            # ── 3) Simulación ─────────────────────────────────────────────────────────
            if self.simulate:
                total_qty = sum(float(e.get('quantity', 0)) for e in entries)
                # precio promedio ponderado estimado solo con las que tienen 'price'
                limit_entries = [e for e in entries if e.get('type', 'LIMIT').upper() == 'LIMIT' and 'price' in e]
                if limit_entries and total_qty:
                    sim_avg = sum(float(e['price']) * float(e.get('quantity', 0)) for e in limit_entries) / total_qty
                else:
                    sim_avg = 0.0
                logger.info(
                    "🧪 (Simulado) Pyramiding %s %s | %d niveles | avg≈%.4f | qty=%.6f",
                    side, symbol, len(entries), sim_avg, total_qty
                )
                try:
                    self.state_manager.update_trade_state(symbol, getattr(TradeState, 'OPEN', 'OPEN'))
                except Exception:
                    pass
                return

            # ── 4) Configurar apalancamiento ──────────────────────────────────────────
            try:
                with self._api_lock:
                    self.api.set_leverage(symbol, leverage)
                logger.debug("Leverage %sx configurado para %s", leverage, symbol)
            except Exception as e:
                logger.warning(
                    "No se pudo configurar leverage para %s: %s — se continúa con el actual", symbol, e
                )

            # ── 5) Enviar pyramiding_batch ────────────────────────────────────────────
            batch_result = None
            try:
                with self._api_lock:
                    batch_result = self.api.pyramiding_batch(
                        symbol=symbol,
                        side=side,
                        entries=entries,
                        position_side=position_side,
                        time_in_force=time_in_force,
                        validate_prices=True,
                        leverage=None,   # ya se configuró arriba, evitar doble llamada
                    )
            except ValueError as ve:
                # Error de validación local (precio inválido, entries mal formados, etc.)
                logger.error("❌ OPEN_PYRAMIDING: validación fallida para %s: %s", symbol, ve)
                return
            except Exception as e:
                logger.exception("❌ Error inesperado en pyramiding_batch para %s: %s", symbol, e)
                return

            # ── 6) Analizar resultados ────────────────────────────────────────────────
            if not batch_result or not isinstance(batch_result, list):
                logger.error("❌ pyramiding_batch no devolvió resultados para %s", symbol)
                return

            accepted_levels = []
            rejected_count  = 0

            for i, r in enumerate(batch_result):
                if not isinstance(r, dict):
                    continue

                entry_ref = entries[i] if i < len(entries) else {}

                if 'orderId' in r:
                    # Orden aceptada por Binance
                    lvl_price = float(r.get('price') or entry_ref.get('price') or 0)
                    lvl_qty   = float(r.get('origQty') or entry_ref.get('quantity') or 0)
                    accepted_levels.append({
                        'index':    i,
                        'orderId':  r['orderId'],
                        'price':    lvl_price,
                        'quantity': lvl_qty,
                        'status':   r.get('status'),
                        'type':     r.get('type'),
                    })
                    logger.info(
                        "   ✅ Nivel %d aceptado | orderId=%s | price=%.4f | qty=%.6f | status=%s",
                        i, r['orderId'], lvl_price, lvl_qty, r.get('status')
                    )
                elif 'code' in r:
                    # Binance devolvió error para esta sub-orden
                    # (pyramiding_batch ya intentó el fallback individual)
                    rejected_count += 1
                    logger.warning(
                        "   ⚠️  Nivel %d rechazado (code=%s: %s) para %s",
                        i, r.get('code'), r.get('msg'), symbol
                    )

            if not accepted_levels:
                logger.error(
                    "❌ Todos los niveles pyramid fueron rechazados para %s — no se abre posición",
                    symbol
                )
                return

            # ── 7) Calcular precio promedio ponderado y cantidad total ─────────────────
            total_quantity     = sum(lvl['quantity'] for lvl in accepted_levels)
            weighted_avg_price = (
                sum(lvl['price'] * lvl['quantity'] for lvl in accepted_levels) / total_quantity
                if total_quantity > 0 else 0.0
            )

            logger.info(
                "✅ Pyramiding ABIERTO: %s %s | %d/%d niveles | "
                "qty_total=%.6f | precio_promedio=%.4f | rechazadas=%d",
                side, symbol,
                len(accepted_levels), len(entries),
                total_quantity, weighted_avg_price, rejected_count
            )

            # ── 8) Persistir datos en el trade para PnL correcto al cierre ────────────
            #   _record_closed_position ya lee 'weighted_avg_price' y 'total_quantity'
            #   (ver el PATCH en esa función) para calcular el PnL real del pyramiding.
            try:
                trade = self.state_manager.get_trade(symbol)
                if trade:
                    trade.weighted_avg_price = weighted_avg_price
                    trade.total_quantity     = total_quantity
                    trade.pyramid_levels     = accepted_levels
                    logger.debug(
                        "Trade %s actualizado → avg=%.4f | qty=%.6f | niveles=%d",
                        symbol, weighted_avg_price, total_quantity, len(accepted_levels)
                    )
            except Exception as e:
                logger.warning(
                    "No se pudieron persistir datos pyramid en el trade de %s: %s", symbol, e
                )

            # Marcar posición como OPEN en el state_manager
            try:
                self.state_manager.update_trade_state(symbol, getattr(TradeState, 'OPEN', 'OPEN'))
            except Exception as e:
                logger.warning(
                    "No se pudo actualizar trade_state a OPEN para %s: %s", symbol, e
                )

            # ── 9) TP / SL: guardar referencia en el trade ────────────────────────────
            #   Las entradas son LIMIT y pueden llenarse en momentos distintos,
            #   por lo que colocar TP/SL ahora sobre el total sería prematuro.
            #   El bot debe monitorear los fills y llamar UPDATE_TP_SL cuando
            #   la posición esté completamente abierta.
            if tp or sl:
                logger.info(
                    "ℹ️  TP=%.4f / SL=%.4f guardados en trade %s para referencia. "
                    "Ajústalos vía UPDATE_TP_SL cuando todas las entradas LIMIT estén filled.",
                    tp or 0, sl or 0, symbol
                )
                try:
                    trade = self.state_manager.get_trade(symbol)
                    if trade:
                        if tp:
                            trade.tp = tp
                        if sl:
                            trade.sl = sl
                except Exception:
                    pass


    def _close_position(self, cmd):
        symbol = cmd.symbol
        logger.info("🔁 CLOSE_POSITION worker: %s", symbol)

        # Obtener trade guardado
        trade = None
        try:
            trade = self.state_manager.get_trade(symbol)
        except Exception:
            pass

        if not trade:
            logger.warning("Trade no encontrado: %s", symbol)
            return

        # Intentar marcar como closing con reintentos
        marked = False
        for attempt in range(MARK_CLOSING_RETRIES):
            try:
                if self.state_manager.mark_closing(symbol):
                    marked = True
                    logger.debug("%s marcado como closing (intento %d)", symbol, attempt + 1)
                    break
            except Exception as e:
                logger.debug("mark_closing fallo temporalmente para %s: %s", symbol, e)
            time.sleep(MARK_CLOSING_RETRY_DELAY)

        if not marked:
            logger.error("❌ No se pudo marcar %s como closing tras %d intentos", symbol, MARK_CLOSING_RETRIES)
            return

        # Obtener precio preferente (cache -> exchange -> fallback entry_price)
        current_price = None
        try:
            if hasattr(self.bot, 'data_cache') and getattr(self.bot.data_cache, 'get_current_price', None):
                current_price = self.bot.data_cache.get_current_price(symbol)
                if current_price:
                    logger.debug("Precio desde cache: %s -> %s", symbol, current_price)
        except Exception:
            logger.debug("Error leyendo cache (ignorado) para %s", symbol)

        if current_price is None and not self.simulate:
            try:
                with self._api_lock:
                    current_price = self.api.get_last_price(symbol)
                    logger.debug("Precio desde exchange para %s -> %s", symbol, current_price)
            except Exception as e:
                logger.warning("Error obteniendo precio del exchange para %s: %s", symbol, e)

        if current_price is None or current_price <= 0:
            logger.warning("Usando entry_price como fallback para %s", symbol)
            current_price = getattr(trade, 'entry_price', 0.0)

        # Cerrar en exchange (si no es simulación)
        if not self.simulate:
            try:
                with self._api_lock:
                    close_result = self.api.close_all_positions(symbol)
                    
                    if close_result:
                        logger.info("✅ Cerrada posición en exchange: %s", symbol)
                    else:
                        logger.warning("⚠️ Exchange devolvió False al cerrar %s", symbol)
            except Exception as e:
                logger.exception("Error cerrando posición en exchange para %s: %s", symbol, e)
        else:
            logger.info("🧪 Simulación: no se cierra en exchange")

        # Registrar cierre SIEMPRE (aunque el exchange falle)
        try:
            self._record_closed_position(symbol, trade, current_price, cmd.data.get('reason', 'CLOSE'))
        except Exception as e:
            logger.exception("Error registrando cierre para %s: %s", symbol, e)
            try:
                self.state_manager.unmark_closing(symbol)
            except Exception:
                pass

    def _record_closed_position(self, symbol: str, trade, exit_price: float, reason: str):
        """Registro de cierre con cálculos PnL y limpieza (basado en tu implementación original)."""
        logger.info("%s", "=" * 60)
        logger.info("📝 INICIANDO REGISTRO DE CIERRE: %s", symbol)
        logger.info("%s", "=" * 60)

         # Evitar doble registro si ese trade_id ya fue registrado
        try:
            trade_id = getattr(trade, 'trade_id', None)
            if trade_id:
                with self._closed_trade_ids_lock:
                    if trade_id in self._closed_trade_ids and any(t.get('trade_id') == trade_id for t in getattr(self.bot, 'completed_trades', [])):
                        logger.debug("Registro de cierre ignorado: trade_id ya registrado %s", trade_id)
                        return True
                    # (si no está en closed set pero llegó aquí, añadimos para consistencia)
                    self._closed_trade_ids.add(trade_id)
        except Exception:
            pass

        try:
            logger.info("🔍 Validando datos para %s", symbol)

            # --- PATCH: usar precio promedio y cantidad total si hay pyramiding ---
            # Entrada original (fallback)
            orig_entry_price = getattr(trade, 'entry_price', 0.0)
            orig_quantity = getattr(trade, 'quantity', 0.0)

            # Preferir weighted avg / total_quantity si están presentes (pyramiding)
            weighted = getattr(trade, 'weighted_avg_price', None)
            total_qty = getattr(trade, 'total_quantity', None)

            # Si tenemos valores válidos para pyramiding, úsalos; si no, usar original
            if total_qty and float(total_qty) > 0 and weighted and float(weighted) > 0:
                entry_price = float(weighted)
                quantity = float(total_qty)
                logger.debug(f"Usando weighted_avg_price para cierre: entry_price={entry_price}, quantity={quantity}")
            else:
                entry_price = float(orig_entry_price)
                quantity = float(orig_quantity)
                logger.debug(f"Usando entry_price original para cierre: entry_price={entry_price}, quantity={quantity}")
            # --- end patch ---

            trade_type = getattr(trade, 'trade_type', 'LONG')

            logger.info("   Entry price: %s | Exit price: %s | Quantity: %s", entry_price, exit_price, quantity)

            if exit_price is None or exit_price <= 0:
                logger.error("❌ Precio de salida inválido para %s: %s", symbol, exit_price)
                exit_price = entry_price
                logger.warning("   Usando entry_price como fallback: %s", exit_price)

            # Calcular pnl
            if trade_type == "LONG":
                pnl_sin_fees = (exit_price - entry_price) * quantity
                roi = ((exit_price - entry_price) / entry_price) * 100 if entry_price else 0.0
                
                logger.debug("LONG PnL calc para %s", symbol)
            else:
                pnl_sin_fees = (entry_price - exit_price) * quantity
                roi = ((entry_price - exit_price) / entry_price) * 100 if entry_price else 0.0
                logger.debug("SHORT PnL calc para %s", symbol)

            entry_fee = entry_price * quantity * FEE_RATE
            exit_fee = exit_price * quantity * FEE_RATE
            total_fees = entry_fee + exit_fee
           
            result = pnl_sin_fees - total_fees
            roi=(result/(entry_price*quantity))*100

            logger.info("💸 Entry fee: %.6f | Exit fee: %.6f | Total fees: %.6f", entry_fee, exit_fee, total_fees)
            logger.info("💰 RESULTADO NETO: %.6f", result)

            # Actualizar balance del bot (si tu bot maneja concurrencia, añade lock en bot)
            balance_before = getattr(self.bot, 'balance', 0.0)
            try:
                self.bot.balance = getattr(self.bot, 'balance', 0.0) + result
            except Exception:
                logger.exception("No se pudo actualizar bot.balance para %s", symbol)

            combined_roi = None
            if getattr(trade, 'is_recovery_mode', False) and hasattr(self.bot, 'calculate_combined_recovery_roi'):
                try:
                    combined_roi = self.bot.calculate_combined_recovery_roi(trade, exit_price)
                except Exception:
                    logger.debug("calculate_combined_recovery_roi falló (ignorado)")

            completed_trade = {
                'symbol': symbol,
                'type': trade_type,
                'entry_time': getattr(trade, 'entry_time', None),
                'exit_time': datetime.now(),
                'entry_price': entry_price,
                'exit_price': exit_price,
                'quantity': quantity,
                'roi': roi,
                'result': result,
                'pnl_sin_fees': pnl_sin_fees,
                'fees': total_fees,
                'entry_fee': entry_fee,
                'exit_fee': exit_fee,
                'reason': reason,
                'bars_held': getattr(trade, 'bars_held', None),
                'tp_sl_updates': len(getattr(trade, 'tp_sl_history', []) or []),
                'highest_price': getattr(trade, 'highest_price', None),
                'lowest_price': getattr(trade, 'lowest_price', None),
                'tp_sl_history': (getattr(trade, 'tp_sl_history', []) or []).copy(),
                'is_recovery': getattr(trade, 'is_recovery_mode', False),
                'recovery_attempts': getattr(trade, 'recovery_attempts', 0),
                'combined_roi': combined_roi
            }

            # Guardar en bot.completed_trades si existe la lista
            try:
                if not hasattr(self.bot, 'completed_trades'):
                    self.bot.completed_trades = []
                trades_antes = len(self.bot.completed_trades)
                self.bot.completed_trades.append(completed_trade)
                trades_despues = len(self.bot.completed_trades)
                logger.info("   Trades antes: %d | Trades después: %d", trades_antes, trades_despues)
            except Exception:
                logger.exception("No se pudo guardar completed_trade para %s", symbol)

            # Actualizar state manager
            try:
                self.state_manager.update_trade_state(symbol, getattr(TradeState, 'CLOSED', 'CLOSED'))
                logger.info("   Estado actualizado a CLOSED para %s", symbol)
            except Exception:
                logger.exception("No se pudo actualizar state_manager para %s", symbol)

            # Log resumen
            status_emoji = "✅" if result > 0 else "❌"
            recovery_tag = " [RECUPERACIÓN]" if getattr(trade, 'is_recovery_mode', False) else ""
            pyramid_levels = len(getattr(trade, 'pyramid_levels', []))
            pyramid_tag = f" [🔥 PYRAMID x{pyramid_levels+1}]" if pyramid_levels > 0 else ""
            weighted_avg = getattr(trade, 'weighted_avg_price', entry_price)
            
            logger.info("%s OPERACIÓN CERRADA%s%s: %s %s", status_emoji, recovery_tag, pyramid_tag, trade_type, symbol)
            if pyramid_levels > 0:
                logger.info("   Precio promedio: %.6f | Niveles pyramid: %d", weighted_avg, pyramid_levels)
            logger.info("   Entry: %.6f → Exit: %.6f | ROI: %.2f%% | PnL: %.6f | Fees: %.6f", entry_price, exit_price, roi, pnl_sin_fees, total_fees)
            logger.info("   Balance: %.6f → %.6f", balance_before, getattr(self.bot, 'balance', 0.0))
            logger.info("   Razón: %s", reason)

            # Limpieza asíncrona y no bloqueante
            def cleanup_delayed():
                time.sleep(2)
                try:
                    self.state_manager.cleanup_symbol(symbol)
                except Exception:
                    logger.debug("cleanup_symbol falló para %s (ignorado)", symbol)
                try:
                    self.state_manager.unmark_closing(symbol)
                except Exception:
                    logger.debug("unmark_closing falló para %s (ignorado)", symbol)
                logger.debug("Limpieza completada para %s", symbol)

            threading.Thread(target=cleanup_delayed, daemon=True).start()
            logger.info("⏰ Limpieza programada para %s (2s)", symbol)

            return True

        except Exception as e:
            logger.exception("❌ ERROR EN _record_closed_position para %s: %s", symbol, e)
            try:
                self.state_manager.unmark_closing(symbol)
            except Exception:
                pass
            return False

    def _update_tp_sl(self, cmd):
        symbol = cmd.symbol
        logger.info("🔁 UPDATE_TP_SL worker: %s", symbol)

        if self.simulate:
            logger.info("🧪 (Simulado) Actualización TP/SL %s", symbol)
            return

        try:
            with self._api_lock:
                logger.info("🔄 TP/SL actualizado para %s: TP=%s, SL=%s", symbol, cmd.data.get('tp'), cmd.data.get('sl'))
        except Exception as e:
            logger.exception("Error actualizando TP/SL para %s: %s", symbol, e)

# ==================== BOT PRINCIPAL ====================

class HeikinAshiTradingBot:
    """
    Bot de trading basado en estrategia Heikin Ashi con EMAs dinámicas
    🆕 CON SISTEMA DE RECUPERACIÓN MEJORADO
    """
    def __init__(self, api_key: str, api_secret: str, testnet: bool = False, simulate: bool = True):
        self.simulate = simulate
        self.api = BinanceAPI(api_key, api_secret, testnet=testnet)
        self.base_url = "https://fapi.binance.com"
        self.session = requests.Session()
        
        # Gestión de estado
        self.state_manager = TradeStateManager()
        
        # IMPORTANTE: Pasar 'self' (la instancia del bot) al OrderExecutor
        self.order_executor = OrderExecutor(self.api, self.state_manager, self, simulate)
        
        # Cache y datos MEJORADO con WebSocket
        self.data_cache = DataCache()
        self._symbol_filters = {}
        self._lev_brackets = {}
        
        # Trades completados
        self.completed_trades = []
        self.balance = 0
        self.inversion_posiciones = True
        self.inversion_posiciones_PROBABLE = True
        self.Bandera_de_Cierre_por_target = False
        
        # 🆕 NUEVO SISTEMA DE RACHA POR ROI
        self.racha_roi = {
            'cierres': [],  # Lista de resultados: 'positivo' o 'negativo'
            'count': 0       # Contador de cierres por ROI
        }

        # 🆕 BLOQUEO TEMPORAL DE OPERACIONES
        self.trading_paused = False
        self.pause_until = None
        self.pause_lock = threading.Lock()
        
        # STOP-LOSS GLOBAL
        self.emergency_stop = False
        self.daily_start_balance = 0
        
        # Control
        self.running = False
        self.monitored_symbols = set()
        self.top_symbols = []
        
        # Colas
        self.signal_queue = Queue(maxsize=50)
        self.exit_queue = Queue(maxsize=50)


                # 🎯 NUEVO: Sistema de Profit Targets
        self.profit_target_manager = ProfitTargetManager(base_amount=20, wait_hours=0.1, bot=self, consider_unrealized=True, use_net_estimate=True)
        self.in_cooldown = False
        self.cooldown_until = None
        self.cooldown_lock = threading.Lock()

        # mapa de overrides por símbolo: { 'SYMBOL': {'invert_until': datetime, 'active': True} }
        self.symbol_signal_inversion_overrides = {}

        
        # Threads
        self.price_thread = None
        self.strategy_thread = None
        self.execution_thread = None
        self.monitor_thread = None
        self.roi_thread = None
        self.kline_ws_cache = None  # se inicializa en run()

    def calculate_current_roi(self, trade: TradeInfo, current_price: float) -> float:
        """
        Calcula el ROI actual de un trade
        🔥 ACTUALIZADO: Usa precio promedio ponderado si hay pyramiding
        """
        # Usar precio promedio ponderado si está disponible (pyramiding)
        if hasattr(trade, 'weighted_avg_price') and trade.weighted_avg_price > 0:
            entry_price = trade.weighted_avg_price
        else:
            entry_price = trade.entry_price
            
        if trade.trade_type == "LONG":
            roi = ((current_price - entry_price) / entry_price) * 100
        else:
            roi = ((entry_price - current_price) / entry_price) * 100
        return roi
    
    def calculate_combined_recovery_roi(self, recovery_trade: TradeInfo, current_price: float) -> float:
        """
        🆕 CALCULA EL ROI REAL COMBINADO basado en PnL total
        """
        if not recovery_trade.is_recovery_mode or recovery_trade.original_entry_price == 0:
            return self.calculate_current_roi(recovery_trade, current_price)
        
        # 1. PnL de la operación original (ya cerrada)
        original_trade_type = "SHORT" if recovery_trade.trade_type == "LONG" else "LONG"
        original_exit_price = recovery_trade.original_exit_price
        
        if original_trade_type == "LONG":
            # La original era LONG y se cerró con pérdida
            original_pnl = (original_exit_price - recovery_trade.original_entry_price) * recovery_trade.quantity
        else:
            # La original era SHORT y se cerró con pérdida
            original_pnl = (recovery_trade.original_entry_price - original_exit_price) * recovery_trade.quantity
        
        # 2. PnL de la operación de recuperación actual
        if recovery_trade.trade_type == "LONG":
            recovery_pnl = (current_price - recovery_trade.entry_price) * recovery_trade.quantity
        else:
            recovery_pnl = (recovery_trade.entry_price - current_price) * recovery_trade.quantity
        
        # 3. PnL combinado
        combined_pnl = original_pnl + recovery_pnl
        
        # 4. ROI combinado = PnL combinado / capital invertido original
        original_invested = recovery_trade.original_entry_price * recovery_trade.quantity
        combined_roi = (combined_pnl / original_invested) * 100
        
        logger.debug(f"📊 ROI Combinado {recovery_trade.symbol}:")
        logger.debug(f"   Original PnL: ${original_pnl:.2f}")
        logger.debug(f"   Recovery PnL: ${recovery_pnl:.2f}")
        logger.debug(f"   Combined: ${combined_pnl:.2f} ({combined_roi:.2f}%)")
        
        return combined_roi
    
    def log_recovery_trigger(self, trade: TradeInfo, current_price: float, trigger_reason: str):
        """🆕 Log detallado del trigger de recuperación"""
        logger.warning("=" * 60)
        logger.warning("🔥 RECUPERACIÓN ACTIVADA - ANÁLISIS DETALLADO")
        logger.warning("=" * 60)
        logger.warning(f"Símbolo: {trade.symbol}")
        logger.warning(f"Trade original: {trade.trade_type}")
        logger.warning(f"Precio entrada: ${trade.entry_price:.4f}")
        logger.warning(f"Precio actual: ${current_price:.4f}")
        logger.warning(f"ROI actual: {self.calculate_current_roi(trade, current_price):.2f}%")
        logger.warning(f"Diferencia: ${abs(current_price - trade.entry_price):.4f}")
        logger.warning(f"Velas mantenidas: {trade.bars_held}")
        logger.warning(f"Ajustes TP/SL: {len(trade.tp_sl_history)}")
        logger.warning(f"Highest: ${trade.highest_price:.4f}")
        logger.warning(f"Lowest: ${trade.lowest_price:.4f}")
        logger.warning(f"Intento de recuperación: {trade.recovery_attempts + 1}/{trade.max_recovery_attempts}")
        logger.warning(f"Motivo: {trigger_reason}")
        logger.warning("=" * 60)
    
    def check_emergency_stop(self) -> bool:
        """🆕 Verifica si se alcanzó el límite de pérdida global"""
        if not EMERGENCY_STOP_ENABLED:
            return False
        
        if self.emergency_stop:
            return True
        
        current_loss = self.balance - self.daily_start_balance
        
        if current_loss < MAX_DAILY_LOSS:
            logger.critical("=" * 60)
            logger.critical("🚨 STOP DE EMERGENCIA ACTIVADO")
            logger.critical("=" * 60)
            logger.critical(f"Pérdida diaria alcanzada: ${current_loss:.2f}")
            logger.critical(f"Límite configurado: ${MAX_DAILY_LOSS:.2f}")
            logger.critical("Cerrando todas las posiciones...")
            logger.critical("=" * 60)
            
            self.emergency_stop = True
            self.close_all_positions_emergency()
            return True
        
        return False
    
    def close_all_positions_emergency(self):
        """🆕 Cierra todas las posiciones en emergencia"""
        active_symbols = list(self.state_manager.get_all_active_symbols())
        
        for symbol in active_symbols:
            try:
                trade = self.state_manager.get_trade(symbol)
                if trade and not self.state_manager.is_closing(symbol):
                    logger.critical(f"🚨 Cerrando {symbol} por emergencia")
                    
                    # Marcar como cerrando
                    self.state_manager.mark_closing(symbol)
                    
                    # Enviar comando de cierre
                    cmd = OrderCommandData(
                        command="CLOSE_POSITION",
                        symbol=symbol,
                        data={'reason': 'EMERGENCY_STOP'}, 
                        trade_id=trade.trade_id
                    )
                    self.order_executor.submit_command(cmd)
                    
            except Exception as e:
                logger.error(f"Error cerrando {symbol} en emergencia: {e}")
        
        # Detener el bot
        self.running = False

    # ==================== OBTENCIÓN DE DATOS ====================
    
    def get_futures_symbols(self) -> List[str]:
        """Obtiene símbolos de futuros activos"""
        try:
            url = f"{self.base_url}/fapi/v1/exchangeInfo"
            response = self.session.get(url, timeout=10)
            data = response.json()
            symbols = []
            for symbol_info in data['symbols']:
                if (symbol_info['status'] == 'TRADING' and 
                    symbol_info['contractType'] == 'PERPETUAL' and
                    symbol_info['symbol'].endswith('USDT')):
                    symbols.append(symbol_info['symbol'])
            return symbols
        except Exception as e:
            logger.error(f"Error obteniendo símbolos: {e}")
            return []
    
    def get_24h_ticker_stats(self) -> pd.DataFrame:
        """Obtiene estadísticas 24h"""
        try:
            url = f"{self.base_url}/fapi/v1/ticker/24hr"
            response = self.session.get(url, timeout=10)
            data = response.json()
            df = pd.DataFrame(data)
            df['priceChangePercent'] = pd.to_numeric(df['priceChangePercent'])
            df['volume'] = pd.to_numeric(df['volume'])
            df['lastPrice'] = pd.to_numeric(df['lastPrice'])
            df = df[df['symbol'].str.endswith('USDT')]
            return df.sort_values('priceChangePercent', ascending=False)
        except Exception as e:
            logger.error(f"Error obteniendo estadísticas 24h: {e}")
            return pd.DataFrame()
    
    def get_top_gainers_losers(self, df: pd.DataFrame, top_n: int = 20) -> Tuple[List[str], List[str]]:
        """Obtiene top gainers y losers"""
        min_volume = df['volume'].quantile(0.3)
        df_filtered = df[df['volume'] >= min_volume]
        top_gainers = df_filtered.head(top_n)['symbol'].tolist()
        top_losers = df_filtered.tail(top_n)['symbol'].tolist()
        return top_gainers, top_losers
    
    def update_monitored_symbols(self):
        """
        VERSIÓN CORREGIDA: Mantiene símbolos con operaciones activas
        incluso después de actualizar la lista de monitoreo
        """
        try:
            # Asegurar que monitored_symbols existe y es un set
            if not hasattr(self, 'monitored_symbols') or self.monitored_symbols is None:
                self.monitored_symbols = set()

            # 1. Obtener símbolos con operaciones ACTIVAS
            active_symbols = set(self.state_manager.get_all_active_symbols() or set())
            logger.info(f"Símbolos con operaciones activas: {len(active_symbols)}")
            if active_symbols:
                logger.info("   " + ", ".join(sorted(active_symbols)))

            # 2. Obtener los top gainers/losers
            ticker_stats = self.get_24h_ticker_stats()
            if ticker_stats.empty:
                logger.warning("No se pudieron obtener estadísticas, manteniendo símbolos actuales")
                # Mantener símbolos activos aunque falle la actualización
                self.monitored_symbols = set(self.monitored_symbols).union(active_symbols)
                return

            # 3. Obtener top gainers/losers
            top_gainers, top_losers = self.get_top_gainers_losers(ticker_stats, 40)
            new_symbols_from_market = set((top_gainers or []) + (top_losers or []))

            # 4. COMBINAR: Nuevos símbolos + Símbolos activos
            combined_symbols = new_symbols_from_market.union(active_symbols)

            # 5. Actualizar
            self.monitored_symbols = combined_symbols
            self.top_symbols = list(combined_symbols)


            logger.info(f"Símbolos de mercado (top gainers/losers): {len(new_symbols_from_market)}")
            logger.info(f"Símbolos activos (operaciones abiertas): {len(active_symbols)}")
            logger.info(f"TOTAL símbolos monitoreados: {len(self.monitored_symbols)}")

            # 6. Log de símbolos removidos/añadidos (para debugging)
            previous = getattr(self, '_previous_monitored_symbols', set())
            removed = previous - self.monitored_symbols
            added = self.monitored_symbols - previous

            if removed:
                logger.debug(f"   ➖ Removidos: {', '.join(sorted(removed))}")
            if added:
                logger.debug(f"   ➕ Añadidos: {', '.join(sorted(added))}")

            self._previous_monitored_symbols = set(self.monitored_symbols)

            #     # 🆕 Sincronizar kline cache si los símbolos cambiaron
            self._sync_kline_cache()

        except Exception as e:
            logger.error(f"Error actualizando símbolos monitoreados: {e}")
            # En caso de error, mantener los actuales
            logger.warning("Manteniendo símbolos anteriores por seguridad")
    
    def get_klines(self, symbol: str, interval: str, limit: int = 1500) -> pd.DataFrame:
        """Obtiene datos de velas"""
        try:
            url = f"{self.base_url}/fapi/v1/klines"
            params = {'symbol': symbol, 'interval': interval, 'limit': limit}
            response = self.session.get(url, params=params, timeout=6)
            data = response.json()
            
            columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume',
                      'close_time', 'quote_volume', 'trades', 'taker_buy_volume',
                      'taker_buy_quote_volume', 'ignore']
            df = pd.DataFrame(data, columns=columns)
            
            numeric_columns = ['open', 'high', 'low', 'close', 'volume']
            for col in numeric_columns:
                df[col] = pd.to_numeric(df[col])
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            
            return df[['timestamp', 'open', 'high', 'low', 'close', 'volume']]
        except Exception as e:
            logger.debug(f"Error obteniendo datos para {symbol}: {e}")
            return pd.DataFrame()

    def _rest_request(self, method, path, **kwargs):
        """Request REST básico con firma"""
        url = f"{self.base_url}{path}"
        
        headers = kwargs.pop("headers", {}) or {}
        headers["X-MBX-APIKEY"] = self.api.client.API_KEY
        
        resp = self.session.request(
            method, url,
            headers=headers,
            timeout=kwargs.get("timeout", 8),
            **kwargs
        )
        
        return resp

    def _sign_params(self, params: dict) -> str:
        """Devuelve query string firmado"""
        params = params.copy()
        params["timestamp"] = int(time.time() * 1000)
        query = urllib.parse.urlencode(params)
        
        signature = hmac.new(
            self.api.client.API_SECRET.encode(),
            query.encode(),
            hashlib.sha256
        ).hexdigest()
        
        return f"{query}&signature={signature}"

    def _get_symbol_filters(self, symbol: str) -> Optional[dict]:
        """
        Obtiene filtros (minNotional, lotSize) y los cachea.
        Retorna dict con: minQty, maxQty, stepSize, minNotional
        """
        symbol = symbol.upper().strip()
        
        # Inicializar cache si no existe
        if not hasattr(self, "_symbol_filters"):
            self._symbol_filters = {}
        
        # Devolver desde cache si existe
        if symbol in self._symbol_filters:
            return self._symbol_filters[symbol]
        
        # Pedir exchangeInfo
        qs = self._sign_params({"symbol": symbol})
        resp = self._rest_request("GET", f"/fapi/v1/exchangeInfo?{qs}")
        
        if not resp or not resp.ok:
            return None
        
        try:
            data = resp.json()
        except Exception:
            logger.error("No se pudo parsear JSON de exchangeInfo")
            return None
        
        symbols_data = data.get("symbols") if isinstance(data, dict) else None
        if not symbols_data:
            return None
        
        for d in symbols_data:
            sym = d.get("symbol", "").upper()
            if not sym:
                continue
            
            filters = {f["filterType"]: f for f in d.get("filters", [])}
            
            # Extraer LOT_SIZE
            lot = filters.get("LOT_SIZE", {})
            min_qty = float(lot.get("minQty", 0))
            max_qty = float(lot.get("maxQty", 0))
            step_qty = float(lot.get("stepSize", 0))
            
            # Adjuntar valores útiles
            filters["minQty"] = min_qty
            filters["maxQty"] = max_qty
            filters["stepSize"] = step_qty
            
            # MIN_NOTIONAL
            min_notional = None
            if "MIN_NOTIONAL" in filters:
                try:
                    min_notional = float(filters["MIN_NOTIONAL"].get("notional", 0))
                except:
                    pass
            filters["minNotional"] = min_notional
            
            # PRICE_FILTER para tickSize
            price_filter = filters.get("PRICE_FILTER", {})
            filters["tickSize"] = float(price_filter.get("tickSize", 0))
            
            self._symbol_filters[sym] = filters
        
        return self._symbol_filters.get(symbol)

    def _load_leverage_brackets(self, symbol: str) -> Optional[list]:
        """Consulta leverage brackets para el símbolo"""
        symbol = symbol.upper().strip()
        qs = self._sign_params({"symbol": symbol})
        resp = self._rest_request("GET", f"/fapi/v1/leverageBracket?{qs}")
        
        if not resp or not resp.ok:
            return None
        
        try:
            data = resp.json()
        except Exception as e:
            logger.error(f"No se pudo parsear JSON leverageBracket: {e}")
            return None
        
        # Normalizar respuesta
        if isinstance(data, dict):
            return data.get("brackets", [])
        elif isinstance(data, list):
            for d in data:
                if d.get("symbol", "").upper() == symbol:
                    return d.get("brackets", [])
            if data:
                return data[0].get("brackets", [])
        
        return None

    def _max_leverage_for_symbol(self, symbol: str) -> Optional[int]:
        """
        Retorna el leverage máximo disponible para el símbolo.
        Usa cache acumulativo.
        """
        symbol = symbol.upper().strip()
        
        # Inicializar cache
        if not hasattr(self, "_lev_brackets"):
            self._lev_brackets = {}
        
        # Devolver desde cache si existe
        if symbol in self._lev_brackets:
            br = self._lev_brackets[symbol]
            try:
                return int(br[0].get("initialLeverage", 20))
            except Exception:
                return None
        
        # Consultar API
        qs = self._sign_params({"symbol": symbol})
        resp = self._rest_request("GET", f"/fapi/v1/leverageBracket?{qs}")
        
        if not resp or not resp.ok:
            return None
        
        try:
            data = resp.json()
        except Exception:
            logger.error("No se pudo parsear JSON de leverageBracket")
            return None
        
        # Normalizar
        if isinstance(data, dict):
            data = [data]
        
        if not isinstance(data, list) or not data:
            return None
        
        # 🚀 ACUMULAR en cache
        for d in data:
            sym = d.get("symbol", "").upper()
            if sym:
                self._lev_brackets[sym] = d.get("brackets", [])
        
        br = self._lev_brackets.get(symbol)
        if not br:
            return None
        
        try:
            return int(br[0].get("initialLeverage", 20))
        except Exception:
            return None

    def _get_min_notional_for_leverage(self, symbol: str, leverage: int) -> Optional[float]:
        """
        Retorna el notional mínimo para el leverage dado según brackets.
        """
        brackets = self._load_leverage_brackets(symbol)
        if not brackets:
            return None
        
        for br in brackets:
            init_lev = br.get("initialLeverage")
            if init_lev is None:
                continue
            
            try:
                init_lev = int(init_lev)
            except:
                continue
            
            if leverage <= init_lev:
                floor = br.get("notionalFloor")
                if floor is not None:
                    try:
                        return float(floor)
                    except:
                        pass
                
                cap = br.get("notionalCap")
                if cap is not None:
                    try:
                        return float(cap)
                    except:
                        pass
                break
        
        # Fallback: primer bracket
        first = brackets[0]
        fl = first.get("notionalFloor")
        if fl is not None:
            try:
                return float(fl)
            except:
                pass
        
        return None   
   
    # ==================== ANÁLISIS DE SEÑALES HEIKIN ASHI ====================
    
    
    def calculate_bollinger_bands(self, df, ema_period=70, std_dev=1.2):
        """
        Calcula bandas de Bollinger usando una EMA como 'middle' (según tu especificación).
        Retorna (ema, upper, lower) como pd.Series.
        """
        try:
            ema = calculate_ema(df['close'], ema_period)
            std = df['close'].rolling(window=ema_period).std()
            upper = ema + std * std_dev
            lower = ema - std * std_dev
            return ema, upper, lower
        except Exception as e:
            logger.debug(f"Error en calculate_bollinger_bands para dataframe: {e}")
            return pd.Series(), pd.Series(), pd.Series()


    def add_indicators(self, df):
    
        df = df.copy()
        # nota: 'ema_200' usa periodo 300 (según tu snippet original)
        df['ema_200'] = calculate_ema(df['close'], 300)
        df['ema_70'] = calculate_ema(df['close'], 70)
        df['ema_20'] = calculate_ema(df['close'], 20)
        # ── Williams %R ──────────────────────────────────────────
        df['williams_r'] = calculate_williams_r(df, period=300)

        df['bb_middle'], df['bb_upper'], df['bb_lower'] = self.calculate_bollinger_bands(df, ema_period=70, std_dev=0.5)
        # Version 'V' idéntica (sigues usando la misma configuración en el snippet)
        df['bb_middleV'], df['bb_upperV'], df['bb_lowerV'] = self.calculate_bollinger_bands(df, ema_period=70, std_dev=0.5)
        # Evitar división por cero más adelante: asegurar >0
        df['bb_middleV'] = df['bb_middleV'].replace(0, np.nan)
        df['bb_width'] = (df['bb_upperV'] - df['bb_lowerV']) / df['bb_middleV']
        
            # ── Momentum ─────────────────────────────────────────────────
        df['momentum']     = calculate_momentum(df['close'], period=10)   # corto plazo
        df['momentum_slow']= calculate_momentum(df['close'], period=70)   # alineado con tu EMA lenta

        # Momentum normalizado (útil para comparar activos con precios distintos)
        df['momentum_pct'] = df['close'].pct_change(14) * 100
        
        df.dropna(inplace=True)
        return df

    def check_long_entry(self, df: pd.DataFrame, idx: int = -2, symbol: str = "BTCUSDT") -> bool:
        """
        Señal LONG por breakout real hacia arriba de BB_upper.
        Condiciones:
          1. open actual > bb_upper  (rompe la banda superior)
          2. open actual > ema_200   (a favor de la tendencia)
          3. bb_width > umbral       (bandas con amplitud mínima)
          4. Las BB_LOOKBACK_CANDLES velas anteriores estuvieron
             DENTRO de las bandas  (bb_lower <= open <= bb_upper)
             → confirma que fue un squeeze antes del breakout
        """
        try:
            symbol = symbol.upper()
            row = df.iloc[-1]
            # Condiciones básicas de la vela actual
            basic = (
                row['open'] > row['bb_upper'] and   # breakout por arriba
                row['open'] > row['ema_200'] and     # tendencia alcista
                row['bb_width'] > 0.002312            # bandas con cuerpo suficiente
            )
            if not basic:
                return False
            # Verificar que las N velas previas estuvieron dentro de las bandas
            n = BB_LOOKBACK_CANDLES
            if abs(idx) + n > len(df):              # no hay suficiente historial
                return False
            prev_inside = all(
                df.iloc[idx - i]['bb_lower'] <= df.iloc[idx - i]['open'] <= df.iloc[idx - i]['bb_upper']
                for i in range(1, n + 1)
            )
            return prev_inside
        except Exception:
            return False

    def check_short_entry(self, df: pd.DataFrame, idx: int = -2, symbol: str = "BTCUSDT") -> bool:
        """
        Señal SHORT por breakout real hacia abajo de BB_lower.
        Condiciones:
          1. open actual < bb_lower  (rompe la banda inferior)
          2. open actual < ema_200   (a favor de la tendencia bajista)
          3. bb_width > umbral       (bandas con amplitud mínima)
          4. Las BB_LOOKBACK_CANDLES velas anteriores estuvieron
             DENTRO de las bandas  (bb_lower <= open <= bb_upper)
             → confirma squeeze antes del breakout
        """
        try:
            symbol = symbol.upper()
            row = df.iloc[-1]
            basic = (
                row['open'] < row['bb_lower'] and   # breakout por abajo
                row['open'] < row['ema_200'] and     # tendencia bajista
                row['bb_width'] > 0.002312
            )
            if not basic:
                return False
            n = BB_LOOKBACK_CANDLES
            if abs(idx) + n > len(df):
                return False
            prev_inside = all(
                df.iloc[idx - i]['bb_lower'] <= df.iloc[idx - i]['open'] <= df.iloc[idx - i]['bb_upper']
                for i in range(1, n + 1)
            )
            return prev_inside
        except Exception:
            return False

    def check_long_exit(self, row):
        return row['open'] < row['bb_lower']

    def check_short_exit(self, row):
        return row['open'] > row['bb_upper']

    def check_btc_ema20_and_invert(self, symbol: str = "BTCUSDT"):
        """
        Verifica si el precio actual de BTC está por encima o debajo del EMA20
        
        Si BTC > EMA20 → self.inversion_posiciones = False (estrategia normal)
        Si BTC < EMA20 → self.inversion_posiciones = True (estrategia invertida)
        
        Args:
            symbol: Símbolo a verificar (por defecto BTCUSDT)
        
        Returns:
            dict con información del análisis
        """
        try:
            logger.info("=" * 60)
            logger.info(f"🔍 VERIFICANDO {symbol} vs EMA20...")
            logger.info("=" * 60)
            
            # 1. Obtener datos de 1 minuto
            df_1m = self.get_klines(symbol, '1m', 1500)
            
            if df_1m is None or df_1m.empty or len(df_1m) < EMA_PERIOD_BTC + 5:
                logger.error(f"❌ No hay suficientes datos para {symbol}")
                return None
            
            # 2. Convertir a Heikin Ashi
            ha_df = calculate_heikin_ashi(df_1m)
            if ha_df.empty:
                logger.error(f"❌ Error convirtiendo a Heikin Ashi para {symbol}")
                return None
            
            # 3. Añadir EMAs
            ha_df = add_heikin_ashi_indicators(ha_df, EMA_PERIOD_BTC)
            if ha_df.empty or len(ha_df) < EMA_PERIOD_BTC + 2:
                logger.error(f"❌ Error calculando EMAs para {symbol}")
                return None
            
            # 4. Obtener la última vela cerrada
            last_bar = ha_df.iloc[-2]  # Última vela completamente cerrada
            
            # 5. Obtener precio actual
            current_price = self.data_cache.get_current_price(symbol)
            if current_price is None:
                current_price = float(last_bar['ha_close'])
            
            # 6. Obtener EMA20
            ema20_high = float(last_bar['ema_high'])
            ema20_low = float(last_bar['ema_low'])
            ema20_avg = (ema20_high + ema20_low) / 2
            
            # 7. Comparar precio con EMA20
            above_ema = current_price > ema20_avg
            
            # 8. Determinar nueva configuración
            previous_inversion = self.inversion_posiciones
            
            if above_ema:
                self.inversion_posiciones = False
                decision = "✅ MODO NORMAL (No invertido)"
            else:
                self.inversion_posiciones = True
                decision = "🔄 MODO INVERTIDO"
            
            # 9. Log detallado
            difference = current_price - ema20_avg
            pct_difference = (difference / ema20_avg) * 100
            
            logger.info(f"\n📊 ANÁLISIS DE {symbol}:")
            logger.info(f"   Precio actual: ${current_price:.2f}")
            logger.info(f"   EMA20 (promedio): ${ema20_avg:.2f}")
            logger.info(f"   EMA20 (high): ${ema20_high:.2f}")
            logger.info(f"   EMA20 (low): ${ema20_low:.2f}")
            logger.info(f"   Diferencia: ${difference:.2f} ({pct_difference:.2f}%)")
            logger.info(f"\n   Posición respecto a EMA20: {'ARRIBA ⬆️' if above_ema else 'ABAJO ⬇️'}")
            logger.info(f"   {decision}")
            
            # 10. Mostrar si hubo cambio
            if previous_inversion != self.inversion_posiciones:
                logger.warning(f"\n   🔄 CAMBIO DETECTADO:")
                logger.warning(f"      Antes: {'Invertido 🔄' if previous_inversion else 'Normal ✅'}")
                logger.warning(f"      Ahora: {'Invertido 🔄' if self.inversion_posiciones else 'Normal ✅'}")
            else:
                logger.info(f"\n   ℹ️ Sin cambios (manteniendo configuración actual)")
            
            logger.info("=" * 60)
            
            # 11. Retornar información del análisis
            return {
                'symbol': symbol,
                'current_price': current_price,
                'ema20': ema20_avg,
                'ema20_high': ema20_high,
                'ema20_low': ema20_low,
                'above_ema': above_ema,
                'difference': difference,
                'pct_difference': pct_difference,
                'inversion_posiciones': self.inversion_posiciones,
                'changed': previous_inversion != self.inversion_posiciones,
                'decision': decision,
                'timestamp': datetime.now()
            }
            
        except Exception as e:
            logger.error(f"❌ Error en check_btc_ema20_and_invert: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return None

    def btc_ema20_monitor_thread(self, check_interval: int = 180):
        """
        🔄 Thread que verifica periódicamente el precio de BTC vs EMA20
        
        Se ejecuta cada 5 minutos (configurable) y ajusta automáticamente
        la estrategia según la posición del precio
        
        Args:
            check_interval: Segundos entre verificaciones (default: 300 = 5 minutos)
        """
        logger.info("🔄 Iniciando monitor BTC/EMA20...")
        
        while self.running:
            try:
                # Verificar BTC vs EMA20
                result = self.check_btc_ema20_and_invert("BTCUSDT")
                
                if result:
                    logger.debug(f"✅ Verificación completada: Inversión={'Activada' if result['inversion_posiciones'] else 'Desactivada'}")
                else:
                    logger.warning("⚠️ No se pudo completar la verificación de BTC/EMA20")
                
                # Esperar hasta la próxima verificación
                time.sleep(check_interval)
                
            except Exception as e:
                logger.error(f"❌ Error en btc_ema20_monitor_thread: {e}")
                time.sleep(30)  # Retentar en 30 segundos en caso de error

    def analyze_heikin_ashi_signal(self, symbol: str) -> Dict:

        try:
            df_1m, _ = self.data_cache.get_data(symbol)
            if df_1m is None or df_1m.empty or len(df_1m) < 100:
                return {}
            df = self.add_indicators(df_1m)
            if df is None or df.empty or len(df) < 3:
                return {}
            # Usar la vela cerrada anterior para evitar señales sobre vela en formación
            last_bar = df.iloc[-2]
            signal_type = None

            # if self.inversion_posiciones:
            #     if self.check_short_entry(df, -2,symbol):
            #         signal_type = "LONG"
            #     elif self.check_long_entry(df, -2,symbol):
            #         signal_type = "SHORT"
            #     else:
            #         return {}
            # else:
            if self.check_long_entry(df, -2,symbol): # and last_bar['open'] > last_bar['ema_20'] and last_bar['ema_20'] > last_bar['ema_70'] and last_bar['williams_r'] > -40:
                    signal_type = "LONG"
            elif self.check_short_entry(df, -2,symbol): # and last_bar['open'] < last_bar['ema_20'] and last_bar['ema_20'] < last_bar['ema_70'] and last_bar['williams_r'] < -60:
                    signal_type = "SHORT"
            else:
                    return {}                
            
            # -------------- Nuevo check: historial de pérdidas por símbolo --------------
            # try:
            #     if signal_type and self._should_invert_signal_for_symbol(symbol, signal_type):
            #         # invertir señal
            #         original_signal = signal_type
            #         signal_type = "LONG" if signal_type == "SHORT" else "SHORT"
            #         logger.warning(f"🔄 Señal invertida para {symbol}: {original_signal} -> {signal_type} (por historial de pérdidas)")
            # except Exception as e:
            #     logger.debug(f"Error aplicando inversión por historial para {symbol}: {e}")
            # ---------------------------------------------------------------------------
            
            if signal_type == "LONG" and self.inversion_posiciones_PROBABLE:
                signal_type = "SHORT"
            elif signal_type == "SHORT" and self.inversion_posiciones_PROBABLE:
                signal_type = "LONG"
                
            # ---------------------------------------------------------------------------
                
            # Precio actual preferente desde WebSocket/cache
            current_price = self.data_cache.get_current_price(symbol)
            if current_price is None:
                current_price = float(last_bar['close'])
                
            # TP/SL inicial: simple y coherente con las bandas
            if signal_type == "LONG":
                # initial_tp = float(last_bar['bb_upper'])
                # initial_sl = float(last_bar['bb_lower'])
                initial_tp = current_price/(-(ROI_CRITICAL_PROFIT/100)+1)
                initial_sl = current_price/(-(ROI_CRITICAL_LOSS/100)+1)
            else:
                # initial_tp = float(last_bar['bb_lower'])
                # initial_sl = float(last_bar['bb_upper'])
                initial_tp = current_price/((ROI_CRITICAL_PROFIT/100)+1)
                initial_sl = current_price/((ROI_CRITICAL_LOSS/100)+1)

            return {
                'symbol': symbol,
                'signal_type': signal_type,
                'current_price': float(current_price),
                'initial_tp': initial_tp,
                'initial_sl': initial_sl,
                'ema_200': float(last_bar['ema_200']),
                'bb_middle': float(last_bar['bb_middle']),
                'bb_upper': float(last_bar['bb_upper']),
                'bb_lower': float(last_bar['bb_lower']),
                'bb_width': float(last_bar['bb_width']),
                'open': float(last_bar['open']),
                'close': float(last_bar['close']),
                'high': float(last_bar['high']),
                'low': float(last_bar['low']),
            }

        except Exception as e:
            logger.debug(f"Error analizando (EMA+BB) para {symbol}: {e}")
            return {}
        
        
# ---------------- Helper: decidir si invertir señal por historial ----------------
    def _should_invert_signal_for_symbol(self, symbol: str, signal_type: str,
                                        recent_n: int = LOSS_HISTORY_LEN,
                                        loss_roi_thresh: float = LOSS_ROI_THRESHOLD,
                                        min_loss_count: int = LOSS_MIN_COUNT) -> bool:
        """
        Mira los últimos `recent_n` trades cerrados (self.completed_trades).
        Si hay al menos `min_loss_count` trades del mismo símbolo con roi <= loss_roi_thresh
        y cuyo 'type' coincide con la señal propuesta, devuelve True -> invertir señal.
        """
        try:
            if not hasattr(self, 'completed_trades') or not self.completed_trades:
                return False

            recent = self.completed_trades[-recent_n:]
            sym_trades = [t for t in recent if t.get('symbol') == symbol]

            if not sym_trades:
                return False

            # Trades perdedores del mismo símbolo y misma dirección que la señal
            bad_same_side = [t for t in sym_trades
                            if (t.get('roi') is not None) and (t.get('roi') <= loss_roi_thresh)
                            and (t.get('type') == signal_type)]

            if len(bad_same_side) >= min_loss_count:
                logger.warning(f"⚠️ Historial negativo detectado para {symbol}: {len(bad_same_side)} cierres <= {loss_roi_thresh}% en los últimos {recent_n} trades. Invirtiendo señal propuesta ({signal_type}).")
                # registrar override temporal para evitar flapping inmediato
                try:
                    self.symbol_signal_inversion_overrides[symbol] = {
                        'invert_until': datetime.now() + timedelta(seconds=SYMBOL_INVERT_TTL_SECONDS),
                        'active': True
                    }
                except Exception:
                    pass
                return True

            # también respetar override activo (si existe y no expiró) como refuerzo
            ov = self.symbol_signal_inversion_overrides.get(symbol)
            if ov and ov.get('active') and ov.get('invert_until') and datetime.now() < ov['invert_until']:
                logger.debug(f"🔁 Override inversion activo para {symbol} hasta {ov['invert_until']}")
                return True

            return False

        except Exception as e:
            logger.debug(f"Error en _should_invert_signal_for_symbol: {e}")
            return False
# ---------------------------------------------------------------------------------

    # ==================== GESTIÓN DE TRADES ====================
    
    def calculate_position_size(self, symbol: str, current_price: float,
                                leverage: int, dynamic_sl_pct: Optional[float] = None) -> float:
        """
        Calcula la cantidad (qty) mínima válida respetando:
        - minQty
        - minNotional
        - stepSize
        - leverage brackets
        """
        
        filters = self._get_symbol_filters(symbol)
        
        min_qty = float(filters.get("minQty", 0.0))
        min_notional = float(filters.get("minNotional", 0.0))
        
        # 🔹 Qty mínima por minNotional (con margen 20%)
        min_qty_by_notional = (min_notional * 1.2) / current_price if current_price > 0 else 0.0
        
        # 🔹 NUEVO: Qty mínima por leverage bracket
        min_notional_lev = self._get_min_notional_for_leverage(symbol, leverage)
        min_qty_by_leverage = 0.0
        if min_notional_lev and min_notional_lev > 0:
            min_qty_by_leverage = (min_notional_lev * 1.1) / current_price
        
        # 🔹 Escoger el MAYOR de todos
        final_qty = max(min_qty, min_qty_by_notional, min_qty_by_leverage)
        
        # 🔹 Ajustar a stepSize
        step_size = float(filters.get("stepSize", 0.0))
        if step_size > 0:
            final_qty = (final_qty // step_size) * step_size
        
        return float(final_qty)
    
    def open_trade(self, signal: Dict):
        """Abre nueva operación"""
        try:

             # 🆕 DOBLE VERIFICACIÓN ANTES DE ABRIR
            if not self.is_trading_allowed():
                logger.warning(f"⏸️ Apertura cancelada (trading pausado): {signal['symbol']}")
                return
            
            symbol = signal['symbol']
            
            # Verificar límite de operaciones concurrentes
            if len(self.state_manager.get_all_active_symbols()) >= MAX_CONCURRENT_TRADES:
                return
            
            # Verificar si ya existe trade activo
            if self.state_manager.get_trade(symbol):
                return
            
            entry_price = signal['current_price']
            trade_type = signal['signal_type']
            MAXIMO_APALANCAMIENTO= self._max_leverage_for_symbol(symbol)
            
            # Calcular cantidad
            MULTIPLICADOR= max((1+(self.balance/6.12)),1)
            quantity = (self.calculate_position_size(symbol, entry_price, MAXIMO_APALANCAMIENTO))*2
            
            if quantity <= 0:
                logger.warning(f"⚠️ Cantidad inválida para {symbol}")
                return
            
            # Crear TradeInfo
            trade_info = TradeInfo(
                symbol=symbol,
                trade_type=trade_type,
                entry_price=entry_price,
                entry_time=datetime.now(),
                current_tp=signal['initial_tp'],
                current_sl=signal['initial_sl'],
                quantity=quantity,
                state=TradeState.PENDING_OPEN,
                highest_price=entry_price if trade_type == "LONG" else 0,
                lowest_price=entry_price if trade_type == "SHORT" else float('inf')
            )
            
            # Agregar al state manager
            if not self.state_manager.add_trade(symbol, trade_info):
                return
            
            # Registrar TP/SL inicial
            self.state_manager.update_tp_sl(
                symbol, 
                signal['initial_tp'], 
                signal['initial_sl'],
                "Initial setup"
            )
            
            # Enviar comando de apertura
            side = "BUY" if trade_type == "LONG" else "SELL"
            cmd = OrderCommandData(
                command="OPEN_POSITION",
                symbol=symbol,
                data={
                    'side': side,
                    'quantity': quantity,
                    'tp': signal['initial_tp'],
                    'sl':signal ['initial_sl'],
                    'leverage': MAXIMO_APALANCAMIENTO  # <-- NUEVO          
                }
            )
            self.order_executor.submit_command(cmd)
            
            logger.info(f"🚀 NUEVA OPERACIÓN HEIKIN ASHI: {trade_type} {symbol}")
            logger.info(f"   Precio: ${entry_price:.4f} | TP: ${signal['initial_tp']:.4f} | SL: ${signal['initial_sl']:.4f}")
            logger.info("-" * 50)
            
        except Exception as e:
            logger.error(f"Error abriendo operación: {e}")
    
    def open_recovery_trade(self, original_trade: TradeInfo, current_price: float, trigger_reason: str):
        """
        🆕 ABRE UN TRADE DE RECUPERACIÓN CON LÓGICA CORREGIDA
        """
        try:
            symbol = original_trade.symbol
            
            # Log detallado
            self.log_recovery_trigger(original_trade, current_price, trigger_reason)
            
            # Determinar nuevo tipo (CONTRARIO al original)
            new_trade_type = "SHORT" if original_trade.trade_type == "LONG" else "SHORT"

            MAXIMO_APALANCAMIENTO= self._max_leverage_for_symbol(symbol)
            
            # 🆕 Calcular cantidad REDUCIDA (60% del original)
            quantity = original_trade.quantity * RECOVERY_POSITION_SIZE_MULTIPLIER
            
            logger.info(f"🔄 CONFIGURANDO TRADE DE RECUPERACIÓN:")
            logger.info(f"   Trade original: {original_trade.trade_type} @ ${original_trade.entry_price:.4f}")
            logger.info(f"   Precio actual: ${current_price:.4f}")
            logger.info(f"   Nuevo trade: {new_trade_type} @ ${current_price:.4f}")
            logger.info(f"   Cantidad: {quantity:.6f} ({RECOVERY_POSITION_SIZE_MULTIPLIER*100}% del original)")
            
            # 🆕 TP/SL CONSERVADORES Y SIMPLES PARA RECUPERACIÓN
            # Objetivo: pequeñas ganancias rápidas para compensar la pérdida
            if new_trade_type == "LONG":
                # LONG: esperamos que suba
                initial_tp = current_price * 1.025  # +2.5% TP
                initial_sl = current_price * 0.985  # -1.5% SL
            else:  # SHORT
                # SHORT: esperamos que baje
                initial_tp = current_price * 0.975  # -2.5% TP
                initial_sl = current_price * 1.015  # +1.5% SL
            
            # Crear TradeInfo de recuperación
            recovery_trade = TradeInfo(
                symbol=symbol,
                trade_type=new_trade_type,
                entry_price=current_price,
                entry_time=datetime.now(),
                current_tp=initial_tp,
                current_sl=initial_sl,
                quantity=quantity,
                state=TradeState.PENDING_OPEN,
                highest_price=current_price if new_trade_type == "LONG" else 0,
                lowest_price=current_price if new_trade_type == "SHORT" else float('inf'),
                is_recovery_mode=True,
                original_entry_price=original_trade.entry_price,
                original_exit_price=current_price,  # 🆕 Guardamos precio de cierre del original
                recovery_triggered_at=datetime.now(),
                recovery_attempts=original_trade.recovery_attempts,
                recovery_reason=trigger_reason
            )
            
            # Agregar al state manager
            if not self.state_manager.add_trade(symbol, recovery_trade):
                logger.error(f"❌ No se pudo agregar trade de recuperación para {symbol}")
                return
            
            # Registrar TP/SL inicial
            self.state_manager.update_tp_sl(
                symbol, 
                initial_tp, 
                initial_sl,
                f"Recovery mode: {trigger_reason}"
            )
            
            # Enviar comando de apertura
            side = "BUY" if new_trade_type == "LONG" else "SELL"
            cmd = OrderCommandData(
                command="OPEN_POSITION",
                symbol=symbol,
                data={
                    'side': side,
                    'quantity': quantity,
                    'tp':initial_tp,
                    'sl':initial_sl,
                    'leverage': MAXIMO_APALANCAMIENTO  # <-- NUEVO
                }
            )
            self.order_executor.submit_command(cmd)
            
            logger.info(f"🔄 TRADE DE RECUPERACIÓN ACTIVADO: {new_trade_type} {symbol}")
            logger.info(f"   TP: ${initial_tp:.4f} (+{((initial_tp/current_price)-1)*100:.2f}%)")
            logger.info(f"   SL: ${initial_sl:.4f} ({((initial_sl/current_price)-1)*100:.2f}%)")
            logger.info(f"   Objetivo ROI combinado: {ROI_RECOVERY_TARGET_FINAL}%")
            logger.info("-" * 50)
            
        except Exception as e:
            logger.error(f"Error abriendo trade de recuperación: {e}")
            import traceback
            logger.error(traceback.format_exc())



    # ==================== 🔥 FUNCIONES DE PYRAMIDING (ESCALADO DE POSICIONES) ====================
    
    def calculate_position_roi(self, trade: TradeInfo, current_price: float) -> float:
        """
        Calcula el ROI actual de una posición basado en el precio promedio ponderado
        
        Args:
            trade: Información del trade
            current_price: Precio actual del mercado
            
        Returns:
            ROI en porcentaje
        """
        if trade.weighted_avg_price == 0:
            return 0.0
            
        if trade.trade_type == "LONG":
            roi = ((current_price - trade.weighted_avg_price) / trade.weighted_avg_price) * 100
        else:  # SHORT
            roi = ((trade.weighted_avg_price - current_price) / trade.weighted_avg_price) * 100
            
        return roi
    
    def update_weighted_avg_price(self, trade: TradeInfo, new_price: float, new_quantity: float):
        """
        Actualiza el precio promedio ponderado cuando se añade una nueva posición
        
        Fórmula: nuevo_precio_avg = (precio_avg_actual * cantidad_actual + nuevo_precio * nueva_cantidad) / cantidad_total
        
        Args:
            trade: Información del trade
            new_price: Precio de entrada de la nueva posición
            new_quantity: Cantidad de la nueva posición
        """
        current_total_value = trade.weighted_avg_price * trade.total_quantity
        new_value = new_price * new_quantity
        
        trade.total_quantity += new_quantity
        trade.weighted_avg_price = (current_total_value + new_value) / trade.total_quantity
        
        logger.info(f"📊 {trade.symbol} - Precio promedio actualizado: ${trade.weighted_avg_price:.4f} | Cantidad total: {trade.total_quantity:.4f}")
    
    def check_and_execute_pyramiding(self, symbol: str):
        """
        🔥 FUNCIÓN PRINCIPAL DE PYRAMIDING
        
        Verifica si una posición ha alcanzado algún nivel de ROI para escalado
        y ejecuta la apertura de posición adicional si corresponde.
        
        Niveles: 5%, 10%, 15%, 20%, 25%
        
        Args:
            symbol: Símbolo del par
        """
        # Verificar si pyramiding está habilitado globalmente
        if not PYRAMIDING_ENABLED:
            return
            
        # Obtener trade actual
        trade = self.state_manager.get_trade(symbol)
        if not trade:
            return
            
        # Verificar si pyramiding está habilitado para este trade
        if not trade.pyramiding_enabled:
            return
            
        # Obtener precio actual
        current_price = self.data_cache.get_current_price(symbol)
        if not current_price:
            return
            
        # Calcular ROI actual
        current_roi = self.calculate_position_roi(trade, current_price)
        
        # Verificar cada nivel de ROI
        for roi_level in PYRAMIDING_ROI_LEVELS:
            # Verificar si este nivel ya fue completado
            if roi_level in trade.completed_pyramiding_levels:
                continue
                
            # Verificar si alcanzamos este nivel de ROI
            if current_roi >= roi_level:
                logger.info(f"🔥 PYRAMIDING ACTIVADO para {symbol} en ROI {roi_level}% (ROI actual: {current_roi:.2f}%)")
                
                # Ejecutar pyramiding
                success = self.execute_pyramiding_level(trade, current_price, roi_level)
                
                if success:
                    # Marcar este nivel como completado
                    trade.completed_pyramiding_levels.add(roi_level)
                    logger.info(f"✅ Pyramiding nivel {roi_level}% completado para {symbol}")
                else:
                    logger.warning(f"⚠️ Pyramiding nivel {roi_level}% falló para {symbol}")
    
    def execute_pyramiding_level(self, trade: TradeInfo, current_price: float, roi_level: float) -> bool:
        try:
            symbol = trade.symbol

            # calcular cantidad nueva (mismo tamaño original * multiplicador)
            new_quantity = trade.original_quantity * PYRAMIDING_MULTIPLIER
            if new_quantity <= 0:
                logger.warning(f"PYR: cantidad calculada inválida {new_quantity} para {symbol}")
                return False

            # calcular TP/SL basado en la distancia porcentual actual respecto al weighted_avg_price
            if trade.weighted_avg_price <= 0:
                base = trade.entry_price
            else:
                base = trade.weighted_avg_price

            if trade.trade_type == "LONG":
                tp_dist = ((trade.current_tp - base) / base) if base else 0.02
                sl_dist = ((base - trade.current_sl) / base) if base else 0.01
                new_tp = current_price * (1 + tp_dist)
                new_sl = current_price * (1 - sl_dist)
                side = "BUY"
            else:  # SHORT
                tp_dist = ((base - trade.current_tp) / base) if base else 0.02
                sl_dist = ((trade.current_sl - base) / base) if base else 0.01
                new_tp = current_price * (1 - tp_dist)
                new_sl = current_price * (1 + sl_dist)
                side = "SELL"

            logger.info(f"PYR: abriendo pyramiding {symbol} lvl {roi_level}% qty {new_quantity} price {current_price}")

            # Crear comando usando la estructura OrderCommandData que utiliza OrderExecutor
            cmd = OrderCommandData(
                command="OPEN_POSITION",
                symbol=symbol,
                data={
                    'side': side,
                    'quantity': new_quantity,
                    'tp': new_tp,
                    'sl': new_sl,
                    # opcional: marca para saber que es pyramiding (no usada por OrderExecutor, pero útil)
                    'is_pyramiding': True,
                    'pyramiding_level': roi_level,
                    'leverage': self._max_leverage_for_symbol(symbol) if hasattr(self, '_max_leverage_for_symbol') else None
                }
            )

            # Enviar la orden correctamente
            self.order_executor.submit_command(cmd)

            # Actualizar inmediatamente el precio ponderado / cantidades (modo simulado o para reflejar intent)
            try:
                trade.pyramid_symbol_enabled = True  # Marcar que este trade tiene pyramiding activo
                self.update_weighted_avg_price(trade, current_price, new_quantity)
                pyramid_level = PyramidLevel(
                    level=len(trade.pyramid_levels) + 1,
                    roi_threshold=roi_level,
                    entry_price=current_price,
                    quantity=new_quantity,
                    entry_time=datetime.now()
                )
                trade.pyramid_levels.append(pyramid_level)
            except Exception as e:
                logger.debug(f"PYR: fallo al actualizar weighted avg para {symbol}: {e}")

            return True
        except Exception as e:
            logger.exception(f"PYR: excepción en execute_pyramiding_level para {trade.symbol}: {e}")
            return False



    def check_and_execute_inverse_pyramiding(self, symbol: str):
        """
        Pyramiding inverso:
        - Solo actúa si INVERSE_PYRAMIDING_ENABLED y self.inversion_posiciones_PROBABLE True
        - Si una posición está en pérdida (ROI negativo) y alcanza uno de los niveles negativos,
        abre una orden adicional para intentar recuperar (en la dirección opuesta si procede).
        - Respeta INVERSE_PYRAMIDING_MAX_ADDS; si el trade ya tiene >= INVERSE_PYRAMIDING_ABORT_LEVEL
        niveles, fuerza aceptar pérdida y cerrar.
        """
        try:
            if not INVERSE_PYRAMIDING_ENABLED:
                return
            # Solo cuando el bot indicó inversión por historial (tu flag)
            if not getattr(self, 'inversion_posiciones_PROBABLE', False):
                return

            trade = self.state_manager.get_trade(symbol)
            if not trade:
                return

            # Si el trade no permite pyramiding en modo normal Y no estamos en modo inverso activado -> salir.
            # Pero si INVERSE_PYRAMIDING está activado y self.inversion_posiciones_PROBABLE True, permitimos ejecutar.
            if not getattr(trade, 'pyramiding_enabled', True) and not (
                INVERSE_PYRAMIDING_ENABLED and getattr(self, 'inversion_posiciones_PROBABLE', False)
            ):
                return

            # Obtener precio y ROI actual
            current_price = self.data_cache.get_current_price(symbol)
            if not current_price:
                return

            current_roi = self.calculate_position_roi(trade, current_price)
            # Asegurar ROI negativo significativo
            if current_roi is None or current_roi > INVERSE_PYRAMIDING_MINIMUM_ROI_TO_TRIGGER:
                return

            # Contador total de niveles ya aplicados (incluye pyramid_levels actuales)
            total_levels = len(getattr(trade, 'pyramid_levels', []))

            # Si ya llegamos al abort level -> aceptar pérdida y cerrar
            if total_levels >= INVERSE_PYRAMIDING_ABORT_LEVEL:
                logger.warning(f"PYR_INV: niveles ({total_levels}) >= abort_level ({INVERSE_PYRAMIDING_ABORT_LEVEL}) para {symbol}. Aceptando pérdida.")
                self._force_accept_loss_and_close(symbol, reason=f"inverse_pyramid_abort_levels_{total_levels}")
                return

            # Cuántos adds hemos hecho ya (no más de INVERSE_PYRAMIDING_MAX_ADDS)
            adds_done = total_levels
            if adds_done >= INVERSE_PYRAMIDING_MAX_ADDS:
                logger.info(f"PYR_INV: ya se hicieron {adds_done} adds (max={INVERSE_PYRAMIDING_MAX_ADDS}) para {symbol}. No abrir más.")
                return

            # Buscar el siguiente nivel negativo no usado
            # Normalizamos niveles a positivo para uso en sets (ej: -1.0 -> 1.0)
            completed_neg_levels = getattr(trade, 'completed_inverse_pyramid_levels', set())
            for neg_level in INVERSE_PYRAMIDING_NEG_ROI_LEVELS:
                if abs(neg_level) in completed_neg_levels:
                    continue
                # Si ROI actual es menor o igual a este umbral (más negativo)
                if current_roi <= neg_level:
                    logger.info(f"🔥 PYRAMIDING INVERSO activado para {symbol} en ROI {current_roi:.2f}% (umbral {neg_level}%)")
                    
                    # Determinar side: para intentar recuperar abrimos en sentido contrario
                    # Si trade original era LONG y está en pérdida, abrimos SHORT para recuperar y viceversa.
                    if trade.trade_type == "LONG":
                        pyr_side = "SELL"
                        new_trade_type = "SHORT"
                    else:
                        pyr_side = "BUY"
                        new_trade_type = "LONG"

                    # calcular cantidad a añadir:
                    
                    # calcular cantidad a añadir (con aceleración a partir de cierto nivel)
                    # level_actual será el siguiente nivel que vamos a añadir (1-based)
                    next_level_index = len(getattr(trade, 'pyramid_levels', [])) + 1

                    # cantidad base (lo que ya tenías)
                    base_quantity = float(trade.original_quantity or getattr(trade, 'quantity', 0.0)) * float(INVERSE_PYRAMIDING_MULTIPLIER or 1.0)

                    # Si el siguiente nivel está en o después del nivel de aceleración, aplicamos factor (x3, x3^2, ...)
                    if next_level_index >= INVERSE_PYRAMIDING_ACCELERATION_START_LEVEL:
                        # cuántos niveles desde el inicio de aceleración (1 -> primer nivel acelerado)
                        levels_since = next_level_index - INVERSE_PYRAMIDING_ACCELERATION_START_LEVEL + 1
                        accel_mult = float(INVERSE_PYRAMIDING_ACCELERATION_MULTIPLIER) ** max(1, int(levels_since))
                        # limitar el multiplicador para evitar tamaños descontrolados
                        accel_mult = min(float(INVERSE_PYRAMIDING_ACCELERATION_MAX_MULT), accel_mult)
                        new_quantity = base_quantity * accel_mult
                    else:
                        new_quantity = base_quantity

                    # seguridad: no permitir qty <= 0
                    if new_quantity <= 0:
                        logger.warning(f"PYR_INV: cantidad inválida {new_quantity} para {symbol}")
                        return
                    
                    if new_quantity <= 0:
                        logger.warning(f"PYR_INV: cantidad inválida {new_quantity} para {symbol}")
                        return

                    # calcular TP/SL conservadores para recuperar (puedes ajustar porcentajes)
                    if new_trade_type == "LONG":
                        tp = current_price * 1.02   # +2% objetivo rápido
                        sl = current_price * 0.99   # -1% stop
                    else:
                        tp = current_price * 0.98   # -2% objetivo para SHORT
                        sl = current_price * 1.01   # +1% stop

                    # crear comando y enviarlo a executor (usa misma estructura que execute_pyramiding_level)
                    cmd = OrderCommandData(
                        command="OPEN_POSITION",
                        symbol=symbol,
                        data={
                            'side': pyr_side,
                            'quantity': new_quantity,
                            'tp': tp,
                            'sl': sl,
                            'is_inverse_pyramiding': True,
                            'inverse_pyramiding_level': neg_level,
                            'leverage': getattr(self, '_max_leverage_for_symbol', lambda s: None)(symbol)
                        },
                        trade_id=trade.trade_id
                    )
                    self.order_executor.submit_command(cmd)

                    # registrar meta en trade (para no re-ejecutar el mismo nivel)
                    try:
                        if not hasattr(trade, 'completed_inverse_pyramid_levels'):
                            trade.completed_inverse_pyramid_levels = set()
                        trade.completed_inverse_pyramid_levels.add(abs(neg_level))
                        # añadir pyramid_level para contabilizar total_levels y avg
                        trade.pyramid_symbol_enabled = True  # Marcar que este trade tiene pyramiding activo (aunque sea inverso)
                        pyramid_level = PyramidLevel(
                            level=len(trade.pyramid_levels) + 1,
                            roi_threshold=neg_level,
                            entry_price=current_price,
                            quantity=new_quantity,
                            entry_time=datetime.now()
                        )
                        trade.pyramid_levels.append(pyramid_level)
                        # actualizar weighted_avg_price / total_quantity (reusa tu helper)
                        self.update_weighted_avg_price(trade, current_price, new_quantity)
                        logger.info(f"PYR_INV: nivel agregado para {symbol} -> qty={new_quantity} tp={tp} sl={sl}")
                    except Exception as e:
                        logger.debug(f"PYR_INV: fallo al actualizar trade {symbol}: {e}")

                    # Salimos tras ejecutar un nivel (evita múltiples en la misma iteración)
                    return

        except Exception as e:
            logger.exception(f"PYR_INV: excepción en check_and_execute_inverse_pyramiding para {symbol}: {e}")



    def _force_accept_loss_and_close(self, symbol: str, reason: str = "inverse_pyramid_force_accept"):
        """
        Fuerza aceptar la perdida: envía CLOSE_POSITION y registra reason.
        """
        try:
            trade = self.state_manager.get_trade(symbol)
            if not trade:
                logger.warning(f"force_accept_loss: trade no existe para {symbol}")
                return

            # Crear comando de cierre (usa trade_id para evitar dobles)
            cmd = OrderCommandData(
                command="CLOSE_POSITION",
                symbol=symbol,
                data={'reason': reason},
                trade_id=trade.trade_id
            )
            self.order_executor.submit_command(cmd)
            logger.warning(f"force_accept_loss: enviado CLOSE para {symbol} por {reason}")
        except Exception as e:
            logger.exception(f"force_accept_loss: error cerrando {symbol}: {e}")



    # ==================== FIN FUNCIONES DE PYRAMIDING ====================

    def _is_touching_ema(self, price: float, ema200: float, tol: float = EMATOUCH_TOLERANCE) -> bool:
        if price is None or ema200 is None or ema200 == 0:
            return False
        return abs(price - ema200) <= abs(ema200) * tol

    def _can_do_martingale(self, trade: TradeInfo) -> bool:
        # límite por trade (usa recovery_attempts o un contador específico)
        adds_done = getattr(trade, 'martingale_adds', 0)
        return (MARTINGALE_ENABLED and adds_done < MARTINGALE_MAX_ADDS)

    def check_and_execute_martingale_on_ema(self, trade: TradeInfo, df_LOP: pd.DataFrame, current_price: float, current_roi: float):
        """
        Condición: si trade pierde >= 8% (ROI <= ROI_CRITICAL_LOSS) y:
        - LONG: bb_upper and bb_lower > price (ambas bandas por encima del precio)
        - SHORT: bb_upper and bb_lower < price (ambas bandas por debajo del precio)
        - y el precio 'toca' la ema_200 (dentro de EMATOUCH_TOLERANCE)
        Abre posición adicional en la misma dirección (martingala), actualiza weighted avg.
        """
        try:
            if trade is None:
                return False

            # Primero validaciones rápidas
            if not self._can_do_martingale(trade):
                return False

            # asegurar df_LOP no vacío y columnas presentes
            if df_LOP is None or df_LOP.empty:
                return False
            ema200 = float(df_LOP['ema_200'].iloc[-1])
            bb_upper = float(df_LOP['bb_upper'].iloc[-1])
            bb_lower = float(df_LOP['bb_lower'].iloc[-1])

            # Tolerancia touch
            touching = self._is_touching_ema(current_price, ema200)
            if not touching:
                return False

            # Condiciones por side
            if trade.trade_type == "LONG":
                # ambas bandas por encima del precio -> precio por debajo de bb_lower (condición solicitada)
                bands_ok = (bb_upper > current_price and bb_lower > current_price)
                roi_trigger = (current_roi is not None and current_roi <= ROI_CRITICAL_LOSS)
                if not (bands_ok and roi_trigger):
                    return False
                side = "BUY"

            else:  # SHORT
                # ambas bandas por debajo del precio -> precio por encima de bb_upper
                bands_ok = (bb_upper < current_price and bb_lower < current_price)
                roi_trigger = (current_roi is not None and current_roi <= ROI_CRITICAL_LOSS)
                if not (bands_ok and roi_trigger):
                    return False
                side = "SELL"

            # Calcular cantidad a añadir:
            original_qty = float(getattr(trade, 'original_quantity', trade.quantity or 0.0) or 0.0)
            # Protección: si original_qty 0 -> usar trade.quantity
            if original_qty <= 0:
                original_qty = float(getattr(trade, 'quantity', 0.0) or 0.0)
            if original_qty <= 0:
                logger.warning("Martingale: cantidad original inválida para %s", trade.symbol)
                return False

            adds_done = getattr(trade, 'martingale_adds', 0)
            proposed_qty = original_qty * (MARTINGALE_MULTIPLIER ** (adds_done + 1))

            # tope absoluto relativo a original
            max_allowed = original_qty * MARTINGALE_MAX_TOTAL_MULT
            if proposed_qty > max_allowed:
                proposed_qty = max_allowed

            # (Opcional) validar con min notional / step usando tu helper si lo tienes
            try:
                # si tienes método calculate_position_size, preferirlo
                if hasattr(self, 'calculate_position_size') and callable(self.calculate_position_size):
                    max_leverage = self._max_leverage_for_symbol(trade.symbol) or None
                    # intenta respetar la función original (si aceptas pasar entry_price y leverage)
                    # si falla, seguir con proposed_qty
                    try:
                        suggested = self.calculate_position_size(trade.symbol, current_price, max_leverage)
                        if suggested and suggested > 0:
                            # evitar reducir a cantidades ínfimas; solo usar si suggested es razonable
                            proposed_qty = max(proposed_qty, suggested)
                    except Exception:
                        pass
            except Exception:
                pass

            # Preparar comando OPEN_POSITION
            cmd = OrderCommandData(
                command="OPEN_POSITION",
                symbol=trade.symbol,
                data={
                    'side': side,
                    'quantity': proposed_qty,
                    'tp': trade.current_tp,
                    'sl': trade.current_sl,
                    'is_martingale': True,
                    'martingale_level': adds_done + 1,
                    'leverage': self._max_leverage_for_symbol(trade.symbol) if hasattr(self, '_max_leverage_for_symbol') else None
                }
            )

            try:
                self.order_executor.submit_command(cmd)
            except Exception as e:
                logger.warning("Martingale: fallo al enviar comando para %s: %s", trade.symbol, e)
                return False

            # Actualizar estado localmente (optimista)
            try:
                # marcar que este trade tiene pyramiding/martingale
                trade.pyramid_symbol_enabled = True
                trade.martingale_adds = adds_done + 1
                # actualizar weighted avg suponiendo que la orden entrará al precio actual
                self.update_weighted_avg_price(trade, current_price, proposed_qty)
                # añadir un registro de este add
                lvl = PyramidLevel(level=(len(trade.pyramid_levels) + 1),
                                roi_threshold=current_roi,
                                entry_price=current_price,
                                quantity=proposed_qty,
                                entry_time=datetime.now())
                trade.pyramid_levels.append(lvl)
                logger.info("🔁 Martingale enviado: %s | level=%d | qty=%.6f | price=%.6f", trade.symbol, lvl.level, proposed_qty, current_price)
            except Exception as e:
                logger.debug("Martingale: fallo al actualizar trade internamente: %s", e)

            return True

        except Exception as e:
            logger.exception("Error en check_and_execute_martingale_on_ema: %s", e)
            return False

    def check_exit_and_update(self, symbol: str):
        """
        🔧 VERSIÓN MEJORADA: Prevenir re-entrada durante procesamiento de cierre
        """
        if self.state_manager.is_closing(symbol):
            logger.debug(f"⚠️ {symbol} ya está en proceso de cierre, ignorando")
            return
            
        try:
            trade = self.state_manager.get_trade(symbol)
            if not trade:
                return
 
            if self.check_emergency_stop():
                return
            
            # Obtener datos y convertir a Heikin Ashi
            df_1m, _ = self.data_cache.get_data(symbol)
            if df_1m is None or df_1m.empty or len(df_1m) < EMA_PERIOD + 2:
                return
            
            df_LOP = self.add_indicators(df_1m)
            ha_df = calculate_heikin_ashi(df_1m)

            if ha_df.empty:
                return
            
            ha_df = add_heikin_ashi_indicators(ha_df, EMA_PERIOD)
            if ha_df.empty:
                return
            
            # Última vela cerrada
            last_bar = ha_df.iloc[-2]
            last_barLOP = df_LOP.iloc[-2]
            
            # 🆕 Vela anterior (para detectar cruce de momentum)
            prev_barLOP = df_LOP.iloc[-3] if len(df_LOP) >= 3 else last_barLOP            
            current_bar = ha_df.iloc[-1]
            
            # Actualizar contador de velas
            trade.bars_held += 1
            
            # Obtener precio actual
            current_price = self.data_cache.get_current_price(symbol)
            if current_price is None:
                current_price = float(current_bar['ha_close'])
            
            # Actualizar highest/lowest
            if trade.trade_type == "LONG":
                trade.highest_price = max(trade.highest_price, current_price)
            else:
                trade.lowest_price = min(trade.lowest_price, current_price)
            
            # Calcular ROI actual
            current_roi = self.calculate_current_roi(trade, current_price)

            # intentar martingale si cumple condiciones de toque EMA200 y -8% (opción solicitada)
            try:
                if self.check_and_execute_martingale_on_ema(trade, df_LOP, current_price, current_roi):
                    # si abrimos martingale, devolvemos para evitar cerrar en el mismo ciclo
                    return
            except Exception:
                pass

            # 🔥 PYRAMIDING
            self.check_and_execute_pyramiding(symbol)
            
            try:
                    self.check_and_execute_inverse_pyramiding(symbol)
            except Exception as e:
                    logger.debug(f"Error ejecutando inverse pyramiding para {symbol}: {e}")




            # ----------------- CIERRE POR ROI (PYRAMIDING) -----------------
            try:
                # sólo si pyramiding está activado globalmente y en este trade
                if PYRAMIDING_CLOSE_ON_ROI_ENABLED and trade.pyramid_symbol_enabled:
                    # umbral: primero intentar override por trade, si existe
                    threshold = getattr(trade, 'pyramiding_close_roi', None)
                    if threshold is None:
                        threshold = PYRAMIDING_CLOSE_ROI
                        
                    if not self.inversion_posiciones_PROBABLE:

                        # evaluar cierre
                        if current_roi is not None and current_roi <= float(threshold):
                            exit_signal = {
                                'symbol': symbol,
                                'exit_price': current_price,
                                'exit_reason': f'PYRAMID_CLOSE_ROI (ROI={current_roi:.2f}% >= {threshold}%)',
                                'timestamp': datetime.now()
                            }
                            try:
                                self.exit_queue.put_nowait(exit_signal)
                                logger.info(f"📤 PYR CIERRE POR ROI enviado: {symbol} | ROI={current_roi:.2f}% >= {threshold}%")
                            except Full:
                                logger.warning(f"Cola de salidas llena al intentar cerrar por pyramiding ROI: {symbol}")
                            # retornamos para no seguir con más lógica en este ciclo (evita dobles señales)
                            return
                        
                    if self.inversion_posiciones_PROBABLE:
                        if current_roi is not None and current_roi >= float(threshold):
                            exit_signal = {
                                'symbol': symbol,
                                'exit_price': current_price,
                                'exit_reason': f'PYRAMID_CLOSE_ROI (ROI={current_roi:.2f}% <= {threshold}%)',
                                'timestamp': datetime.now()
                            }
                            try:
                                self.exit_queue.put_nowait(exit_signal)
                                logger.info(f"📤 PYR CIERRE POR ROI enviado: {symbol} | ROI={current_roi:.2f}% <= {threshold}%")
                            except Full:
                                logger.warning(f"Cola de salidas llena al intentar cerrar por pyramiding ROI: {symbol}")
                            return
                    
                    
                    
            except Exception as e:
                logger.debug(f"Error en cierre por ROI (pyramiding) para {symbol}: {e}")
            # ---------------------------------------------------------------



                    # ========== 🆕 SISTEMA DE RECUPERACIÓN MEJORADO ==========

            if RECOVERY_MODE_ENABLED and not trade.is_recovery_mode:
                # REGLA: Si ROI < -6.6% SIN señal de cierre = cambio de tendencia
                if current_roi < ROI_CRITICAL_LOSS:
                    
                    # Verificar si hay señal de cierre normal
                    has_exit_signal = False

                    
                    
                    if trade.trade_type == "LONG":
                        # LONG: señal de salida si vela cierra arriba de EMA_high
                        if self.check_long_exit(last_barLOP):
                            has_exit_signal = True
                    else:  # SHORT
                        # SHORT: señal de salida si vela cierra abajo de EMA_low
                        if self.check_short_exit(last_barLOP):
                            has_exit_signal = True
                    
                    # 🔥 SITUACIÓN CRÍTICA: ROI < -6.6% SIN señal de cierre

                    if not has_exit_signal:
                        # 🆕 Verificar límite de intentos
                        if trade.recovery_attempts >= trade.max_recovery_attempts:
                            logger.error(f"❌ Límite de recuperación alcanzado para {symbol}")
                            
                            # Cerrar normalmente
                            exit_signal = {
                                'symbol': symbol,
                                'exit_price': current_price,
                                'exit_reason': f'MAX_RECOVERY_ATTEMPTS (ROI: {current_roi:.2f}%)',
                                'timestamp': datetime.now()
                            }
                            try:
                                self.exit_queue.put_nowait(exit_signal)
                            except Full:
                                pass
                            return
                        
                        logger.warning(f"⚠️ Activando recuperación para {symbol} (ROI: {current_roi:.2f}%)")
                        
                        # Incrementar contador
                        trade.recovery_attempts += 1
                        
                        # Log detallado
                        self.log_recovery_trigger(trade, current_price, 
                            f"Critical ROI {current_roi:.2f}% without exit signal")
                        
                        # ✅ ENVIAR A LA COLA NORMAL (como cualquier otro cierre)
                        exit_signal = {
                            'symbol': symbol,
                            'exit_price': current_price,
                            'exit_reason': f'RECOVERY_TRIGGER (ROI: {current_roi:.2f}%)',
                            'timestamp': datetime.now()
                        }
                        
                        try:
                            self.exit_queue.put_nowait(exit_signal)
                            logger.info(f"📤 Señal de cierre enviada para activar recuperación")
                        except Full:
                            logger.error(f"❌ Cola de salidas llena")
                            return
                        
                        # ⏳ Esperar procesamiento
                        time.sleep(2)
                        
                        # 🔄 Abrir recuperación
                        self.open_recovery_trade(trade, current_price, 
                            f"Critical ROI {current_roi:.2f}% without exit signal")
                        
                        return
            
            # ========== VERIFICAR CONDICIONES DE SALIDA NORMALES ==========
            
            exit_signal = None
            
            # 🆕 MODO RECUPERACIÓN: Condiciones de cierre optimizadas
            if trade.is_recovery_mode:
                combined_roi = self.calculate_combined_recovery_roi(trade, current_price)
                
                logger.debug(f"🔄 Monitoreando recuperación {symbol}:")
                logger.debug(f"   ROI actual trade: {current_roi:.2f}%")
                logger.debug(f"   ROI combinado: {combined_roi:.2f}%")
                logger.debug(f"   Objetivo: {ROI_RECOVERY_TARGET_FINAL}%")
                
                # Verificar si alcanzó objetivo de recuperación
                if combined_roi >= ROI_RECOVERY_TARGET_FINAL:
                    exit_signal = {
                        'symbol': symbol,
                        'exit_price': current_price,
                        'exit_reason': f'RECOVERY_SUCCESS (Combined ROI: {combined_roi:.2f}%)',
                        'timestamp': datetime.now()
                    }
                    logger.info(f"✅ ¡RECUPERACIÓN EXITOSA! {symbol} - ROI combinado: {combined_roi:.2f}%")
                
                # 🆕 Stop-loss si la recuperación también falla
                elif current_roi < -3.0:
                    logger.warning(f"⚠️ Recuperación fallando para {symbol}: {current_roi:.2f}%")
                    
                    # Si ya intentamos recuperar y sigue mal, cerrar
                    if trade.recovery_attempts >= trade.max_recovery_attempts - 1:
                        exit_signal = {
                            'symbol': symbol,
                            'exit_price': current_price,
                            'exit_reason': f'RECOVERY_FAILED (ROI: {current_roi:.2f}%, Combined: {combined_roi:.2f}%)',
                            'timestamp': datetime.now()
                        }
                        logger.error(f"❌ Recuperación fallida definitiva: {symbol}")
            
            else:

                # ==============================================================================
                # 🎯 4. VERIFICACIÓN DE GLOBAL PROFIT TARGET (Prioridad Alta)
                # ==============================================================================
                # Si el manager dice que llegamos a la meta, cerramos ESTA posición individualmente.
                # El manager maneja el cálculo global, aquí solo ejecutamos la salida.
                if self.profit_target_manager.is_target_reached() or self.Bandera_de_Cierre_por_target:
                    
                    # Definir razón específica
                    target_val = self.profit_target_manager.get_current_target()
                    exit_reason = f"PROFIT_TARGET_REACHED (Target: ${target_val:.2f})"
                    
                    logger.warning(f"🎯 {symbol} cerrando por Objetivo Global alcanzado.")

                    exit_signal = {
                        'symbol': symbol,
                        'exit_price': current_price,
                        'exit_reason': exit_reason,
                        'timestamp': datetime.now()
                    }

                    try:
                        self.exit_queue.put_nowait(exit_signal)
                        logger.info(f"📤 Señal de salida enviada por TARGET: {symbol}")
                    except Full:
                        logger.warning(f"Cola de salidas llena al intentar cerrar por Target: {symbol}")
                    
                    return

                # ==============================================================================
                # 🆕 5. CIERRE POR MOMENTUM ADVERSO CON ROI NEGATIVO
                # ==============================================================================
                
                if trade.trade_type == "LONG" and self.inversion_posiciones_PROBABLE:
                    signal_type = "SHORT"
                    current_roi = (-current_roi)
                elif trade.trade_type == "SHORT" and self.inversion_posiciones_PROBABLE:
                    signal_type = "LONG"
                    current_roi = (-current_roi)
                
                if MOMENTUM_EXIT_ENABLED and current_roi is not None and current_roi < MOMENTUM_EXIT_ROI_MAX:

                    momentum_exit_signal = None

                    if signal_type == "LONG":
                        if check_bearish_momentum(self, last_barLOP, prev_barLOP):
                            momentum_exit_signal = {
                                'symbol': symbol,
                                'exit_price': current_price,
                                'exit_reason': f'MOMENTUM_BEARISH_EXIT | LONG en pérdida (ROI: {current_roi:.2f}%)',
                                'timestamp': datetime.now()
                            }
                            logger.warning(f"📉 Momentum BEARISH en LONG perdedor: {symbol} | ROI: {current_roi:.2f}%")

                    else:  # SHORT
                        if check_bullish_momentum(self,last_barLOP, prev_barLOP):
                            momentum_exit_signal = {
                                'symbol': symbol,
                                'exit_price': current_price,
                                'exit_reason': f'MOMENTUM_BULLISH_EXIT | SHORT en pérdida (ROI: {current_roi:.2f}%)',
                                'timestamp': datetime.now()
                            }
                            logger.warning(f"📈 Momentum BULLISH en SHORT perdedor: {symbol} | ROI: {current_roi:.2f}%")

                    if momentum_exit_signal:
                        try:
                            self.exit_queue.put_nowait(momentum_exit_signal)
                            logger.info(f"📤 Cierre por Momentum: {symbol} → {momentum_exit_signal['exit_reason']}")
                        except Full:
                            logger.warning(f"Cola de salidas llena (momentum exit): {symbol}")
                        return

                # ==============================================================================
                # 6. MODO NORMAL: Condiciones de cierre estándar (tu código existente)
                # ==============================================================================

                if not self.inversion_posiciones_PROBABLE:
                # MODO NORMAL: Condiciones de cierre estándar
                    if signal_type == "LONG":
                        if self.check_long_exit(last_barLOP) or current_roi < ROI_CRITICAL_LOSS or current_roi > (ROI_CRITICAL_PROFIT):
                            
                            # 🆕 VERIFICAR ROI antes de cerrar
                            if self.check_long_exit(last_barLOP)  and not self.inversion_posiciones:
                                logger.info(f"⏸️ Señal de cierre detectada pero ROI muy bajo: {current_roi:.2f}%")
                                logger.info(f"   Esperando recuperación...")
                                exit_signal = {
                                    'symbol': symbol,
                                    'exit_price': current_price,
                                    'exit_reason': f'HA_CLOSE_ABOVE_EMA_HIGH (ROI: {current_roi:.2f}%)',
                                    'timestamp': datetime.now()
                                }
                                # NO cerrar, continuar esperando
                            elif current_roi < ROI_CRITICAL_LOSS or current_roi > ROI_CRITICAL_PROFIT:
                                exit_signal = {
                                    'symbol': symbol,
                                    'exit_price': current_price,
                                    'exit_reason': f'HA_CLOSE_ABOVE_EMA_HIGH (ROI: {current_roi:.2f}%)',
                                    'timestamp': datetime.now()
                                }
                    
                    else:  # SHORT
                        if self.check_short_exit(last_barLOP) or current_roi < ROI_CRITICAL_LOSS or current_roi > (ROI_CRITICAL_PROFIT):
                            
                            if self.check_short_exit(last_barLOP)  and not self.inversion_posiciones:
                                
                                logger.info(f"⏸️ Señal de cierre detectada pero ROI muy bajo: {current_roi:.2f}%")
                                logger.info(f"   Esperando recuperación...")
                                exit_signal = {
                                    'symbol': symbol,
                                    'exit_price': current_price,
                                    'exit_reason': f'HA_CLOSE_ABOVE_EMA_HIGH (ROI: {current_roi:.2f}%)',
                                    'timestamp': datetime.now()
                                }

                            elif current_roi < ROI_CRITICAL_LOSS or current_roi > ROI_CRITICAL_PROFIT:
                                exit_signal = {
                                    'symbol': symbol,
                                    'exit_price': current_price,
                                    'exit_reason': f'HA_CLOSE_BELOW_EMA_LOW (ROI: {current_roi:.2f}%)',
                                    'timestamp': datetime.now()
                                }
                
                else:
                    if signal_type == "SHORT":
                        if self.check_long_exit(last_barLOP) or current_roi < ROI_CRITICAL_LOSS or current_roi > (ROI_CRITICAL_PROFIT):
                            
                            # 🆕 VERIFICAR ROI antes de cerrar
                            if self.check_short_exit(last_barLOP) and self.inversion_posiciones:
                                logger.info(f"⏸️ Señal de cierre detectada pero ROI muy bajo: {current_roi:.2f}%")
                                logger.info(f"   Esperando recuperación...")
                                exit_signal = {
                                    'symbol': symbol,
                                    'exit_price': current_price,
                                    'exit_reason': f'HA_CLOSE_ABOVE_EMA_HIGH (ROI: {current_roi:.2f}%)',
                                    'timestamp': datetime.now()
                                }
                                
                                # NO cerrar, continuar esperando
                            elif current_roi < ROI_CRITICAL_LOSS or current_roi > ROI_CRITICAL_PROFIT:
                                exit_signal = {
                                    'symbol': symbol,
                                    'exit_price': current_price,
                                    'exit_reason': f'HA_CLOSE_ABOVE_EMA_HIGH (ROI: {current_roi:.2f}%)',
                                    'timestamp': datetime.now()
                                }
                    
                    else:  # LONG
                        if self.check_short_exit(last_barLOP) or current_roi < ROI_CRITICAL_LOSS or current_roi > (ROI_CRITICAL_PROFIT):
                            
                            if self.check_long_exit(last_barLOP) and self.inversion_posiciones:
                                logger.info(f"⏸️ Señal de cierre detectada pero ROI muy bajo: {current_roi:.2f}%")
                                logger.info(f"   Esperando recuperación...")
                                exit_signal = {
                                    'symbol': symbol,
                                    'exit_price': current_price,
                                    'exit_reason': f'HA_CLOSE_ABOVE_EMA_HIGH (ROI: {current_roi:.2f}%)',
                                    'timestamp': datetime.now()
                                }

                            elif current_roi < ROI_CRITICAL_LOSS or current_roi > ROI_CRITICAL_PROFIT:
                                exit_signal = {
                                    'symbol': symbol,
                                    'exit_price': current_price,
                                    'exit_reason': f'HA_CLOSE_BELOW_EMA_LOW (ROI: {current_roi:.2f}%)',
                                    'timestamp': datetime.now()
                                }

            # Si hay señal de salida, enviar a cola
            if exit_signal:
                try:
                    self.exit_queue.put_nowait(exit_signal)
                    logger.info(f"📤 Señal de salida detectada: {symbol} - {exit_signal['exit_reason']}")
                except Full:
                    logger.warning(f"Cola de salidas llena: {symbol}")
                return
            
            # ========== ACTUALIZAR TP/SL DINÁMICAMENTE ==========
            
            # Solo actualizar TP/SL si NO está en modo recuperación crítica
            if not trade.is_recovery_mode or current_roi > -2.0:
                
            # ===================== ACTUALIZACION ROI-BASED SL (mantener distancia fija 4% si ROI >= 5%) =====================

                # default: mante­nemos la lógica anterior en new_tp/new_sl
                new_tp = None
                new_sl = None
                update_reason = None

                # lógica previa (ejemplo existente)
                if trade.trade_type == "LONG":
                    # default SL/TP calculados por EMA/price
                    new_sl = float(current_bar['ema_high'])
                    if current_price < trade.entry_price * 0.99:
                        new_tp = float(current_price * 0.985)
                    else:
                        new_tp = trade.current_tp
                    update_reason = f"Bar {trade.bars_held}: SL=EMA_high, TP trailing down"
                else:  # SHORT
                    new_sl = float(current_bar['ema_low'])
                    if current_price > trade.entry_price * 1.01:
                        new_tp = float(current_price * 1.015)
                    else:
                        new_tp = trade.current_tp
                    update_reason = f"Bar {trade.bars_held}: SL=EMA_low, TP trailing up"

                # --- NUEVA LÓGICA: ajustar SL para proteger ganancias cuando ROI >= threshold ---
                try:
                    if current_roi >= PROTECT_ROI_THRESHOLD:
                        desired_sl_roi = current_roi - PROTECT_DISTANCE  # p.ej. 10% - 4% = 6%
                        # sólo actuamos si avanzó la protección respecto a lo guardado
                        if desired_sl_roi > getattr(trade, 'last_protected_roi', 0.0):
                            if trade.trade_type == "LONG":
                                # precio que corresponde a desired_sl_roi para LONG
                                desired_sl_price = trade.entry_price * (1.0 + desired_sl_roi / 100.0)
                                # Validación: SL no puede quedar por encima del precio actual
                                if desired_sl_price < current_price:
                                    # aplicamos sólo si aprieta (sube) respecto al SL actual
                                    if desired_sl_price > (trade.current_sl or 0.0):
                                        new_sl = desired_sl_price
                                        trade.last_protected_roi = desired_sl_roi
                                        update_reason = f"ROI-protect: lock {desired_sl_roi:.2f}% (keep {PROTECT_DISTANCE}% dist)"
                            else:  # SHORT
                                # para SHORT, precio correspondiente a desired_sl_roi
                                desired_sl_price = trade.entry_price * (1.0 - desired_sl_roi / 100.0)
                                # Validación: SL debe quedar por encima del precio actual (para proteger ganancias)
                                if desired_sl_price > current_price:
                                    # aplicamos sólo si aprieta (baja) respecto al SL actual
                                    # (numéricamente menor = más cerca del precio actual)
                                    if (trade.current_sl is None) or (desired_sl_price < trade.current_sl):
                                        new_sl = desired_sl_price
                                        trade.last_protected_roi = desired_sl_roi
                                        update_reason = f"ROI-protect: lock {desired_sl_roi:.2f}% (keep {PROTECT_DISTANCE}% dist)"
                except Exception as e:
                    logger.debug(f"Error aplicando ROI-protect SL para {symbol}: {e}")
                # ================================================================================================================

                
                tp_changed = abs(new_tp - trade.current_tp) > trade.entry_price * 0.001
                sl_changed = abs(new_sl - trade.current_sl) > trade.entry_price * 0.001
                
                if tp_changed or sl_changed:
                    a=1
                    # self.state_manager.update_tp_sl(symbol, new_tp, new_sl, update_reason)
                    
                    # if not self.simulate:
                    #     cmd = OrderCommandData(
                    #         command="UPDATE_TP_SL",
                    #         symbol=symbol,
                    #         data={
                    #             'tp': new_tp,
                    #             'sl': new_sl,
                    #             'direction': trade.trade_type
                    #         }
                    #     )
                    #     self.order_executor.submit_command(cmd)
                    
                    # logger.debug(f"📊 TP/SL actualizado: {symbol} | ROI: {current_roi:.2f}%")
            
        except Exception as e:
            logger.debug(f"Error en check_exit_and_update para {symbol}: {e}")




    def process_exit_signal(self, exit_signal: Dict):
        """🆕 VERSIÓN SIMPLIFICADA - sin verificación bloqueante"""
        symbol = exit_signal['symbol']
        
        try:
            exit_price = exit_signal['exit_price']
            exit_reason = exit_signal['exit_reason']
            
            # Obtener trade
            trade = self.state_manager.get_trade(symbol)
            if not trade:
                logger.warning(f"⚠️ Trade no encontrado para cerrar: {symbol}")
                return
            
            # 🆕 VERIFICAR si ya está marcado como closing
            if self.state_manager.is_closing(symbol):
                logger.debug(f"⏭️ {symbol} ya está marcándose para cierre, ignorando")
                return
            
            logger.info(f"🔄 Iniciando proceso de cierre: {symbol} - {exit_reason} @ ${exit_price:.4f}")
            
            # Enviar comando de cierre (será procesado por OrderExecutor)
            cmd = OrderCommandData(
                command="CLOSE_POSITION",
                symbol=symbol,
                data={'reason': exit_reason}, trade_id=trade.trade_id
            )
            self.order_executor.submit_command(cmd)
            
        except Exception as e:
            logger.error(f"❌ Error procesando señal de salida para {symbol}: {e}")
            import traceback
            logger.error(traceback.format_exc())

    # 2. MÉTODO PARA PAUSAR/REANUDAR TRADING
    def pause_trading(self, seconds: int = 30):
        """Pausa temporalmente la apertura de nuevas operaciones"""
        with self.pause_lock:
            self.trading_paused = True
            self.pause_until = datetime.now() + timedelta(seconds=seconds)
            logger.warning(f"⏸️ Trading pausado por {seconds} segundos hasta {self.pause_until.strftime('%H:%M:%S')}")
    
    def resume_trading_if_ready(self):
        """Reanuda el trading si ya pasó el tiempo de pausa"""
        with self.pause_lock:
            if not self.trading_paused:
                return True
            
            if datetime.now() >= self.pause_until:
                self.trading_paused = False
                logger.info("▶️ Trading reanudado automáticamente")
                return True
            
            return False
    
    def is_trading_allowed(self) -> bool:
        """Verifica si se pueden abrir nuevas operaciones"""
        with self.pause_lock:
            if not self.trading_paused:
                return True
            
            if datetime.now() >= self.pause_until:
                self.trading_paused = False
                logger.info("▶️ Trading reanudado automáticamente")
                return True
            
            remaining = (self.pause_until - datetime.now()).total_seconds()
            logger.debug(f"⏸️ Trading pausado. Reanudar en {remaining:.0f}s")
            return False
    
    def compute_unrealized_pnl_summary(self) -> Dict:
        """
        Recorre trades activos y devuelve:
        - detalle por trade (symbol, side, entry, current, qty, pnl_gross, pnl_net_est, roi%)
        - total_unrealized_gross
        - total_unrealized_net_est
        - combined_balance_gross = self.balance + total_unrealized_gross
        - combined_balance_net_est = self.balance + total_unrealized_net_est

        NOTAS:
        - Usa self.data_cache.get_current_price(symbol) como precio preferente.
        - PnL neto es estimado restando 'exit fee' (entry fee ya pudo haberse pagado al abrir).
        - FEE_RATE debe existir en tu módulo (usa la constante que ya usas al registrar cierres).
        """
        details = []
        total_unrealized_gross = 0.0
        total_unrealized_net = 0.0

        try:
            active_symbols = list(self.state_manager.get_all_active_symbols())
            for symbol in active_symbols:
                trade = self.state_manager.get_trade(symbol)
                if not trade:
                    continue

                # obtener precio actual (ws/cache) o fallback entry_price
                try:
                    current_price = self.data_cache.get_current_price(symbol)
                except Exception:
                    current_price = None
                if current_price is None or current_price <= 0:
                    current_price = getattr(trade, 'entry_price', 0.0)

                # PYRAMIDING FIX
                raw_qty   = getattr(trade, 'total_quantity', None)
                raw_entry = getattr(trade, 'weighted_avg_price', None)

                qty   = float(raw_qty)   if raw_qty   and float(raw_qty)   > 0 else float(trade.quantity)
                entry = float(raw_entry) if raw_entry and float(raw_entry) > 0 else float(trade.entry_price)
                side = getattr(trade, 'trade_type', 'LONG')

                # PnL bruto
                if side == "LONG":
                    pnl_gross = (current_price - entry) * qty
                else:
                    pnl_gross = (entry - current_price) * qty

                # Fees estimates (puedes ajustar si tu lógica de fees es distinta)
                try:
                    entry_fee_est = entry * qty * FEE_RATE
                except Exception:
                    entry_fee_est = 0.0
                try:
                    exit_fee_est = current_price * qty * FEE_RATE
                except Exception:
                    exit_fee_est = 0.0

                # PnL neto estimado: restamos exit_fee (si ya pagaste entry_fee al abrir, no restamos de nuevo;
                # si no, deberías restar entry_fee_est también. Aquí muestro exit_fee como estimado).
                pnl_net_est = pnl_gross - exit_fee_est

                # ROI respecto al capital invertido (entry*qty)
                invested = entry * qty if entry and qty else 1.0
                roi_pct = (pnl_gross / invested) * 100 if invested else 0.0

                details.append({
                    'symbol': symbol,
                    'side': side,
                    'entry': entry,
                    'current': current_price,
                    'qty': qty,
                    'pnl_gross': pnl_gross,
                    'pnl_net_est': pnl_net_est,
                    'entry_fee_est': entry_fee_est,
                    'exit_fee_est': exit_fee_est,
                    'roi_pct': roi_pct
                })

                total_unrealized_gross += pnl_gross
                total_unrealized_net += pnl_net_est

        except Exception as e:
            logger.exception("Error calculando PnL no realizado: %s", e)

        combined_balance_gross = getattr(self, 'balance', 0.0) + total_unrealized_gross
        combined_balance_net = getattr(self, 'balance', 0.0) + total_unrealized_net

        return {
            'details': details,
            'total_unrealized_gross': total_unrealized_gross,
            'total_unrealized_net_est': total_unrealized_net,
            'combined_balance_gross': combined_balance_gross,
            'combined_balance_net_est': combined_balance_net
        }



    # ==================== THREADS ====================
    def _init_kline_cache(self, symbols):
        """
        Crea (o re-crea) el KlineWebSocketCache con el set de símbolos actual.
        Se llama desde run() y desde _sync_kline_cache() cuando cambia el set.
        """
        # Detener instancia anterior si existía
        if self.kline_ws_cache is not None:
            try:
                self.kline_ws_cache.stop()
            except Exception as e:
                logger.warning(f"⚠️  Error deteniendo kline_ws_cache anterior: {e}")

        # Construir dict {symbol: ['1m', '5m']}
        pairs = {s: ['1m', '5m'] for s in symbols}

        # self.kline_ws_cache = KlineWebSocketCache(
        #     pairs=pairs,
        #     max_candles=1500,
        #     include_open_candle=True,   # mantener la vela en formación
        #     backfill_on_start=True,     # cargar histórico por REST al arrancar
        #     streams_per_connection=40,
        #     periodic_refresh_minutes=5,
        #     integrity_check_enabled=True,
        #     stream_health_check_seconds=120,
        # )
        
        self.kline_ws_cache = KlineWebSocketCache(
            pairs=pairs,
            max_candles=1500,
            include_open_candle=True,
            backfill_on_start=True,
            streams_per_connection=40,
        )
        
        self.kline_ws_cache.start()
        logger.info(f"✅ KlineWebSocketCache iniciado para {len(symbols)} símbolos "
                    f"({len(symbols)*2} streams: 1m + 5m)")

    def _sync_kline_cache(self):
        """
        Llámalo al final de update_monitored_symbols() para que el WebSocket
        de klines se actualice cuando el set de símbolos cambia.
        """
        if self.kline_ws_cache is None:
            return  # Aún no se inicializó (antes del run())

        # Símbolos que tiene el cache actualmente
        cached_symbols = set(sym for sym, _ in self.kline_ws_cache.subscribed_streams)
        current_symbols = set(self.monitored_symbols)

        added   = current_symbols - cached_symbols
        removed = cached_symbols  - current_symbols

        if not added and not removed:
            return  # Sin cambios, no hacer nada

        logger.info(f"🔄 Símbolos cambiaron: +{len(added)} -{len(removed)} → reiniciando kline_ws_cache")
        self._init_kline_cache(current_symbols)
    

    def price_monitor_thread(self):
        """
        Monitor de precios — VERSIÓN WEBSOCKET.
        Lee velas directamente de KlineWebSocketCache en vez de hacer
        llamadas REST con get_klines(). Esto elimina el polling de 10s
        y mantiene data_cache siempre actualizado con latencia de ms.
        """
        logger.info("📡 Iniciando monitor de precios (KlineWebSocket)...")

        # Esperar a que el cache esté listo con datos iniciales
        time.sleep(8)

        while self.running:
            try:
                # Si el cache todavía no se inicializó, esperar
                if self.kline_ws_cache is None:
                    time.sleep(2)
                    continue

                if not self.monitored_symbols:
                    time.sleep(5)
                    continue

                symbols_snapshot = list(self.monitored_symbols)
                updated = 0

                for symbol in symbols_snapshot:
                    try:
                        # Leer velas cerradas del WebSocket (only_closed=True para análisis)
                        df_1m = self.kline_ws_cache.get_dataframe(symbol, '1m', only_closed=True)
                        df_5m = self.kline_ws_cache.get_dataframe(symbol, '5m', only_closed=True)

                        # Verificar que tenemos suficientes datos
                        if df_1m.empty or df_5m.empty:
                            continue
                        if len(df_1m) < 50 or len(df_5m) < 50:
                            continue

                        # El DataFrame ya viene con columnas:
                        # timestamp, open, high, low, close, volume, ...
                        # que es exactamente lo que espera data_cache.update_data()
                        self.data_cache.update_data(symbol, df_1m, df_5m)
                        updated += 1

                    except Exception as e:
                        logger.debug(f"Error leyendo klines WS para {symbol}: {e}")
                        continue

                if updated > 0:
                    logger.debug(f"📊 data_cache actualizado: {updated}/{len(symbols_snapshot)} símbolos (WS)")

                # Refresco rápido: el WebSocket ya empuja datos en tiempo real,
                # este loop solo propaga al data_cache cada segundo.
                time.sleep(1)

            except Exception as e:
                logger.error(f"Error en price_monitor_thread (WS): {e}")
                time.sleep(5)
    


    def strategy_analysis_thread(self):
        """Análisis de estrategia Heikin Ashi"""
        logger.info("🧠 Iniciando análisis de estrategia Heikin Ashi...")
        while self.running:
            try:
                # 🆕 VERIFICAR SI EL TRADING ESTÁ PAUSADO
                if not self.is_trading_allowed():
                    time.sleep(5)
                    continue
                
                if not self.monitored_symbols:
                    time.sleep(5)
                    continue
                
                symbols_snapshot = list(self.monitored_symbols)
                
                for symbol in symbols_snapshot:
                    # Verificar si ya tiene operación activa
                    if self.state_manager.get_trade(symbol):
                        continue
                    
                    # 🆕 VERIFICAR NUEVAMENTE ANTES DE ANALIZAR
                    if not self.is_trading_allowed():
                        break
                    
                    # Analizar señal
                    signal = self.analyze_heikin_ashi_signal(symbol)
                    
                    if signal:
                        try:
                            self.signal_queue.put_nowait(signal)
                        except Full:
                            logger.warning(f"⚠️ Cola de señales llena")
                            break
                
                time.sleep(5)  # Analizar cada 5 segundos
                
            except Exception as e:
                logger.error(f"❌ Error en análisis de estrategia: {e}")
                time.sleep(5)

    def execution_thread_func(self):
        """Hilo de ejecución"""
        logger.info("⚡ Iniciando hilo de ejecución...")
        while self.running:
            try:
                # Procesar señales de entrada
                try:
                    signal = self.signal_queue.get(timeout=0.5)
                    self.process_entry_signal(signal)
                except Empty:
                    pass
                
                # Procesar señales de salida
                try:
                    exit_signal = self.exit_queue.get(timeout=0.1)
                    self.process_exit_signal(exit_signal)
                except Empty:
                    pass
                
            except Exception as e:
                logger.error(f"Error en hilo de ejecución: {e}")
                time.sleep(1)
    
    def process_entry_signal(self, signal: Dict):
        """Procesa señal de entrada"""
        try:
            # 🆕 VERIFICAR SI EL TRADING ESTÁ PAUSADO
            if not self.is_trading_allowed():
                logger.debug(f"⏸️ Señal ignorada (trading pausado): {signal['symbol']}")
                return
            
            symbol = signal['symbol']
            
            # Verificar límite de operaciones
            if len(self.state_manager.get_all_active_symbols()) >= MAX_CONCURRENT_TRADES:
                return
            
            # Verificar si ya existe trade
            if self.state_manager.get_trade(symbol):
                return
            
            self.open_trade(signal)
            
        except Exception as e:
            logger.error(f"❌ Error procesando señal de entrada: {e}")

    def trade_monitor_thread_func(self):
        """Monitor de operaciones activas"""
        logger.info("👁️ Iniciando monitor de operaciones...")
        
        while self.running:
            try:
                # Obtener símbolos activos
                active_symbols = list(self.state_manager.get_all_active_symbols())
                
                if not active_symbols:
                    time.sleep(2)
                    continue
                
                # Verificar cada trade
                for symbol in active_symbols:
                    self.check_exit_and_update(symbol)
                    # 🔥 VERIFICAR PYRAMIDING
                    self.check_and_execute_pyramiding(symbol)
                
                time.sleep(1)  # Verificar cada segundo
                
            except Exception as e:
                logger.error(f"Error en monitor de operaciones: {e}")
                time.sleep(2)
    
    # ==================== ESTADO Y ESTADÍSTICAS ====================
    
    def show_status(self):
        """Muestra estado del bot"""
        active_symbols = self.state_manager.get_all_active_symbols()
        active_count = len(active_symbols)
        
        completed_count = len(self.completed_trades)
        if completed_count > 0:
            wins = sum(1 for t in self.completed_trades if t['result'] > 0)
            winrate = (wins / completed_count * 100)
            
            # 🆕 Estadísticas de recuperación
            recovery_trades = [t for t in self.completed_trades if t.get('is_recovery', False)]
            recovery_count = len(recovery_trades)
            recovery_wins = sum(1 for t in recovery_trades if t['result'] > 0)
        else:
            winrate = 0
            recovery_count = 0
            recovery_wins = 0

        
        pnl_summary = self.compute_unrealized_pnl_summary()
        
        logger.info(f"\n📊 ESTADO DEL BOT HEIKIN ASHI - {datetime.now().strftime('%H:%M:%S')}")
        logger.info("=" * 60)
        
        # 🆕 MOSTRAR SI ESTÁ PAUSADO
        with self.pause_lock:
            if self.trading_paused:
                remaining = (self.pause_until - datetime.now()).total_seconds()
                logger.warning(f"⏸️ TRADING PAUSADO - Reanudar en {remaining:.0f}s")
        
        logger.info(f"💰 Balance: ${self.balance:.2f}")
        logger.info("📌 PnL No Realizado (neto estimado): $%.2f | Balance + PnL (neto): $%.2f",
            pnl_summary['total_unrealized_net_est'],
            pnl_summary['combined_balance_net_est'])
        
        logger.info(f"📈 Operaciones activas: {active_count}/{MAX_CONCURRENT_TRADES}")
        logger.info(f"📋 Operaciones completadas: {completed_count}")
        logger.info(f"🎯 Winrate: {winrate:.1f}%")
        
        # 🆕 MOSTRAR RACHA POR ROI
        if self.racha_roi['count'] > 0:
            logger.info(f"🎲 Racha ROI: {self.racha_roi['cierres']} ({self.racha_roi['count']}/3)")
            logger.info(f"🔄 Inversión activa: {self.inversion_posiciones}")
        
        # Mostrar stats de recuperación
        if recovery_count > 0:
            recovery_winrate = (recovery_wins / recovery_count * 100) if recovery_count > 0 else 0
            logger.info(f"🔄 Recuperaciones: {recovery_count} ({recovery_winrate:.1f}% exitosas)")
        
        logger.info(f"📡 Símbolos monitoreados: {len(self.monitored_symbols)}")
        
        # Mostrar estado de emergencia
        if EMERGENCY_STOP_ENABLED:
            daily_pnl = self.balance - self.daily_start_balance
            logger.info(f"⚠️ PnL diario: ${daily_pnl:.2f} / Límite: ${MAX_DAILY_LOSS:.2f}")
        
        if active_symbols:
            logger.info("\n📄 OPERACIONES ACTIVAS:")
            for symbol in active_symbols:
                trade = self.state_manager.get_trade(symbol)
                if not trade:
                    continue
                
                current_price = self.data_cache.get_current_price(symbol)
                if current_price:
                    if trade.trade_type == "LONG":
                        unrealized_pnl = (current_price - trade.entry_price) * trade.quantity
                    else:
                        unrealized_pnl = (trade.entry_price - current_price) * trade.quantity
                    
                    # 🔥 USAR NUEVO CÁLCULO DE ROI BASADO EN PRECIO PROMEDIO
                    roi = self.calculate_position_roi(trade, current_price)
                    
                    # Mostrar si es recuperación o pyramiding
                    recovery_tag = " [🔄 RECUPERACIÓN]" if trade.is_recovery_mode else ""
                    pyramid_tag = f" [🔥 PYRAMID x{len(trade.pyramid_levels)+1}]" if len(trade.pyramid_levels) > 0 else ""
                    
                    # Recalcular PnL usando precio promedio ponderado
                    if trade.trade_type == "LONG":
                        unrealized_pnl = (current_price - trade.weighted_avg_price) * trade.total_quantity
                    else:
                        unrealized_pnl = (trade.weighted_avg_price - current_price) * trade.total_quantity
                    
                    logger.info(
                        f"   {trade.trade_type} {symbol}{recovery_tag}{pyramid_tag} | "
                        f"Entrada Prom: ${trade.weighted_avg_price:.4f} | "
                        f"Cantidad: {trade.total_quantity:.4f} | "
                        f"Actual: ${current_price:.4f} | "
                        f"ROI: {roi:.2f}% | "
                        f"PnL: ${unrealized_pnl:.2f} | "
                        f"Velas: {trade.bars_held}"
                    )
                    
                    # Si es recuperación, mostrar ROI combinado
                    if trade.is_recovery_mode:
                        combined_roi = self.calculate_combined_recovery_roi(trade, current_price)
                        logger.info(f"      ROI Combinado: {combined_roi:.2f}% | Objetivo: {ROI_RECOVERY_TARGET_FINAL}%")
        
        logger.info("=" * 60)

    def show_detailed_status(self):
        """Muestra estado detallado"""
        logger.info("\n📈 ESTADO DETALLADO - ESTRATEGIA HEIKIN ASHI")
        logger.info("=" * 60)
        
        if self.completed_trades:
            recent_trades = self.completed_trades[-10:]
            wins = sum(1 for t in self.completed_trades if t['result'] > 0)
            total = len(self.completed_trades)
            winrate = (wins / total * 100) if total > 0 else 0
            total_pnl = sum(t['result'] for t in self.completed_trades)
            avg_bars_held = sum(t['bars_held'] for t in self.completed_trades) / total
            avg_adjustments = sum(t['tp_sl_updates'] for t in self.completed_trades) / total
            
            # 🆕 Estadísticas de recuperación
            recovery_trades = [t for t in self.completed_trades if t.get('is_recovery', False)]
            normal_trades = [t for t in self.completed_trades if not t.get('is_recovery', False)]
            
            logger.info(f"📊 ESTADÍSTICAS GENERALES:")
            logger.info(f"   Total operaciones: {total}")
            logger.info(f"   Normales: {len(normal_trades)} | Recuperaciones: {len(recovery_trades)}")
            logger.info(f"   Ganadas: {wins} | Perdidas: {total - wins}")
            logger.info(f"   Winrate: {winrate:.2f}%")
            logger.info(f"   PnL total: ${total_pnl:.2f}")
            logger.info(f"   Promedio velas mantenidas: {avg_bars_held:.1f}")
            logger.info(f"   Promedio ajustes TP/SL: {avg_adjustments:.1f}")
            
            # 🆕 Stats de recuperación
            if recovery_trades:
                recovery_wins = sum(1 for t in recovery_trades if t['result'] > 0)
                recovery_winrate = (recovery_wins / len(recovery_trades) * 100)
                recovery_pnl = sum(t['result'] for t in recovery_trades)
                
                logger.info(f"\n🔄 ESTADÍSTICAS DE RECUPERACIÓN:")
                logger.info(f"   Total recuperaciones: {len(recovery_trades)}")
                logger.info(f"   Exitosas: {recovery_wins} ({recovery_winrate:.1f}%)")
                logger.info(f"   PnL recuperaciones: ${recovery_pnl:.2f}")
            
            # 🔥 NUEVO: Stats de pyramiding
            pyramid_trades = [t for t in self.completed_trades if t.get('pyramid_levels', 0) > 0]
            if pyramid_trades:
                pyramid_wins = sum(1 for t in pyramid_trades if t['result'] > 0)
                pyramid_winrate = (pyramid_wins / len(pyramid_trades) * 100)
                pyramid_pnl = sum(t['result'] for t in pyramid_trades)
                avg_pyramid_levels = sum(t.get('pyramid_levels', 0) for t in pyramid_trades) / len(pyramid_trades)
                
                logger.info(f"\n🔥 ESTADÍSTICAS DE PYRAMIDING:")
                logger.info(f"   Total con pyramiding: {len(pyramid_trades)}")
                logger.info(f"   Exitosas: {pyramid_wins} ({pyramid_winrate:.1f}%)")
                logger.info(f"   PnL pyramiding: ${pyramid_pnl:.2f}")
                logger.info(f"   Niveles promedio: {avg_pyramid_levels:.1f}")
            
            logger.info(f"\n📋 ÚLTIMAS 10 OPERACIONES:")
            for trade in recent_trades:
                status = "✅" if trade['result'] > 0 else "❌"
                recovery_tag = " [🔄]" if trade.get('is_recovery', False) else ""
                pyramid_tag = f" [🔥x{trade.get('pyramid_levels', 0)+1}]" if trade.get('pyramid_levels', 0) > 0 else ""
                logger.info(
                    f"   {status} {trade['type']} {trade['symbol']}{recovery_tag}{pyramid_tag} | "
                    f"ROI: {trade['roi']:.2f}% | "
                    f"${trade['result']:.2f} | "
                    f"Velas: {trade['bars_held']} | "
                    f"{trade['reason']}"
                )
        
        logger.info("=" * 60)
    
    def close_all_positions_global_and_wait(self, reason: str):
        """
        Cierra TODAS las posiciones activas y espera confirmación.
        NO continúa hasta que esté seguro de que todas se cerraron.
        """
        active_symbols = list(self.state_manager.get_all_active_symbols())
        
        if not active_symbols:
            logger.info(f"No hay posiciones activas para cerrar ({reason})")
            return
        
        logger.warning(f"Iniciando cierre global de {len(active_symbols)} posición(es)...")
        
        # 1. Enviar comandos de cierre para TODAS las posiciones
        for symbol in active_symbols:
            try:
                trade = self.state_manager.get_trade(symbol)
                if trade and not self.state_manager.is_closing(symbol):
                    self.state_manager.mark_closing(symbol)
                    cmd = OrderCommandData(
                        command="CLOSE_POSITION",
                        symbol=symbol,
                        data={'reason': reason}, trade_id=trade.trade_id
                    )
                    self.order_executor.submit_command(cmd)
                    logger.warning(f"Enviado cierre para {symbol}")
            except Exception as e:
                logger.error(f"Error enviando cierre para {symbol}: {e}")
        
        # 2. Esperar hasta que se cierren TODAS
        max_wait_seconds = 30
        start_time = time.time()
        
        while time.time() - start_time < max_wait_seconds:
            remaining_symbols = list(self.state_manager.get_all_active_symbols())
            
            if not remaining_symbols:
                logger.info(f"Todas las posiciones se han cerrado correctamente")
                return
            
            logger.info(f"Esperando cierre de {len(remaining_symbols)} posiciones: {', '.join(remaining_symbols)}")
            time.sleep(2)
        
        # 3. Si después de 30 segundos aún hay posiciones, forzar limpieza
        logger.error(f"TIMEOUT: Aún hay {len(remaining_symbols)} posiciones después de 30 segundos")
        logger.error(f"Forzando limpieza de state manager para: {remaining_symbols}")
        
        for symbol in remaining_symbols:
            try:
                self.state_manager.cleanup_symbol(symbol)
                logger.warning(f"Limpieza forzada de {symbol}")
            except Exception as e:
                logger.error(f"Error limpiando {symbol}: {e}")

    def close_all_positions_emergency(self):
        """Cierra todas las posiciones en emergencia"""
        active_symbols = list(self.state_manager.get_all_active_symbols())
        
        logger.critical(f"CIERRE DE EMERGENCIA: {len(active_symbols)} posiciones")
        
        for symbol in active_symbols:
            try:
                trade = self.state_manager.get_trade(symbol)
                if trade and not self.state_manager.is_closing(symbol):
                    self.state_manager.mark_closing(symbol)
                    cmd = OrderCommandData(
                        command="CLOSE_POSITION",
                        symbol=symbol,
                        data={'reason': 'EMERGENCY_STOP'}, trade_id=trade.trade_id
                    )
                    self.order_executor.submit_command(cmd)
                    logger.critical(f"Cierre de emergencia: {symbol}")
                    
            except Exception as e:
                logger.error(f"Error cerrando {symbol} en emergencia: {e}")
        
        # Esperar un poco para que se procesen los cierres
        time.sleep(2)
        
        # Limpiar lo que quede
        remaining = list(self.state_manager.get_all_active_symbols())
        for symbol in remaining:
            self.state_manager.cleanup_symbol(symbol)
            logger.warning(f"Limpieza forzada de emergencia: {symbol}")
        
        self.running = False

    def export_trades_to_csv(self, filename: str = "heikin_ashi_trades_recovery.csv"):
        """Exporta trades a CSV"""
        if not self.completed_trades:
            logger.info("No hay trades para exportar")
            return
        
        df = pd.DataFrame(self.completed_trades)
        
        # Guardar trades básicos
        df_basic = df.drop('tp_sl_history', axis=1, errors='ignore')
        df_basic.to_csv(filename, index=False)
        logger.info(f"📁 Trades exportados a {filename}")
        
        # Guardar historial de ajustes TP/SL
        adjustments = []
        for trade in self.completed_trades:
            for adj in trade.get('tp_sl_history', []):
                adjustments.append({
                    'symbol': trade['symbol'],
                    'trade_type': trade['type'],
                    'entry_time': trade['entry_time'],
                    'adjustment_time': adj['timestamp'],
                    'tp': adj['tp'],
                    'sl': adj['sl'],
                    'bars_held': adj['bars_held'],
                    'reason': adj['reason']
                })
        
        if adjustments:
            df_adj = pd.DataFrame(adjustments)
            adj_filename = filename.replace('.csv', '_adjustments.csv')
            df_adj.to_csv(adj_filename, index=False)
            logger.info(f"📁 Ajustes TP/SL exportados a {adj_filename}")

    def evaluar_racha_roi(self):
        """
        🆕 Evalúa cada 3 cierres por ROI si debe invertir self.inversion_posiciones
        """
        if self.racha_roi['count'] < 3:
            return
        
        # Contar positivos y negativos
        positivos = self.racha_roi['cierres'].count('positivo')
        negativos = self.racha_roi['cierres'].count('negativo')
        
        logger.info("=" * 60)
        logger.info("🎲 EVALUACIÓN DE RACHA (cada 3 cierres por ROI)")
        logger.info(f"   Últimos 3 resultados: {self.racha_roi['cierres']}")
        logger.info(f"   Positivos: {positivos} | Negativos: {negativos}")
        logger.info(f"   Inversión actual: {self.inversion_posiciones}")
        
        # LÓGICA DE DECISIÓN:
        # Si hay más negativos que positivos (2 o 3 negativos), invertir estrategia
        if negativos >= 2:
            self.inversion_posiciones = not self.inversion_posiciones
            logger.warning(f"   🔄 INVERSIÓN ACTIVADA: {self.inversion_posiciones}")
        else:
            # Si hay más positivos (2 o 3), mantener estrategia actual
            logger.info(f"   ✅ Estrategia MANTENIDA: {self.inversion_posiciones}")
        
        # Vaciar el diccionario para empezar nueva racha
        self.racha_roi = {
            'cierres': [],
            'count': 0
        }
        logger.info("   📊 Racha reiniciada")
        logger.info("=" * 60)
   
    def monitor_global_roi_thread(self):
        """Monitorea el ROI promedio global - VERSIÓN CORREGIDA"""
        logger.info("🔍 Iniciando monitor global de ROI...")

        while self.running:
            try:
                # Si está pausado, solo verificar si debe reanudar
                if self.trading_paused:
                    self.resume_trading_if_ready()
                    time.sleep(GLOBAL_ROI_CHECK_INTERVAL)
                    continue
                
                # Calcular ROI promedio combinando trades cerrados y abiertos
                total_roi = 0.0
                count = 0

                # 1. ROI de trades completados
                for t in self.completed_trades:
                    total_roi += t.get('roi', 0.0)
                    count += 1

                # 2. ROI de trades abiertos
                active_symbols = list(self.state_manager.get_all_active_symbols())
                for symbol in active_symbols:
                    trade = self.state_manager.get_trade(symbol)
                    if not trade:
                        continue
                    current_price = self.data_cache.get_current_price(symbol)
                    if current_price is None:
                        continue
                    roi = self.calculate_current_roi(trade, current_price)
                    total_roi += roi
                    count += 1

                if count == 0:
                    time.sleep(GLOBAL_ROI_CHECK_INTERVAL)
                    continue

                average_roi = total_roi / count
                logger.info(f"📊 ROI promedio global actual: {average_roi:.2f}% ({count} operaciones)")

                # REINICIO POR GANANCIA
                if average_roi >= ROI_RESET_THRESHOLD_POSITIVE:
                    logger.warning(f"🎯 ROI promedio >= {ROI_RESET_THRESHOLD_POSITIVE}%. Procesando cierre...")
                    
                    # 🔧 FIX 1: PROCESAR TODOS LOS TRADES ABIERTOS ANTES DE LIMPIAR
                    self._process_active_trades_for_roi_reset(active_symbols, "ROI_POSITIVE_RESET")
                    
                    # Ahora sí limpiar
                    self.completed_trades.clear()
                    
                    # Registrar en racha
                    self.racha_roi['cierres'].append('positivo')
                    self.racha_roi['count'] += 1
                    logger.info(f"✅ Cierre por ROI positivo registrado ({self.racha_roi['count']}/3)")
                    
                    # Evaluar racha
                    self.evaluar_racha_roi()
                    
                    # Pausar trading
                    self.pause_trading(seconds=60)
                    logger.info("⏸️ Trading pausado por 60 segundos después del reinicio")
                    time.sleep(3)
                    continue

                # REINICIO POR PÉRDIDA
                elif  average_roi<= ROI_RESET_THRESHOLD_NEGATIVE:
                    logger.warning(f"⚠️ ROI promedio <= {ROI_RESET_THRESHOLD_NEGATIVE}%. Procesando cierre...")
                    
                    # 🔧 FIX 1: PROCESAR TODOS LOS TRADES ABIERTOS ANTES DE LIMPIAR
                    self._process_active_trades_for_roi_reset(active_symbols, "ROI_NEGATIVE_RESET")
                    
                    # Ahora sí limpiar
                    self.completed_trades.clear()
                    
                    # Registrar en racha
                    self.racha_roi['cierres'].append('negativo')
                    self.racha_roi['count'] += 1
                    logger.warning(f"❌ Cierre por ROI negativo registrado ({self.racha_roi['count']}/3)")
                    
                    # Evaluar racha
                    self.evaluar_racha_roi()
                    
                    # Pausar trading
                    self.pause_trading(seconds=60)
                    logger.info("⏸️ Trading pausado por 60 segundos después del reinicio por pérdida")
                    time.sleep(3)
                    continue

                time.sleep(GLOBAL_ROI_CHECK_INTERVAL)

            except Exception as e:
                logger.error(f"❌ Error en monitor_global_roi_thread: {e}")
                time.sleep(10)

    def _process_active_trades_for_roi_reset(self, active_symbols: List[str], reason: str):
        """
        🔧 FIX CRÍTICO: Procesa todos los trades abiertos para actualizar el balance
        antes de hacer limpeza forzada
        """
        logger.info(f"💰 Procesando {len(active_symbols)} trades abiertos para cierre por {reason}...")
        
        for symbol in active_symbols:
            try:
                trade = self.state_manager.get_trade(symbol)
                if not trade:
                    logger.debug(f"⚠️ Trade no encontrado para {symbol}")
                    continue
                
                # Obtener precio actual
                current_price = self.data_cache.get_current_price(symbol)
                if current_price is None or current_price <= 0:
                    logger.warning(f"⚠️ Precio inválido para {symbol}: {current_price}")
                    current_price = trade.entry_price
                
                logger.info(f"📌 Cerrando {symbol} a ${current_price:.4f} por {reason}")
                
                # REGISTRAR DIRECTAMENTE (sin ir por OrderExecutor que puede fallar)
                self.order_executor._record_closed_position(symbol, trade, current_price, reason)              
                # Limpiar estado
                self.state_manager.cleanup_symbol(symbol)
                
            except Exception as e:
                logger.error(f"❌ Error procesando {symbol}: {e}")
                import traceback
                logger.error(traceback.format_exc())
                # Intentar al menos limpiarlo del state manager
                try:
                    self.state_manager.cleanup_symbol(symbol)
                except:
                    pass

    def validate_active_symbols_monitoring(self):
        """
        ðŸ†• NUEVA FUNCIÓN: Valida que TODOS los símbolos con operaciones activas
        estén siendo monitoreados. Se ejecuta periódicamente.
        """
        try:
            active_symbols = self.state_manager.get_all_active_symbols()
            
            if not active_symbols:
                return  # No hay operaciones activas
            
            monitored = self.monitored_symbols
            missing = active_symbols - monitored
            
            if not missing:
                logger.debug(f"✅ Validación OK: {len(active_symbols)} activos, todos monitoreados")
                return
            
            # ðŸ†• PROBLEMA DETECTADO: Hay símbolos activos NO monitoreados
            logger.error(f"🚨 CRÍTICO: {len(missing)} símbolos ACTIVOS NO están siendo monitoreados!")
            logger.error(f"   Símbolos no monitoreados: {', '.join(sorted(missing))}")
            
            # Añadirlos de emergencia
            for symbol in missing:
                trade = self.state_manager.get_trade(symbol)
                if trade:
                    logger.warning(f"   ⚠️ {symbol}: {trade.trade_type} @ ${trade.entry_price:.4f}")
            
            # ACCIÓN CORRECTIVA
            logger.warning("   🔧 Añadiendo automáticamente a monitoreo...")
            self.monitored_symbols = self.monitored_symbols.union(missing)
            self.top_symbols = list(self.monitored_symbols)
            
            # Actualizar WebSocket
            if self.data_cache.ws_price_cache:
                try:
                    symbols_list = list(self.monitored_symbols)
                    self.data_cache.initialize_websocket(symbols_list)
                    logger.info(f"   ✅ WebSocket reconfigured para {len(symbols_list)} símbolos")
                except Exception as e:
                    logger.error(f"   Error reconfig WebSocket: {e}")
            
            logger.info(f"   ✅ Validación completada: {len(self.monitored_symbols)} símbolos ahora monitoreados")
            
        except Exception as e:
            logger.error(f"❌ Error en validate_active_symbols_monitoring: {e}")
            import traceback
            logger.error(traceback.format_exc())
  
    # ==================== CONTROL PRINCIPAL ====================
    
    # ==================== NUEVOS MÉTODOS ====================

    def set_cooldown(self, hours: float):
        """Activa el cooldown de 4 horas"""
        with self.cooldown_lock:
            self.in_cooldown = True
            self.cooldown_until = datetime.now() + timedelta(hours=hours)
            logger.warning(f"⏸️ COOLDOWN ACTIVADO POR {hours} HORAS")
            logger.warning(f"   Hasta: {self.cooldown_until.strftime('%Y-%m-%d %H:%M:%S')}")

    def is_in_cooldown(self) -> bool:
        """Verifica si está en período de cooldown"""
        with self.cooldown_lock:
            if not self.in_cooldown:
                return False
            
            if datetime.now() >= self.cooldown_until:
                self.in_cooldown = False
                logger.info("✅ COOLDOWN FINALIZADO - Reanudando operaciones")
                return False
            
            remaining = (self.cooldown_until - datetime.now()).total_seconds()
            if remaining > 0:
                logger.debug(f"⏳ En cooldown. Reanudar en {remaining/3600:.1f} horas")
            
            return True

    def check_profit_target_reached(self) -> bool:
        """
        Verifica si se alcanzó el target de ganancia actual
        Retorna True si se alcanzó
        """
        current_gain = self.balance - self.daily_start_balance
        current_target = self.profit_target_manager.get_current_target()
        
        # Mostrar estado
        logger.info(f"💰 Balance actual: ${self.balance:.2f}")
        logger.info(f"📈 Ganancia acumulada: ${current_gain:.2f}")
        logger.info(f"🎯 Target actual: ${current_target:.2f}")
        
        if current_gain >= current_target:
            logger.warning("=" * 60)
            logger.warning(f"🎉 ¡¡¡TARGET DE GANANCIA ALCANZADO!!!")
            logger.warning(f"   Ganancia: ${current_gain:.2f} >= Target: ${current_target:.2f}")
            logger.warning(f"   Balance total: ${self.balance:.2f}")
            logger.warning("=" * 60)
            return True
        
        return False

    def handle_profit_target_closure(self):
        """
        Maneja el cierre cuando se alcanza un profit target:
        1. Cierra todas las posiciones
        2. Actualiza el target
        3. Activa cooldown de 4 horas
        4. Pausa el trading
        """
        logger.warning("=" * 60)
        logger.warning("🎯 EJECUTANDO CIERRE POR PROFIT TARGET")
        logger.warning("=" * 60)
        
        try:
            # 1. Obtener posiciones activas
            active_symbols = list(self.state_manager.get_all_active_symbols())
            logger.info(f"📊 Posiciones activas: {len(active_symbols)}")
            
            if active_symbols:
                logger.info("🚀 Cerrando todas las posiciones...")
                
                # Enviar comandos de cierre
                for symbol in active_symbols:
                    try:
                        trade = self.state_manager.get_trade(symbol)
                        if trade and not self.state_manager.is_closing(symbol):
                            self.state_manager.mark_closing(symbol)
                            cmd = OrderCommandData(
                                command="CLOSE_POSITION",
                                symbol=symbol,
                                data={'reason': 'PROFIT_TARGET_REACHED'}, trade_id=trade.trade_id
                            )
                            self.order_executor.submit_command(cmd)
                            logger.info(f"   ✅ Cierre enviado: {symbol}")
                    except Exception as e:
                        logger.error(f"   ❌ Error cerrando {symbol}: {e}")
                
                # 2. Esperar a que se cierren (máximo 30 segundos)
                logger.info("⏳ Esperando confirmación de cierres...")
                max_wait = 30
                start_time = time.time()
                
                while time.time() - start_time < max_wait:
                    remaining_symbols = list(self.state_manager.get_all_active_symbols())
                    if not remaining_symbols:
                        logger.info("✅ Todas las posiciones cerradas")
                        break
                    logger.debug(f"   Esperando: {remaining_symbols}")
                    time.sleep(2)
                else:
                    remaining = list(self.state_manager.get_all_active_symbols())
                    if remaining:
                        logger.warning(f"⚠️ Timeout: {len(remaining)} posiciones aún abiertas")
                        for symbol in remaining:
                            self.state_manager.cleanup_symbol(symbol)
            
            else:
                logger.info("📭 No hay posiciones activas para cerrar")
            
            # 3. Actualizar target
            self.profit_target_manager.set_next_target()
            
            # 4. Resetear contador diario
            self.daily_start_balance = self.balance
            logger.info(f"💾 Nuevo punto de partida: ${self.daily_start_balance:.2f}")
            
            # 5. Activar cooldown de 4 horas
            self.set_cooldown(hours=self.profit_target_manager.wait_hours)
            
            # 6. Pausar trading (como medida adicional)
            self.pause_trading(seconds=int(self.profit_target_manager.wait_hours * 3600))
            
            logger.warning("=" * 60)
            logger.warning("✅ CICLO COMPLETADO - ESPERANDO 4 HORAS...")
            logger.warning("=" * 60)
            
        except Exception as e:
            logger.error(f"❌ Error en handle_profit_target_closure: {e}")
            import traceback
            logger.error(traceback.format_exc())


    # ==================== NUEVO THREAD ====================

    def profit_target_monitor_thread(self):
            """
            🎯 Monitorea continuamente si se alcanzó el profit target.
            
            CAMBIO IMPORTANTE: 
            Ya no cierra posiciones directamente. Detecta el objetivo alcanzado, 
            pausa nuevas entradas y ESPERA a que 'check_exit_and_update' 
            vacíe las posiciones una a una. Luego configura el cooldown.
            """
            logger.info("🎯 Iniciando monitor de profit targets (Modo Supervisor)...")
            logger.info(f"   Base de ganancia: ${self.profit_target_manager.base_amount:.2f}")
            logger.info(f"   Cooldown: {self.profit_target_manager.wait_hours} horas")
            
            check_interval = 5  # Verificar cada 5 segundos
            
            while self.running:
                try:
                    # 1. Verificar si estamos en cooldown (Prioridad 1)
                    if self.is_in_cooldown():
                        remaining_seconds = (self.cooldown_until - datetime.now()).total_seconds()
                        if remaining_seconds > 0:
                            if int(remaining_seconds) % 3600 == 0: # Log cada hora
                                logger.info(f"⏳ En cooldown: {remaining_seconds / 3600:.1f} horas restantes")
                            time.sleep(check_interval)
                            continue
                        else:
                            # El cooldown terminó automáticamente en is_in_cooldown(), seguimos.
                            pass
                    
                    # 2. Verificar si se alcanzó el target
                    # Nota: check_exit_and_update también lee esto para cerrar posiciones individuales
                    if self.profit_target_manager.is_target_reached():
                        
                        logger.warning("=" * 60)
                        logger.warning("🎉 TARGET ALCANZADO - Iniciando protocolo de cierre ordenado")
                        logger.warning("=" * 60)

                        # A. PAUSAR TRADING INMEDIATAMENTE
                        # Evita que se abran nuevas operaciones mientras intentamos cerrar todo
                        self.pause_trading(seconds=300) # Pausa temporal de seguridad (5 min)

                        # B. ESPERAR A QUE SE CIERREN LAS POSICIONES
                        # check_exit_and_update está enviando las órdenes de cierre a la cola.
                        # Este hilo solo debe esperar a que terminen.
                        
                        max_wait = 300 # Tiempo máximo de espera en segundos
                        start_wait = time.time()
                        
                        while True:
                            active_count = len(self.state_manager.get_all_active_symbols())
                            
                            if active_count == 0:
                                logger.info("✅ Todas las posiciones se han cerrado correctamente.")
                                break
                            
                            if time.time() - start_wait > max_wait:
                                logger.error(f"⚠️ Timeout esperando cierre: Quedan {active_count} posiciones.")
                                # Aquí podrías forzar un cierre de emergencia si quisieras, 
                                # pero respetando tu lógica, solo logueamos y procedemos.
                                break
                                
                            logger.info(f"⏳ Esperando que 'check_exit_and_update' cierre {active_count} posiciones...")
                            self.Bandera_de_Cierre_por_target = True
                            time.sleep(2)

                        # C. ACTUALIZAR ESTADO DEL BOT (Administrativo)
                        # Una vez cerradas las posiciones, preparamos el siguiente ciclo
                        
                        # 1. Actualizar el target monetario
                        self.profit_target_manager.set_next_target()
                        
                        # 2. Resetear el punto de partida diario (PnL Realizado se consolida)
                        self.daily_start_balance = self.balance
                        logger.info(f"💾 Nuevo balance base para cálculo: ${self.daily_start_balance:.2f}")

                        # 3. Activar el Cooldown largo
                        self.set_cooldown(hours=self.profit_target_manager.wait_hours)
                        
                        logger.warning("✅ CICLO COMPLETADO - Bot entra en reposo.")
                        self.Bandera_de_Cierre_por_target = False
                        
                        time.sleep(5)
                        continue
                    
                    time.sleep(check_interval)
                    
                except Exception as e:
                    logger.error(f"❌ Error en profit_target_monitor_thread: {e}")
                    time.sleep(10)

    # ==================== MODIFICAR run() ====================
    # Reemplazar en la función run() donde inicia los threads, agregar:

    def run(self):
        """VERSIÓN MEJORADA con monitor de profit targets"""
        logger.info("🤖 INICIANDO BOT DE TRADING - ESTRATEGIA HEIKIN ASHI")
        logger.info("=" * 60)
        logger.info(f"💰 Balance inicial: ${self.balance:.2f}")
        logger.info(f"🎯 SISTEMA DE PROFIT TARGETS ACTIVO")
        logger.info(f"   Target inicial: ${self.profit_target_manager.get_current_target():.2f}")
        logger.info(f"   Incremento: ${self.profit_target_manager.base_amount:.2f}")
        logger.info(f"   Cooldown: {self.profit_target_manager.wait_hours} horas")
        logger.info("=" * 60)
        
        self.running = True
        self.daily_start_balance = self.balance
        
        # Iniciar OrderExecutor
        self.order_executor.start()
        
        try:
            # Obtener símbolos iniciales
            logger.info("🔍 Obteniendo símbolos iniciales...")
            self.update_monitored_symbols()
            
                 # INICIALIZAR WEBSOCKET DE PRECIOS (para precios en tiempo real)
            if self.monitored_symbols:
                     symbols_list = list(self.monitored_symbols)
                     self.data_cache.initialize_websocket(symbols_list)
                     logger.info(f"✅ WebSocket de precios iniciado para {len(symbols_list)} símbolos")
            
                 # 🆕 INICIALIZAR WEBSOCKET DE KLINES (velas 1m y 5m en tiempo real)
            if self.monitored_symbols:
                     logger.info("📊 Iniciando KlineWebSocketCache...")
                     self._init_kline_cache(list(self.monitored_symbols))
                     # Dar tiempo para el backfill REST inicial antes de arrancar threads
                     logger.info("⏳ Esperando backfill inicial de klines (15s)...")
                     time.sleep(15)
            
            # Iniciar threads normales
            self.price_thread = threading.Thread(target=self.price_monitor_thread, daemon=True)
            self.strategy_thread = threading.Thread(target=self.strategy_analysis_thread, daemon=True)
            self.execution_thread = threading.Thread(target=self.execution_thread_func, daemon=True)
            self.monitor_thread = threading.Thread(target=self.trade_monitor_thread_func, daemon=True)
            self.roi_thread = threading.Thread(target=self.monitor_global_roi_thread, daemon=True)
            
            # 🎯 NUEVO THREAD: Monitor de Profit Targets
            self.profit_target_thread = threading.Thread(
                target=self.profit_target_monitor_thread, 
                daemon=True
            )

             # 🆕 NUEVO THREAD: Monitor BTC/EMA20
            self.btc_ema20_thread = threading.Thread(target=self.btc_ema20_monitor_thread,args=(180,),daemon=True)# Verificar cada 5 minutos
            
            # Iniciar todos los threads
            self.roi_thread.start()
            self.price_thread.start()
            self.strategy_thread.start()
            self.execution_thread.start()
            self.monitor_thread.start()
            self.profit_target_thread.start()  # 🎯 Iniciar monitor
            self.btc_ema20_thread.start()  # ✅ AÑADIR ESTA LÍNEA
            
            # Thread para actualizar símbolos periódicamente
            def periodic_symbols_update():
                """Actualiza símbolos cada 10 minutos"""
                while self.running:
                    try:
                        time.sleep(600)
                        logger.info("Actualizando lista de símbolos...")
                        self.update_monitored_symbols()
                    except Exception as e:
                        logger.error(f"Error en actualización periódica: {e}")
            
            symbols_thread = threading.Thread(target=periodic_symbols_update, daemon=True)
            symbols_thread.start()
            
            # Loop principal mejorado
            status_counter = 0
            validation_counter = 0
            target_display_counter = 0
            
            while self.running and not self.emergency_stop:
                # Mostrar estado cada 20 segundos
                if status_counter % 20 == 0:
                    self.show_status()
                
                # Mostrar estado detallado cada 60 segundos
                if status_counter % 60 == 0:
                    self.show_detailed_status()
                
                # Mostrar estado del profit target cada 30 segundos
                if target_display_counter % 30 == 0:
                    current_gain = self.balance - self.daily_start_balance
                    current_target = self.profit_target_manager.get_current_target()
                    in_cooldown = self.is_in_cooldown()
                    status = "⏸️ EN COOLDOWN" if in_cooldown else "🟢 OPERANDO"

                    pnl_summary = self.compute_unrealized_pnl_summary()
                    unreal_gross = pnl_summary['total_unrealized_gross']
                    combined_gross = pnl_summary['combined_balance_gross']

                    logger.info(f"{status} | 💰 Ganancia: ${current_gain:.2f} | 🎯 Target: ${current_target:.2f} | "
                                f"PnL no realizado: ${unreal_gross:.2f} | Balance+PnL: ${combined_gross:.2f}")

                
                # Validar símbolos activos cada 30 segundos
                if validation_counter % 30 == 0:
                    self.validate_active_symbols_monitoring()
                
                status_counter += 1
                validation_counter += 1
                target_display_counter += 1
                time.sleep(1)
                
        except KeyboardInterrupt:
            logger.info("\n🛑 Bot detenido por el usuario")
            self.running = False
        except Exception as e:
            logger.error(f"❌ Error crítico: {e}")
            import traceback
            logger.error(traceback.format_exc())
            self.running = False
        finally:
            self.cleanup()


    # ==================== MODIFICAR cleanup() ====================
    # Reemplazar en cleanup():

    def cleanup(self):
        """VERSIÓN MEJORADA con limpieza del profit target thread"""
        logger.info("🧹 Limpiando recursos...")
        self.running = False
        
        # Detener WebSocket de precios
        self.data_cache.stop_websocket()

        # 🆕 Detener WebSocket de klines
        if self.kline_ws_cache is not None:
            try:
                self.kline_ws_cache.stop()
                logger.info("✅ KlineWebSocketCache detenido")
            except Exception as e:
                logger.error(f"Error deteniendo kline_ws_cache: {e}")
        
        # Detener OrderExecutor
        self.order_executor.stop()
        
        # Esperar profit target thread
        if hasattr(self, "profit_target_thread") and self.profit_target_thread and self.profit_target_thread.is_alive():
            self.profit_target_thread.join(timeout=5)

        if hasattr(self, "roi_thread") and self.roi_thread and self.roi_thread.is_alive():
            self.roi_thread.join(timeout=5)
        
        # Esperar otros threads
        threads = [self.price_thread, self.strategy_thread, 
                self.execution_thread, self.monitor_thread]
        for thread in threads:
            if thread and thread.is_alive():
                thread.join(timeout=5)
        
        # Exportar datos
        self.export_trades_to_csv()
        
        # Mostrar resumen final mejorado
        self.show_final_summary_with_targets()


    # ==================== NUEVO RESUMEN FINAL ====================

    def show_final_summary_with_targets(self):
        """Resumen final con información de profit targets"""
        logger.info("\n📊 RESUMEN FINAL - SISTEMA DE PROFIT TARGETS")
        logger.info("=" * 60)
        logger.info(f"💰 Balance inicial: ${self.daily_start_balance:.2f}")
        logger.info(f"💰 Balance final: ${self.balance:.2f}")
        logger.info(f"📈 Ganancia/Pérdida: ${self.balance - self.daily_start_balance:.2f}")
        
        # Información de targets completados
        completed_targets = self.profit_target_manager.target_history
        if completed_targets:
            logger.info(f"\n🎯 TARGETS ALCANZADOS: {len(completed_targets)}")
            for i, target_info in enumerate(completed_targets, 1):
                logger.info(f"   #{i}: ${target_info['target']:.2f} @ {target_info['timestamp'].strftime('%H:%M:%S')}")
        
        # Target actual
        current_target = self.profit_target_manager.get_current_target()
        logger.info(f"\n🎯 PRÓXIMO TARGET: ${current_target:.2f}")
        
        # Estadísticas de operaciones
        if self.completed_trades:
            total = len(self.completed_trades)
            wins = sum(1 for t in self.completed_trades if t['result'] > 0)
            winrate = (wins / total * 100)
            total_pnl = sum(t['result'] for t in self.completed_trades)
            
            logger.info(f"\n📊 ESTADÍSTICAS DE OPERACIONES:")
            logger.info(f"   Total: {total}")
            logger.info(f"   Ganadoras: {wins} ({winrate:.1f}%)")
            logger.info(f"   Perdedoras: {total - wins}")
            logger.info(f"   PnL: ${total_pnl:.2f}")
        
        logger.info("=" * 60)
        logger.info("✅ Bot finalizado correctamente")


# ==================== PUNTO DE ENTRADA ====================

def main():
    """Función principal"""
    # Credenciales (reemplaza con las tuyas)
    API_KEY = "j65vqKTAEvJtOZMCQbSiH5GZXfzyg1W70dWvhnb5DHxMOlLaW1JlrohJtYf8hJMH"
    API_SECRET = "qBqVSu0b0stLoN5hWEo5TAeK0IyfI4bNP1kQh7X3JoXVlzBOVutMSr0CWtvTua0O"
    
    # Crear bot en modo simulación
    bot = HeikinAshiTradingBot(
        api_key=API_KEY, 
        api_secret=API_SECRET, 
        testnet=False, 
        simulate=False  # Cambiar a False para trading real
    )
    
    try:
        bot.run()
    except Exception as e:
        logger.error(f"Error ejecutando bot: {e}")
    finally:
        bot.cleanup()

if __name__ == "__main__":
    main()
