
import asyncio
import websockets
import json
import threading
import time
import numpy as np
from collections import defaultdict
from datetime import datetime
import os
import time
from datetime import datetime

class SymbolWebSocketPriceCache:
    """WebSocket optimizado para múltiples símbolos con conexiones agrupadas"""
    
    def __init__(self, symbols, symbols_per_connection=10):
        self.symbols = [s.upper() for s in symbols]
        self.symbols_per_connection = symbols_per_connection
        self.price_cache = {}  # symbol -> price
        self.last_update = {}  # symbol -> timestamp
        self.tasks = []
        self.lock = threading.Lock()
        self.running = False
        self._loop = None
        self.connection_stats = defaultdict(lambda: {"reconnects": 0, "last_error": None})
        
    def _create_symbol_groups(self):
        """Agrupa símbolos para conexiones multiplexadas"""
        groups = []
        for i in range(0, len(self.symbols), self.symbols_per_connection):
            groups.append(self.symbols[i:i + self.symbols_per_connection])
        return groups
    
    async def _ws_combined_stream(self, symbols_group):
        """Maneja un stream combinado para múltiples símbolos"""
        # Construir URL para stream combinado
        streams = [f"{s.lower()}@markPrice@1s" for s in symbols_group]
        url = f"wss://fstream.binance.com/stream?streams={'/'.join(streams)}"
        
        group_id = f"group_{symbols_group[0][:3]}_{len(symbols_group)}"
        reconnect_delay = 1
        consecutive_errors = 0
        
        while self.running:
            try:
                # Configuración optimizada para conexiones estables
                async with websockets.connect(
                    url,
                    ping_interval=30,  # Ping cada 30s
                    ping_timeout=10,    # Timeout de ping 10s
                    close_timeout=10,
                    max_size=10**7,    # Mensajes más grandes
                    max_queue=2000,     # Cola más grande
                    compression=None    # Sin compresión para menor latencia
                ) as ws:
                    print(f"✅ Stream combinado conectado: {group_id} ({len(symbols_group)} símbolos)")
                    reconnect_delay = 1
                    consecutive_errors = 0
                    last_ping = time.time()
                    
                    while self.running:
                        try:
                            # Timeout más largo para streams combinados
                            msg = await asyncio.wait_for(ws.recv(), timeout=45)
                            
                            # Procesar mensaje
                            data = json.loads(msg)
                            
                            # Los streams combinados vienen envueltos en un objeto con 'stream' y 'data'
                            if 'data' in data:
                                price_data = data['data']
                                symbol = price_data.get('s', '').upper()
                                price = float(price_data.get('p', 0.0))
                                
                                if symbol and np.isfinite(price) and price > 0:
                                    with self.lock:
                                        self.price_cache[symbol] = price
                                        self.last_update[symbol] = time.time()
                            
                            # Ping manual periódico
                            if time.time() - last_ping > 30:
                                await ws.ping()
                                last_ping = time.time()
                                
                        except asyncio.TimeoutError:
                            # Si no hay datos en 45s, hacer ping
                            print(f"⏰ Timeout en {group_id}, enviando ping...")
                            await ws.ping()
                            last_ping = time.time()
                            
                        except websockets.ConnectionClosed as e:
                            print(f"🔶 Conexión cerrada para {group_id}: {e}")
                            raise
                            
            except Exception as e:
                consecutive_errors += 1
                self.connection_stats[group_id]["reconnects"] += 1
                self.connection_stats[group_id]["last_error"] = str(e)
                
                # Backoff exponencial con límite
                reconnect_delay = min(reconnect_delay * 1.5, 30)
                
                # Si hay muchos errores consecutivos, esperar más
                if consecutive_errors > 5:
                    reconnect_delay = 60
                    
                print(f"🔴 Error en {group_id}: {e}")
                print(f"   Reconectando en {reconnect_delay:.1f}s (intento #{consecutive_errors})")
                
                await asyncio.sleep(reconnect_delay)
    
    async def _ws_single_symbol(self, symbol):
        """Fallback para símbolos individuales si el stream combinado falla"""
        url = f"wss://fstream.binance.com/ws/{symbol.lower()}@markPrice"
        reconnect_delay = 1
        
        while self.running:
            try:
                async with websockets.connect(
                    url,
                    ping_interval=30,
                    ping_timeout=10,
                    close_timeout=10
                ) as ws:
                    print(f"🟢 WS individual para {symbol}")
                    reconnect_delay = 1
                    
                    while self.running:
                        try:
                            msg = await asyncio.wait_for(ws.recv(), timeout=45)
                            data = json.loads(msg)
                            price = float(data.get('p', 0.0))
                            
                            if np.isfinite(price) and price > 0:
                                with self.lock:
                                    self.price_cache[symbol] = price
                                    self.last_update[symbol] = time.time()
                                    
                        except asyncio.TimeoutError:
                            await ws.ping()
                            
            except Exception as e:
                print(f"🔴 Error WS individual {symbol}: {e}")
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, 60)
    
    async def _monitor_health(self):
        """Monitorea la salud de las conexiones"""
        while self.running:
            await asyncio.sleep(60)  # Check cada minuto
            
            current_time = time.time()
            stale_symbols = []
            
            with self.lock:
                for symbol in self.symbols:
                    last_update = self.last_update.get(symbol, 0)
                    if current_time - last_update > 120:  # Sin updates en 2 minutos
                        stale_symbols.append(symbol)
            
            if stale_symbols:
                print(f"⚠️ Símbolos sin actualización: {stale_symbols}")
                # Aquí podrías implementar reconexión específica para esos símbolos
    
    def start(self):
        """Inicia las conexiones WebSocket"""
        self.running = True
        
        # Crear y arrancar loop asyncio en thread separado
        loop = asyncio.new_event_loop()
        self._loop = loop
        
        def run_loop():
            asyncio.set_event_loop(loop)
            loop.run_forever()
        
        thread = threading.Thread(target=run_loop, daemon=True)
        thread.start()
        
        # Crear grupos de símbolos para streams combinados
        symbol_groups = self._create_symbol_groups()
        
        print(f"📊 Iniciando {len(symbol_groups)} streams combinados para {len(self.symbols)} símbolos")
        
        # Iniciar streams combinados
        for group in symbol_groups:
            task = asyncio.run_coroutine_threadsafe(
                self._ws_combined_stream(group), 
                loop
            )
            self.tasks.append(task)
        
        # Iniciar monitor de salud
        monitor_task = asyncio.run_coroutine_threadsafe(
            self._monitor_health(), 
            loop
        )
        self.tasks.append(monitor_task)
        
        print("✅ WebSocket cache iniciado")
    
    def stop(self):
        """Detiene todas las conexiones"""
        print("🛑 Deteniendo WebSocket cache...")
        self.running = False
        
        # Dar tiempo a las conexiones para cerrarse limpiamente
        time.sleep(2)
        
        # Cancelar todas las tareas
        for task in self.tasks:
            try:
                task.cancel()
            except Exception:
                pass
        
        # Detener el loop
        if self._loop:
            self._loop.call_soon_threadsafe(self._loop.stop)
        
        print("✅ WebSocket cache detenido")
    
    def get_price(self, symbol):
        """Obtiene el precio actual de un símbolo"""
        symbol = symbol.upper()
        with self.lock:
            return self.price_cache.get(symbol)
    
    def get_all_prices(self):
        """Obtiene todos los precios actuales"""
        with self.lock:
            return self.price_cache.copy()
    
    def get_stale_symbols(self, max_age_seconds=60):
        """Obtiene símbolos con datos obsoletos"""
        current_time = time.time()
        stale = []

        for symbol in self.symbols:
            last_update = self.last_update.get(symbol, 0)
            if current_time - last_update > max_age_seconds:
                stale.append(symbol)
        
       
        
        return stale
    
    def get_stats(self):
        """Obtiene estadísticas de las conexiones"""
        with self.lock:
            active_symbols = len(self.price_cache)
            total_symbols = len(self.symbols)
            stale_count = len(self.get_stale_symbols())
            
        return {
            "total_symbols": total_symbols,
            "active_symbols": active_symbols,
            "stale_symbols": stale_count,
            "connection_stats": dict(self.connection_stats)
        }





# Ejemplo de uso
if __name__ == "__main__":
    # Lista de 40 símbolos de ejemplo
    symbols = [
        'BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'ADAUSDT', 'DOGEUSDT',
        'XRPUSDT', 'DOTUSDT', 'UNIUSDT', 'LINKUSDT', 'LTCUSDT',
        'SOLUSDT', 'MATICUSDT', 'AVAXUSDT', 'ATOMUSDT', 'FILUSDT',
        'VETUSDT', 'TRXUSDT', 'ETCUSDT', 'XLMUSDT', 'THETAUSDT',
        'AAVEUSDT', 'ALGOUSDT', 'ICPUSDT', 'SHIBUSDT', 'NEARUSDT',
        'LUNAUSDT', 'AXSUSDT', 'SANDUSDT', 'MANAUSDT', 'GALAUSDT',
        'APEUSDT', 'GMTUSDT', 'OPUSDT', 'ARBUSDT', 'APTUSDT',
        'LDOUSDT', 'STXUSDT', 'IMXUSDT', 'INJUSDT', 'SUIUSDT'
    ]
    
    # Crear e iniciar cache
    cache = SymbolWebSocketPriceCache(symbols, symbols_per_connection=40)
    cache.start()
    
    try:
        # Monitorear precios
        while True:
        
        
            time.sleep(1)  # refresco cada 2s

            # Limpia consola (Windows = cls, Linux/Mac = clear)
            os.system("cls" if os.name == "nt" else "clear")

            print("="*50)
            print(f"📊 Precios actuales ({datetime.now().strftime('%H:%M:%S')})")
            print("-"*50)

            # Mostrar siempre los primeros 3 símbolos
            for symbol in symbols[:3]:
                price = cache.get_price(symbol)
                if price is not None:
                    print(f"{symbol}: ${price:.4f}")
                else:
                    print(f"{symbol}: (sin datos aún)")
            # Mostrar estadísticas
            stats = cache.get_stats()
            print(f"\n📈 Estadísticas:")
            print(f"   Activos: {stats['active_symbols']}/{stats['total_symbols']}")
            print(f"   Obsoletos: {stats['stale_symbols']}")
            
            # Verificar símbolos sin actualización
            stale = cache.get_stale_symbols(max_age_seconds=1)
            if stale:
                print(f"   ⚠️ Sin actualización (>30s): {stale[:5]}")
                
    except KeyboardInterrupt:
        print("\n🛑 Deteniendo...")
        cache.stop()
        print("✅ Finalizado")