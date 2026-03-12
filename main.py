"""
Punto de entrada principal — las credenciales se leen de variables de entorno.
En Railway: Settings > Variables > Add Variable
  BINANCE_API_KEY    = tu_api_key
  BINANCE_API_SECRET = tu_api_secret
"""
import os
import logging
from DeepDEFINITIVIVO_1_FINAL_CON_EMAS_Y_BOLLINGER_TRES import HeikinAshiTradingBot

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    API_KEY    = os.environ.get("BINANCE_API_KEY")
    API_SECRET = os.environ.get("BINANCE_API_SECRET")

    if not API_KEY or not API_SECRET:
        logger.error("❌ Faltan variables de entorno BINANCE_API_KEY y/o BINANCE_API_SECRET")
        raise SystemExit(1)

    bot = HeikinAshiTradingBot(
        api_key=API_KEY,
        api_secret=API_SECRET,
        testnet=False,
        simulate=False
    )

    try:
        bot.run()
    except Exception as e:
        logger.error(f"Error ejecutando bot: {e}")
    finally:
        bot.cleanup()

if __name__ == "__main__":
    main()
