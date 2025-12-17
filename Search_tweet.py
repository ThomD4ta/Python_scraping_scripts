import os
import tweepy
import re
import unicodedata
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime

# --------------------------------------------------
# 1. Cargar variables de entorno
# --------------------------------------------------
load_dotenv()

BEARER_TOKEN = os.getenv("X_BEARER_TOKEN")
ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
ACCESS_TOKEN_SECRET = os.getenv("ACCESS_TOKEN_SECRET")

# Validación básica
if not BEARER_TOKEN:
    raise ValueError("❌ Bearer Token no encontrado")

if len(BEARER_TOKEN) < 100:
    raise ValueError("❌ Bearer Token inválido o incompleto")

# --------------------------------------------------
# 2. Función de normalización de texto
# --------------------------------------------------
def normalize_text(text: str) -> str:
    if not text:
        return ""

    # 1. Pasar a minúsculas
    text = text.lower()

    # 2. Eliminar acentos (á → a, ñ → n, etc.)
    text = unicodedata.normalize("NFKD", text) \
        .encode("ascii", "ignore") \
        .decode("utf-8")

    # 3. Eliminar URLs (Opcional)
    # text = re.sub(r"http\S+", "", text)

    # 4. Eliminar caracteres especiales (emojis, símbolos)
    text = re.sub(r"[^a-z0-9\s]", "", text)

    # 5. Eliminar espacios duplicados
    text = " ".join(text.split())

    return text

# --------------------------------------------------
# 3. Crear cliente de X API v2
# --------------------------------------------------
client = tweepy.Client(
    bearer_token=BEARER_TOKEN,
    wait_on_rate_limit=True
)

# --------------------------------------------------
# 4. Definir el tópico a buscar
# --------------------------------------------------
QUERY_NAME = "Data Engineering Jobs"

QUERY_STRING = (
    "(#dataengineering OR \"data engineer\") "
    "-is:retweet"
)

MAX_RESULTS = 10  # Free Tier: minimo recomendado

# --------------------------------------------------
# 5. Llamar al endpoint /2/tweets/search/recent (CON MANEJO DE ERRORES)
# --------------------------------------------------
try:
    response = client.search_recent_tweets(
        query=QUERY_STRING,
        max_results=MAX_RESULTS,
        tweet_fields=["created_at", "author_id", "lang"]
)
except tweepy.TooManyRequests:
    print("⏳ Límite alcanzado. Espera 15 minutos.")
    exit()
except tweepy.TweepyException as e:
    print("❌ Error de Tweepy:", e)
    exit()
except Exception as e:
    print("❌ Error inesperado:", e)
    exit()

# --------------------------------------------------
# 6. Procesar la respuesta (Con Normalizacion)
# --------------------------------------------------
tweets_data = []

if response.data:
    for tweet in response.data:
        tweets_data.append({
            "query_name": QUERY_NAME,
            "query_string": QUERY_STRING,
            "tweet_id": str(tweet.id),
            "author_id": str(tweet.author_id),
            "created_at": tweet.created_at,
            "text_raw": tweet.text,
            "text_normalized": normalize_text(tweet.text)
        })
else:
    print("⚠️ No se encontraron tweets para este query.")

# --------------------------------------------------
# 7. Convertir a DataFrame
# --------------------------------------------------
tweets_df = pd.DataFrame(tweets_data)

# --------------------------------------------------
# 8. Guardar resultados
# --------------------------------------------------
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
file_name = f"tweets_Xscripts_results_{timestamp}.csv"

tweets_df.to_csv(file_name, index=False)

print(f"✅ Tweets guardados correctamente en {file_name}")
print(f"📊 Total tweets recolectados: {len(tweets_df)}")
