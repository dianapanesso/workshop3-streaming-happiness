import pandas as pd
import json
import time
import uuid
from datetime import datetime
from kafka import KafkaProducer

# 1. Configurar el Productor de Kafka
# Se conecta al broker local y serializa los datos automáticamente a formato JSON
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: k.encode('utf-8')
)

topic_name = 'happiness-events'

def stream_data():
    print(f"Iniciando el streaming de datos hacia el tópico: {topic_name}...")
    
    # 2. Leer los datos históricos procesados
    df = pd.read_csv('data/processed/unified_happiness.csv')
    
    # 3. Iterar registro por registro simulando streaming
    for index, row in df.iterrows():
        # Crear la estructura JSON requerida por el contrato del evento
        event_data = {
            "event_id": f"E{uuid.uuid4().hex[:8].upper()}", # Identificador único
            "country": row['country'],
            "year": int(row['year']),
            "gdp": float(row['gdp']),
            "family": float(row['family']),
            "health": float(row['health']),
            "freedom": float(row['freedom']),
            "generosity": float(row['generosity']),
            "corruption": float(row['corruption']),
            "actual_happiness_score": float(row['actual_happiness_score']),
            "event_time": datetime.now().isoformat() # Timestamp ISO-8601
        }
        
        # 4. Enviar el evento a Kafka usando el país como llave (key)
        country_key = event_data['country']
        producer.send(topic_name, key=country_key, value=event_data)
        
        print(f"Enviado: {country_key} ({event_data['year']}) - ID: {event_data['event_id']}")
        
        # Simular el retraso en la llegada de los datos (1 segundo)
        time.sleep(1)

if __name__ == "__main__":
    try:
        stream_data()
    except KeyboardInterrupt:
        print("\nStreaming detenido manualmente.")
    finally:
        producer.close()
        print("Productor cerrado correctamente.")