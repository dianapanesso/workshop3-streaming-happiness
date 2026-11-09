import json
import joblib
import pandas as pd
import psycopg2
from kafka import KafkaConsumer
from psycopg2.extras import Json
from datetime import datetime

# Conexión a la base de datos PostgreSQL
def get_db_connection():
    return psycopg2.connect(
        dbname="happiness_db", user="admin", password="adminpassword", host="localhost", port="5432"
    )

def init_consumer():
    # 1. Cargar el modelo serializado
    pipeline = joblib.load('models/model.pkl')
    
    # 2. Configurar el Consumidor de Kafka usando el grupo requerido
    consumer = KafkaConsumer(
        'happiness-events',
        bootstrap_servers=['localhost:9092'],
        group_id='happiness-ml-team',
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest' # Para leer desde el principio si es la primera vez
    )
    
    print("Consumidor iniciado. Esperando eventos...")
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    for message in consumer:
        event = message.value
        event_id = event.get('event_id', 'UNKNOWN')
        status = 'VALID'
        
        # 3. Validación del contrato del evento
        required_fields = ['event_id', 'country', 'year', 'gdp', 'family', 'health', 'freedom', 'generosity', 'corruption', 'actual_happiness_score', 'event_time']
        
        if not all(field in event for field in required_fields):
            status = 'INVALID_SCHEMA'
        
        # 4. Almacenamiento en Raw Table antes de la predicción
        cursor.execute(
            """INSERT INTO raw_happiness_events (raw_event_id, event_payload, processing_status) 
               VALUES (%s, %s, %s) ON CONFLICT (raw_event_id) DO NOTHING""",
            (event_id, json.dumps(event), status)
        )
        conn.commit()
        
        # 5. Si el registro es inválido, se salta la predicción para no crashear el pipeline
        if status != 'VALID':
            print(f"Evento {event_id} omitido por estado: {status}")
            continue
            
        try:
            # Insertar en dimensiones (ignora si ya existen)
            cursor.execute("INSERT INTO dim_country (country_name) VALUES (%s) ON CONFLICT (country_name) DO NOTHING RETURNING country_id", (event['country'],))
            cursor.execute("SELECT country_id FROM dim_country WHERE country_name = %s", (event['country'],))
            country_id = cursor.fetchone()[0]

            cursor.execute("INSERT INTO dim_date (year) VALUES (%s) ON CONFLICT (year) DO NOTHING RETURNING date_id", (event['year'],))
            cursor.execute("SELECT date_id FROM dim_date WHERE year = %s", (event['year'],))
            date_id = cursor.fetchone()[0]

            # 6. Preparar características y generar predicción
            features_df = pd.DataFrame([{
                'gdp': event['gdp'], 'family': event['family'], 'health': event['health'],
                'freedom': event['freedom'], 'generosity': event['generosity'], 'corruption': event['corruption']
            }])
            
            predicted_score = pipeline.predict(features_df)[0]
            actual_score = event['actual_happiness_score']
            prediction_error = abs(predicted_score - actual_score)
            
            # 7. Almacenar predicción enlazada al evento raw
            cursor.execute(
                """INSERT INTO fact_predictions 
                   (raw_event_id, country_id, date_id, actual_score, predicted_score, prediction_error, event_time)
                   VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                (event_id, country_id, date_id, actual_score, float(predicted_score), float(prediction_error), event['event_time'])
            )
            conn.commit()
            print(f"Predicción generada para {event['country']} - Real: {actual_score}, Predicción: {predicted_score:.2f}, Error: {prediction_error:.2f}")
            
        except Exception as e:
            conn.rollback()
            # Si hay error en la predicción, actualizar estado en tabla cruda
            cursor.execute("UPDATE raw_happiness_events SET processing_status = 'PREDICTION_ERROR' WHERE raw_event_id = %s", (event_id,))
            conn.commit()
            print(f"Error procesando evento {event_id}: {e}")

if __name__ == "__main__":
    init_consumer()