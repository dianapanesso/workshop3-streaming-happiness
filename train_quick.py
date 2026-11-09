import os
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
from sklearn.ensemble import RandomForestRegressor
import joblib

# 1. Cargar el dataset procesado
df = pd.read_csv('data/processed/unified_happiness.csv')

# 2. Definir características y target
features = ['gdp', 'family', 'health', 'freedom', 'generosity', 'corruption']
X = df[features]
y = df['actual_happiness_score']

# 3. Dividir datos y entrenar el pipeline
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)

pipeline = Pipeline([
    ('scaler', StandardScaler()),
    ('model', RandomForestRegressor(n_estimators=100, random_state=42))
])

pipeline.fit(X_train, y_train)

# 4. Asegurar que la carpeta models exista y guardar el archivo
os.makedirs('models', exist_ok=True)
joblib.dump(pipeline, 'models/model.pkl')

print("¡Archivo model.pkl generado exitosamente en la carpeta models/!")