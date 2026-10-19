# NYC 311: analítica distribuida, ML y streaming

Proyecto integrador de Análisis de Big Data del Magíster en Data Science UDD. Desarrolla una solución analítica de extremo a extremo sobre solicitudes públicas NYC 311: exploración y benchmark distribuido, clasificación de riesgo de atención prolongada, tracking con MLflow, una demostración de Kafka/Structured Streaming y evaluación de gobernanza, equidad y costos.

## Resultados y límites

- Snapshot 2021–2024: 13.072.334 solicitudes únicas.
- Modelo batch: 9.189.270 solicitudes etiquetables de entrenamiento y 3.365.506 de prueba; área PR 0,4092 y recall 0,8449. El umbral genera alertas para 79,27% de la prueba, por lo que el resultado se plantea para revisión humana y no para priorización automática.
- Streaming: replay local acotado que verifica ventanas, watermark, atrasos y recuperación; no demuestra escalabilidad cloud ni capacidad máxima.
- La etiqueta indica demora relativa al percentil 75 histórico por tipo de reclamo; no es un SLA oficial ni una inferencia causal.

Los informes de las tres fases están en sus carpetas. Los notebooks conservan código y narrativa, pero se publican sin salidas ejecutadas para evitar rutas locales y muestras de registros. El snapshot NYC 311, capas derivadas, modelos, tracking, logs, archivos de defensa y materiales docentes se excluyen. La reproducción requiere descargar el snapshot y revisar sus condiciones de uso; los resultados pueden variar entre entornos.

## Estructura

- `fase-1/`: EDA, consultas y benchmark de motores.
- `fase-2/`: pipeline de ML distribuido, experimentación y evaluación temporal.
- `fase-3/`: streaming, gobernanza, FinOps, equidad y síntesis final.
- `data/raw/README.md`: instrucciones para obtener los datos por separado.

## Reproducción

Comienza por cada README de fase y sus `requirements.lock.txt`. La ejecución completa de fase 2 requiere recursos locales considerables; el notebook de fase 3 presenta evidencias históricas y una demo de streaming acotada. No se incluyen modelos ni artefactos de ejecución. Para datos, consulta `fase-3/FUENTES.md` y las notas de `data/raw/README.md`.

## Curso

Los laboratorios del curso están en [analisis-big-data](https://github.com/danteaguirreb/analisis-big-data).
