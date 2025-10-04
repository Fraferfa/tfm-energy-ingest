# Guía de Uso: API PVPC ESIOS para Agentes IA

## Resumen Ejecutivo
Esta guía explica cómo obtener correctamente los precios PVPC (Precio Voluntario para el Pequeño Consumidor) de todas las zonas españolas usando la API de ESIOS, solucionando problemas comunes de datos incompletos.

## Problema Principal Identificado
**La API de ESIOS puede devolver datos incompletos (menos de 24 horas) si no se configuran correctamente los parámetros de fecha y timezone.**

### Síntomas del problema:
- Solo se obtienen 21 horas en lugar de 24
- Faltan las horas 00:00, 01:00, 02:00
- Los datos empiezan desde las 03:00

### Causa raíz:
Conversión incorrecta de timezone que corta las primeras horas del día al convertir de Madrid (CEST +02:00) a UTC.

## Solución Técnica

### Método INCORRECTO (que causa pérdida de datos):
```python
# ❌ EVITAR: Conversión directa a UTC
start = dt.datetime(year, month, day, 0, 0, tzinfo=madrid_tz).astimezone(dt.timezone.utc)
end = dt.datetime(year, month, day, 23, 59, tzinfo=madrid_tz).astimezone(dt.timezone.utc)
```

### Método CORRECTO (garantiza 24 horas completas):
```python
# ✅ USAR: Rango extendido en UTC con filtrado posterior
start_day_utc = dt.datetime.combine(target_date, dt.time(0, 0), tzinfo=dt.timezone.utc) - dt.timedelta(hours=3)
end_day_utc = dt.datetime.combine(target_date + dt.timedelta(days=1), dt.time(0, 0), tzinfo=dt.timezone.utc) + dt.timedelta(hours=1)

# Luego filtrar por fecha objetivo:
df_target_day = df_raw[df_raw['datetime'].dt.date == target_date]
```

## Configuración de la API

### Endpoint y autenticación:
```python
API_PVPC = "https://api.esios.ree.es/indicators/1001"
HEADERS = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "x-api-key": "tu_token_aqui"
}
```

### Parámetros críticos:
```python
params = {
    "start_date": start_day_utc.isoformat().replace("+00:00", "Z"),
    "end_date": end_day_utc.isoformat().replace("+00:00", "Z"),
    "time_trunc": "hour",
    "locale": "es",
    # NO incluir geo_ids[] para obtener todas las zonas
}
```

## Interpretación de la Respuesta

### Estructura de datos:
- **105 valores típicos** = ~24 horas × 5 zonas geográficas
- **Zonas incluidas**: Península, Baleares, Canarias, Ceuta, Melilla
- **Formato de fecha**: ISO 8601 con timezone Madrid (+02:00)

### Procesamiento recomendado:
```python
# 1. Convertir a DataFrame
df_raw = pd.DataFrame(data['indicator']['values'])
df_raw['datetime'] = pd.to_datetime(df_raw['datetime'])

# 2. Pivotar para tener zonas como columnas
df_pivot = df_raw.pivot(index='datetime', columns='geo_name', values='value')

# 3. Verificar completitud (debe ser 24 filas)
assert len(df_pivot) == 24, f"Datos incompletos: solo {len(df_pivot)} horas"
```

## Horarios de Publicación

### Cronograma normal de ESIOS:
- **14:00h**: Publicación inicial (puede estar incompleta)
- **20:15h**: Publicación completa de 24 horas
- **Retrasos posibles**: Hasta 21:00h en días complejos

### Estrategia de consulta:
1. **Consulta después de las 20:15h** para datos completos
2. **Implementar retry logic** si faltan horas
3. **Verificar siempre** que se obtienen exactamente 24 horas

## Casos Especiales

### Precios idénticos entre zonas:
- **Normal**: Cuando no hay congestión en la red
- **Anormal**: Si persiste durante varios días consecutivos
- **Verificación**: Comprobar en la web de ESIOS si es correcto

### Zonas con precios diferentes:
- **Península**: Base del mercado ibérico
- **Islas**: Pueden tener sobrecostes de transporte
- **Ceuta/Melilla**: Sistemas particulares
- **Diferencias típicas**: 0-50 €/MWh en días de congestión

## Mejores Prácticas para Agentes IA

### 1. Validación automática:
```python
def validate_pvpc_data(df):
    zones = ['Península', 'Baleares', 'Canarias', 'Ceuta', 'Melilla']
    
    # Verificar 24 horas
    if len(df) != 24:
        raise ValueError(f"Datos incompletos: {len(df)} horas en lugar de 24")
    
    # Verificar todas las zonas
    missing_zones = [z for z in zones if z not in df.columns]
    if missing_zones:
        raise ValueError(f"Zonas faltantes: {missing_zones}")
    
    # Verificar rango horario completo (0-23)
    hours = sorted(df.index.hour.unique())
    if hours != list(range(24)):
        missing_hours = [h for h in range(24) if h not in hours]
        raise ValueError(f"Horas faltantes: {missing_hours}")
```

### 2. Manejo de errores:
- **Timeout**: 30 segundos mínimo
- **Rate limiting**: Máximo 1 consulta por segundo
- **Retry logic**: 3 intentos con backoff exponencial
- **Logging**: Registrar todas las consultas y errores

### 3. Optimización:
- **Cache**: Guardar datos del día para evitar consultas repetidas
- **Batch processing**: Procesar múltiples días en una sola consulta
- **Filtrado inteligente**: Solo zona específica si no necesitas todas

## Código de Producción Recomendado

Usar la función `get_pvpc_complete_day()` del artifact anterior que implementa todas estas mejores prácticas:

```python
# Obtener datos de mañana
df_pvpc = get_pvpc_complete_day()

# Obtener datos de fecha específica  
df_pvpc = get_pvpc_complete_day(dt.date(2025, 9, 21))

# Verificación automática incluida
# Manejo de errores integrado  
# Logging informativo
```

## Indicadores de Calidad

### ✅ Datos correctos:
- 24 filas (horas 00:00 - 23:00)
- 5 columnas de zonas + hora
- Precios en rango 20-300 €/MWh típicamente
- Sin valores nulos

### ❌ Datos incorrectos:
- Menos de 24 filas
- Zonas faltantes
- Precios negativos o > 500 €/MWh
- Valores nulos o duplicados

## Resolución de Problemas Comunes

| Problema | Causa | Solución |
|----------|-------|----------|
| Solo 21 horas | Timezone incorrecto | Usar rango extendido UTC |
| Todas las zonas iguales | Normal sin congestión | Verificar en web ESIOS |
| Error 401 | Token inválido | Renovar token API |
| Error 429 | Rate limit | Implementar delay |
| Timeout | Hora punta API | Reintentar más tarde |

## Recursos Adicionales

- **Web ESIOS**: https://www.esios.ree.es/es/pvpc
- **Documentación API**: https://api.esios.ree.es/
- **Indicador PVPC**: 1001 (estándar) o 1014 (con peajes)
- **Soporte técnico**: A través del portal web de ESIOS