-- Consulta 1: Total de viajes e ingresos por tipo de servicio
SELECT
    service_type,
    COUNT(*) AS total_trips,
    SUM(total_amount) AS total_revenue
FROM gold_trips_clean
GROUP BY service_type
ORDER BY total_revenue DESC;

-- Consulta 2: Métricas de calidad por servicio, año y mes
SELECT
    service_type,
    year,
    month,
    total_records,
    valid_records,
    rejected_records,
    quality_percentage
FROM quality_metrics_summary
ORDER BY year, month, service_type;

-- Consulta 3: Top 20 rutas (origen-destino) por ingresos
SELECT
    pickup_location_id,
    dropoff_location_id,
    COUNT(*) AS total_trips,
    SUM(total_amount) AS total_revenue,
    AVG(trip_duration_minutes) AS avg_duration
FROM gold_trips_clean
GROUP BY pickup_location_id, dropoff_location_id
ORDER BY total_revenue DESC
LIMIT 20;
