### 📋 Decisiones de Diseño y Criterios de Negocio

#### 1. Detección de Outliers y Criterio de Calidad de Montos
El dataset presenta montos transaccionales extremos que van desde $0,002 hasta $1,47 Billones de pesos (ARS). En el mercado financiero argentino (BYMA), operaciones de miles de millones de pesos son **legítimas** cuando corresponden a inversores institucionales, fondos comunes de inversión (FCI) o mesas de dinero corporativas operando bloques mayoristas (SENEBI). 

Sin embargo, montos de $0,002 ARS carecen de sentido económico (una fracción de centavo de peso no existe de forma transaccional), y montos superiores a $1,50 Billones ARS exceden los volúmenes diarios históricos promedio de todo el mercado para un solo activo.

* **Operación Legítima Extrema:** Montos totales entre $1,00 ARS y $1,47 Billones ARS. Se procesan normalmente para no perder visibilidad del volumen real del negocio.
* **Monto Anómalo / Outlier:** Todo registro donde el `monto_total` (Precio × Cantidad) sea **menor a $1,00 ARS** o **mayor a $1,50 Billones ARS**. Estos registros no se descartan de forma silenciosa; se marcan con un flag analítico y se reportan de manera idempotente en la tabla de auditoría `bronze_byma.calidad_log` para alertas operativas, pero continúan su flujo para asegurar la trazabilidad absoluta exigida por el negocio.

#### 2. Estrategia de Mapeo de Tickers Complejos (D/C)
En el mercado local, los sufijos **D** (Dólar MEP) y **C** (Dólar Cable/CCL) representan el mismo activo subyacente pero liquidado en otra moneda o plaza. Llamar a APIs internacionales con tickers como `AL30D` o `MSFTC` de forma directa fallará en producción, ya que las plataformas externas consolidan la serie histórica sobre su ticker base de referencia.

* **Criterio de Ingeniería:** Se implementó una lógica de unificación en la capa **Silver** mediante expresiones regulares (`D$` y `C$`) para aislar el *ticker base* (`AL30`, `MSFT`) de su moneda de liquidación. Las dimensiones consolidan los atributos bajo el símbolo unificado, permitiendo al equipo de Analytics realizar agrupaciones y métricas cruzadas sin necesidad de realizar costosos joins intermedios.

#### 3. Estrategia de Ingesta ante Restricciones de Infraestructura (Air-Gapped Contingency)
El enunciado explicita evaluar el diseño ante la fragilidad intrínseca de conectar APIs externas. Al ejecutar el pipeline bajo un entorno interactivo Serverless gratuito en **Databricks Community Edition**, el cluster presenta un aislamiento de red por política de la plataforma (`Name or service not known`) que impide la resolución de DNS hacia el exterior.

* **Diseño Defensivo Implementado:** Para mitigar esta fragilidad sin detener el pipeline, se diseñó un mecanismo de **Degradación Elegante Distribuido**. Se encapsuló la llamada HTTP dentro de un **UDF (User Defined Function)** de Spark que ejecuta en paralelo los 3 reintentos con **backoff exponencial** exigidos. Al capturar de forma controlada la falla de infraestructura de red, el pipeline activa automáticamente una capa de contingencia desconectada (*Air-Gapped*).
* **Anclaje de Datos Locales:** El sistema realiza una búsqueda dinámica mediante funciones de ventana (`ROW_NUMBER()`) sobre la capa **Bronze** para extraer el **último precio real operado y legítimo** de cada ticker dentro del dataset del negocio. Ese valor actúa como ancla de mercado para los días hábiles bursátiles, garantizando la trazabilidad histórica real.

#### 4. Estrategia de Imputación para Fines de Semana y Feriados (*Last-Value Forward Fill*)
El dataset contiene 12.498 operaciones ejecutadas en días inhábiles (sábados, domingos o feriados bursátiles de BYMA 2026), fechas para las cuales no existen cotizaciones oficiales de cierre en ningún mercado. Dejar estos campos en `NULL` destruiría la capacidad de los reportes analíticos para calcular desvíos de precios o flags de desvíos en tiempo real.

* **Criterio de Ingeniería:** Se implementó el algoritmo de **Last-Value Forward Fill** nativo en Spark SQL sobre el catálogo de tablas Delta. El pipeline genera la matriz calendario completa cruzando instrumentos y fechas, y utiliza la función analítica **`LAST(precio_base, true) OVER (...)`** para arrastrar de forma cronológica el precio oficial del último día hábil anterior (Viernes) hacia el fin de semana correspondiente. 
* **Optimización del MERGE:** Para asegurar la idempotencia y cuidar los recursos del cluster, el impacto definitivo en la tabla histórica de Silver utiliza un `MERGE` condicionado estrictamente por la cláusula `WHERE target.precio_cierre IS NULL`. Esto optimiza el plan de ejecución de Spark (Catalyst), ordenando en memoria únicamente el set de datos afectado y omitiendo la reescritura de los días de semana.

#### 5 Estrategia de Observabilidad y Umbrales de Calidad (Data Quality)
Un pipeline corporativo real requiere monitoreo proactivo para detectar desvíos antes de que afecten a los reportes analíticos. Para la capa Gold, se definen tres controles de calidad automatizados (Data Quality Gateways):
* **Integridad Referencial Estricta (100%)**: Cada operación asentada en fact_operaciones debe tener un destino válido en las dimensiones (dim_cliente, dim_instrumento). Un valor huérfano sesgaría los informes ejecutivos.
* **Completitud de Desvíos (Mínimo X = 95%)**: El umbral se fija en 95% para días hábiles. Se tolera un 5% de margen debido a la existencia de activos extremadamente ilíquidos o de reciente emisión en BYMA que carecen de registros históricos en la plaza bursátil. Si la falta de datos supera el 5%, denota una falla crítica de la base transaccional o el fallback analítico.
* **Check de Consistencia del Negocio (Monto Total Coherente)**: Se implementa una validación cruzada donde el desvío entre el monto_total registrado y el cálculo derivado analítico (cantidad * precio_operado) debe ser estrictamente igual a $0.00. Un desvío superior delata corrupción de datos en el motor transaccional del core bancario o un error de casteo en el pipeline.
