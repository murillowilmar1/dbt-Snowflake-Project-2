# 📊 dbt_saas_demo

Proyecto completo de modelado analítico para un negocio SaaS utilizando **dbt + Snowflake**.

Este proyecto implementa una arquitectura profesional por capas:

RAW → STAGING → INTERMEDIATE → MARTS → METRICS → EXPOSURE

El objetivo es simular un entorno real de Analytics Engineering aplicando:

- Arquitectura por capas
- Modelado dimensional (Star Schema)
- Métricas SaaS (MRR, Revenue, Cohorts)
- Incrementales
- Data Quality Tests
- Exposures
- Deployment en producción con dbt Cloud
- Data Lineage completo

---

# 🏗️ Arquitectura del Proyecto

## 1️⃣ Capa RAW

La capa `RAW` contiene las tablas fuente sin transformaciones.

Tablas fuente:

- RAW_USERS  
- RAW_PLANS  
- RAW_SUBSCRIPTIONS  
- RAW_PAYMENTS  
- RAW_EVENTS  

📌 Diagrama relacional del modelo RAW:

<img width="1128" height="695" alt="image" src="https://github.com/user-attachments/assets/1c559bfb-083f-4b38-8a26-6f99d19bab8c" />

---

## 2️⃣ Capa STAGING

📂 `models/staging/saas/`

Objetivo:
- Limpieza
- Estandarización
- Tipado correcto
- Normalización de valores
- Aplicación de reglas básicas de calidad

Transformaciones aplicadas:

- `trim` y `upper` en IDs
- Reemplazo de NULL por `_SIN_*`
- Cast explícito de tipos (`date`, `timestamp`, `number`)
- Estandarización de billing_period y status
- Tests: `not_null`, `unique`, `accepted_values`

Modelos:

- `stg_saas__users`
- `stg_saas__plans`
- `stg_saas__subscriptions`
- `stg_saas__payments`
- `stg_saas__events`

---

## 3️⃣ Capa INTERMEDIATE

📂 `models/intermediate/saas/`

Objetivo:
- Enriquecer datos
- Realizar joins entre entidades
- Calcular métricas base
- Preparar estructura para modelo dimensional

Modelos:

- `int_user_current`
- `int_subscription_base`
- `int_payments_enriched`
- `int_mrr_monthly`

En esta capa se calculan:

- `net_amount_usd`
- `total_amount_usd`
- `is_successful_payment`
- MRR mensual normalizado

---

## 4️⃣ Capa MARTS (Star Schema)

📂 `models/marts/saas/`

### 📦 Dimensions

- `dim_user`
- `dim_plan`
- `dim_date`

Incluyen surrogate keys usando `dbt_utils.generate_surrogate_key`.

---

### 📊 Facts

- `fct_payments` (incremental)
- `fct_mrr` (incremental)
- `fct_cohorts`

Configuración incremental con `unique_key` compuesta y lógica de actualización por fecha.

---

## 5️⃣ Métricas Consolidadas

📂 `models/marts/saas/metrics/`

- `mart_metrics__mrr_monthly`
- `mart_metrics__revenue_monthly`

Estas vistas están diseñadas para consumo directo por herramientas BI.

---

# 📈 Métricas SaaS Implementadas

## MRR (Monthly Recurring Revenue)

Reglas:
- Plan mensual → precio directo
- Plan anual → precio / 12
- Se aplica descuento
- Solo pagos `PAID`

## Revenue

- Revenue bruto
- Revenue neto
- Conteo de pagos exitosos

## Cohort Analysis

- Usuarios agrupados por mes de registro
- Seguimiento de actividad mensual
- Cálculo de retención por cohort

---

# 🧪 Data Quality

Se implementan tests:

- not_null
- unique
- accepted_values

Ejecución:

```
dbt build
```

---

# 📊 Data Lineage

El proyecto incluye documentación automática generada con:

```
dbt docs generate
```

Lineage completo:

RAW → STAGING → INTERMEDIATE → DIMS → FACTS → METRICS → EXPOSURE

📌 Lineage visual:

<img width="1839" height="718" alt="image" src="https://github.com/user-attachments/assets/20ddeda5-0a53-4a35-a05f-dcaf635d59d1" />


El Exposure final conecta el dashboard ejecutivo con todos los modelos upstream.

---

# 📺 Exposure

Se definió un Exposure tipo dashboard:

**SaaS Executive Dashboard**

Depende de:

- mart_metrics__mrr_monthly
- mart_metrics__revenue_monthly
- fct_mrr
- fct_payments
- fct_cohorts
- dim_user
- dim_plan
- dim_date

Esto permite:

- Impact analysis
- Trazabilidad completa
- Documentación del consumo final

---

# 🚀 Deployment en Producción

Se configuró un Job en dbt Cloud:

Comando:

```
dbt build
```

El job ejecuta:

1. Staging
2. Intermediate
3. Marts (dims + facts)
4. Tests
5. Métricas
6. Documentación
7. Exposure

Ambientes separados:
- DEV
- PRD

Permite:
- Pipeline automatizado
- Validación en producción
- Documentación siempre actualizada

---

# 📂 Estructura del Proyecto

```
dbt_saas_demo/
├─ macros/
├─ models/
│  ├─ staging/
│  ├─ intermediate/
│  └─ marts/
│     ├─ dims/
│     ├─ facts/
│     └─ metrics/
├─ snapshots/
├─ analyses/
├─ tests/
├─ seeds/
└─ docs/
```

---

# 🧠 Arquitectura Final Implementada

✔ Arquitectura por capas  
✔ Modelo dimensional (Star Schema)  
✔ Surrogate Keys  
✔ Incrementales  
✔ Métricas SaaS  
✔ Cohort Analysis  
✔ Exposure documentado  
✔ Pipeline productivo  
✔ Data lineage completo  

---




## 📊 Streamlit in Snowflake (Native App)

Built using Snowpark session:

- No external credentials
- Governed access
- Fully deployed inside Snowflake
- Interactive filters
- Modern UI with Plotly


<img width="1589" height="638" alt="image" src="https://github.com/user-attachments/assets/8a39ff2d-033c-4417-9a66-4f25c3496d1e" />


<img width="1597" height="682" alt="image" src="https://github.com/user-attachments/assets/6a226a1d-6d76-420c-a517-95907508f7c8" />


<img width="1584" height="629" alt="image" src="https://github.com/user-attachments/assets/d8a34cc9-3aec-448d-8912-8d30da5d9f2d" />


<img width="1582" height="655" alt="image" src="https://github.com/user-attachments/assets/8c1fb51a-3ff0-4845-b83c-e8a5f1676cd6" />

# 🎯 Objetivo del Proyecto

Simular un entorno real de Analytics Engineering SaaS aplicando mejores prácticas profesionales de dbt, modelado dimensional y automatización de pipelines.

Proyecto diseñado como ejercicio avanzado de ingeniería analítica listo para entorno productivo.
