# 📊 dbt_saas_demo

Proyecto de modelado analítico para un negocio SaaS utilizando **dbt + Snowflake**.

Este proyecto sigue una arquitectura moderna por capas:

**RAW → STAGING → INTERMEDIATE → MARTS**

El objetivo es simular un entorno real de modelado analítico SaaS aplicando buenas prácticas de ingeniería de datos, testing y modelado dimensional.

---

# 🏗️ Arquitectura del Proyecto

## 1️⃣ Capa RAW

La capa `RAW` contiene las tablas fuente sin transformaciones.

Estas tablas representan el sistema operacional del SaaS.

Tablas actuales:

- RAW_USERS  
- RAW_PLANS  
- RAW_SUBSCRIPTIONS  
- RAW_PAYMENTS  
- RAW_EVENTS  

📌 El diagrama relacional del modelo RAW se muestra a continuación:

<img width="1128" height="695" alt="image" src="https://github.com/user-attachments/assets/1c559bfb-083f-4b38-8a26-6f99d19bab8c" />



---

## 2️⃣ Capa STAGING

📂 Ubicación:
models/staging/saas/

🎯 Objetivo:

- Limpieza de datos
- Estandarización
- Tipado correcto
- Normalización de valores
- Aplicación de reglas básicas de calidad

Transformaciones aplicadas:

- `trim` y `upper` en IDs y strings
- Reemplazo de valores nulos por `_SIN_*`
- Cast explícito de tipos (`date`, `timestamp`, `number`)
- Estandarización de `billing_period` y `status`
- Aplicación de tests básicos (`not_null`, `unique`, `accepted_values`)

Modelos creados:

- `stg_saas__users`
- `stg_saas__plans`
- `stg_saas__subscriptions`
- `stg_saas__payments`
- `stg_saas__events`

---

## 3️⃣ Capa INTERMEDIATE (En construcción)

📂 Ubicación:
models/intermediate/saas/

🎯 Objetivo:

- Enriquecer datos
- Realizar joins entre entidades
- Crear métricas base
- Preparar estructura para marts

Modelos actuales:

- `int_user_current`
- `int_subscription_base`
- `int_payments_enriched`
- `int_mrr_monthly`

En esta capa:

- Se combinan subscriptions + plans + users
- Se calculan métricas como `net_amount_usd` y `total_amount_usd`
- Se normaliza el MRR mensual
- Se prepara la base para análisis de churn y cohorts

---

# 📈 Métricas Implementadas

## MRR (Monthly Recurring Revenue)

MRR representa el ingreso recurrente mensual normalizado.

Reglas aplicadas:

- Plan mensual → precio directo
- Plan anual → precio / 12
- Se aplica descuento cuando existe
- Solo pagos con estado `PAID`

Ejemplo conceptual:

- Plan mensual $50 → MRR = $50
- Plan anual $1200 → MRR = $100

MRR es una de las métricas más importantes en modelos SaaS porque permite medir crecimiento y salud del negocio.

---

# 🧪 Data Quality

Se implementan tests de dbt en staging e intermediate:

- `not_null`
- `unique`
- `accepted_values`

Ejecución:

dbt build

Los tests garantizan consistencia estructural antes de pasar a la capa dimensional.

---

# 🧰 Macros

📂 Ubicación:
macros/

Se crearon macros reutilizables para:

- Limpieza de IDs
- Cast de fechas
- Cast de timestamps
- Cast de montos
- Estandarización de strings

Esto permite mantener consistencia, reducir duplicación de código y escalar el proyecto fácilmente.

---

# 📂 Estructura del Proyecto

dbt_saas_demo/
├─ macros/
│  ├─ cleaning.sql
│  └─ surrogate_keys.sql
│
├─ models/
│  ├─ staging/
│  ├─ intermediate/
│  └─ marts/
│
├─ snapshots/
├─ tests/
├─ analyses/
├─ seeds/
└─ docs/
