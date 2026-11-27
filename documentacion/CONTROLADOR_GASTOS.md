# Controlador de Gastos (personal)

Billetera de gastos **doméstica y SIN autenticación** (uso compartido del usuario con su
esposa): tres fuentes de dinero, registro de gastos/ingresos y saldo siempre visible por
fuente y en general, con los pesos colombianos convertidos a dólares a la TRM del día.

URL (producción): `https://integralogistica.com/integrapp/ControladorGastos/`
Local: `http://localhost:3000/ControladorGastos`

> ⚠️ **Pública por decisión explícita**: ni el backend ni el front exigen sesión. Cualquiera
> con el link puede ver y modificar los movimientos. No exponer fuera del círculo familiar.

---

## 1. Archivos

| Capa | Archivo | Contenido |
|---|---|---|
| Backend | `integrappi/rutas/controlador_gastos.py` | Router `/controlador-gastos` (sin auth), registrado en `integrappi/main.py` |
| Frontend | `integrapp-next/src/app/ControladorGastos/page.tsx` | Página pública (`'use client'`, sin lectura de cookies) |
| Estilos | `integrapp-next/src/app/ControladorGastos/estilos.css` | CSS propio del módulo |

Base de datos: `integra` (MongoDB), colecciones:
- **`controlador_gastos_movimientos`** — un doc por movimiento (gasto/ingreso).
- **`controlador_gastos_config`** — caché de la TRM del día (doc `clave: "trm"`).

---

## 2. Modelo: los saldos NUNCA se guardan

El saldo de cada fuente se **calcula** con un aggregate sobre los movimientos
(`ingresos − gastos`). Consecuencias importantes:

- Borrar o corregir un movimiento recalcula los saldos solo (no hay número que se
  desincronice).
- El estado inicial se sembró **una única vez** (2026-09-22) como 3 movimientos
  `subtipo: "inicial"` — nunca volver a sembrar (el seed solo corre con la colección vacía).

Documento de movimiento:

```json
{
  "tipo": "gasto" | "ingreso",
  "subtipo": "inicial",            // solo los 3 siembras
  "fuente": "efectivo_usd" | "virtual_usd" | "efectivo_cop",
  "moneda": "USD" | "COP",         // la moneda NATURAL de la fuente (derivada, no la envía el front)
  "monto": 250.0,
  "descripcion": "…", "categoria": "…",   // opcionales
  "fecha": "<datetime hora Bogotá>"
}
```

### Fuentes

| id | Etiqueta | Moneda |
|---|---|---|
| `efectivo_usd` | Dólares físicos | USD |
| `virtual_usd` | Dólares virtuales (tarjeta) | USD |
| `efectivo_cop` | Pesos (COP) | COP |

El monto de cada movimiento va en la moneda de su fuente: un gasto de la fuente COP se
registra en pesos (ej. `50000`), no en dólares.

---

## 3. TRM del día

`_obtener_trm()` (COP por USD), con degradación en cascada:

1. **Caché Mongo** por día (`controlador_gastos_config`, clave `trm`) — hit del mismo día.
2. `https://trm-colombia.vercel.app/api` (proxy de la Superfinanciera, sin key).
3. Fallback `https://open.er-api.com/v6/latest/USD` → `rates.COP` (el 2026-09-22 el
   primero falló y este dio la tasa).
4. Si ambas fuentes caen: última TRM guardada; si nunca hubo: constante `TRM_POR_DEFECTO`
   (4000).

La respuesta siempre indica el `origen` (cache / trm-colombia / er-api /
ultima-conocida / por-defecto) y el front lo muestra como tooltip del chip "TRM hoy".

Con la TRM se calculan los totales consolidados: `total_usd` = USD físicos + USD
virtuales + (COP ÷ TRM); `total_cop` = `total_usd × TRM`.

---

## 4. Endpoints

| Método | Ruta | Descripción |
|---|---|---|
| `GET` | `/controlador-gastos/estado?limite=100` | TRM + fuentes con saldo + totales + últimos movimientos (`limite` máx 500) |
| `POST` | `/controlador-gastos/movimiento` | Body JSON `{tipo, fuente, monto, categoria?, descripcion?}` → crea el movimiento y devuelve el estado recalculado |
| `DELETE` | `/controlador-gastos/movimiento/{id}` | Borra un movimiento (recalcula) |

Validaciones:
- `tipo` ∈ {`gasto`, `ingreso`}; `fuente` ∈ las 3 válidas; `monto > 0`.
- **Gasto mayor que el saldo de la fuente → 409** con mensaje que sugiere registrar el
  ingreso primero. Es una billetera compartida: un gasto que desborda es casi siempre un
  error de dedo, por eso se bloquea (la fuente nunca queda en negativo).

Ejemplo `GET /estado` (recortado):

```json
{
  "trm": { "valor": 3200.18, "fecha": "2026-09-22", "origen": "er-api" },
  "fuentes": [
    { "id": "efectivo_usd", "etiqueta": "Dólares físicos", "moneda": "USD", "saldo": 750.0, "saldo_usd": 750.0 },
    { "id": "efectivo_cop", "etiqueta": "Pesos (COP)", "moneda": "COP", "saldo": 8000000.0, "saldo_usd": 2499.86 }
  ],
  "total_usd": 4499.86, "total_cop": 14400376.0,
  "movimientos": [ ... ], "total_movimientos": 3
}
```

---

## 5. Frontend

`src/app/ControladorGastos/page.tsx`, render 100% cliente (compatible con `output: 'export'`):

- **Header**: título + chip "TRM hoy" (tooltip con fecha y fuente de la tasa).
- **Tarjeta total** (verde): disponible general en USD y equivalente COP.
- **3 tarjetas de fuente**: saldo en su moneda; la de COP añade "≈ X USD al cambio del
  día". Una fuente en 0 o menos se pinta roja (`cg-agotada`).
- **Formulario**: toggle **− Gasto / + Ingreso** (rojo/verde), fuente, monto (placeholder
  según la moneda), categoría y descripción opcionales. Tras guardar refresca estado y
  muestra un toast.
- **Lista de movimientos**: los últimos N (lo que devuelva `/estado`), con signo y color
  por tipo, etiqueta de fuente, categoría/descripción, fecha (hora Bogotá tal cual, sin
  conversar zona) y botón 🗑 con confirmación Swal.
- API base: `process.env.NEXT_PUBLIC_API_BASE_URL` (fallback `http://127.0.0.1:8000`),
  mismo patrón que el resto de páginas.

---

## 6. Cómo usarlo día a día

1. Abrir `/ControladorGastos/` (sin login).
2. Gastar → toggle **− Gasto**, elegir fuente, monto en la moneda de la fuente, categoría
   opcional → "Descontar gasto". El saldo de la fuente y el total bajan al instante.
3. Recibir plata → toggle **+ Ingreso** → "Sumar ingreso".
4. Error de dedo → 🗑 en el movimiento; los saldos se recalculan solos.

## 7. Pendiente

- **Deploy**: Render (backend, ya registrado en `main.py`) + static export a GoDaddy
  (`public_html/integrapp/`, `trailingSlash: true`). El seed inicial ya quedó aplicado en
  la BD de producción.
