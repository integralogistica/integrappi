"""Parser de facturas DIAN: Excel exportado del portal y XML UBL (sueltos o
dentro de ZIP de proveedor).

# puerto a Python de static/js/dian.js del módulo externo (analyzeRows,
parseExcelDate, parseDianXmlString, dupKeys). El PDF que viene dentro del ZIP
se devuelve aparte para subirlo como adjunto GCS — nunca a la base.
"""
import hashlib
import io
import re
import zipfile
import xml.etree.ElementTree as ET
from datetime import date, datetime, timezone
from decimal import Decimal

# La propia empresa: sus facturas de venta no se tramitan como cuentas por pagar
NIT_PROPIA = "900164363"
NOMBRES_PROPIA = ("INTEGRA CADENA DE SERVICIOS",)

_TIPOS_EXCLUIDOS = ("application response", "acuse", "recibo", "aceptacion", "rechazo", "reclamo")
_TIPOS_VALIDOS = ("factura", "venta", "credito", "debito")


def _norm(s) -> str:
    return re.sub(r"\s+", " ", str(s or "")).strip().lower()


def _digits(s) -> str:
    return re.sub(r"\D", "", str(s or ""))


def _num(v) -> float:
    """# puerto de parseNumber/dian.js: tolera $ 1.234.567,89 y variantes."""
    if v is None or v == "":
        return 0.0
    if isinstance(v, (int, float)):
        return float(v)
    t = str(v).replace("$", "").replace(" ", "").replace(" ", "")
    if "," in t and "." in t:
        t = t.replace(".", "").replace(",", ".") if t.rfind(",") > t.rfind(".") else t.replace(",", "")
    elif "," in t:
        t = t.replace(",", ".") if len(t.split(",")[-1]) <= 2 else t.replace(",", "")
    elif t.count(".") > 1:
        t = t.replace(".", "")
    try:
        return float(t)
    except ValueError:
        return 0.0


def _hoy_iso(d: date | None = None) -> str:
    return (d or datetime.now(timezone.utc).date()).isoformat()


def parse_excel_fecha(raw) -> str:
    """# puerto de parseExcelDate: serial Excel, Date y dd/mm/aaaa o aaaa-mm-dd."""
    if raw is None or raw == "":
        return _hoy_iso()
    if isinstance(raw, datetime):
        return _hoy_iso(raw.date())
    if isinstance(raw, date):
        return _hoy_iso(raw)
    if isinstance(raw, (int, float)):
        base = date(1899, 12, 30)  # serial Excel (mismo resultado que (raw-25569)*86400s)
        from datetime import timedelta
        return _hoy_iso(base + timedelta(days=int(raw)))
    s = str(raw).strip()
    m = re.match(r"^(\d{1,2})[/\-](\d{1,2})[/\-](\d{4})", s)
    if m:
        return f"{m.group(3)}-{int(m.group(2)):02d}-{int(m.group(1)):02d}"
    m = re.match(r"^(\d{4})[/\-](\d{1,2})[/\-](\d{1,2})", s)
    if m:
        return f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
    return _hoy_iso()


def es_documento_valido(tipo: str) -> bool:
    """# puerto de isDocumentoValidoDIAN: excluye acuses; permite factura/venta/NC/ND."""
    if not tipo:
        return True
    n = _norm(tipo)
    if any(x in n for x in _TIPOS_EXCLUIDOS):
        return False
    return any(x in n for x in _TIPOS_VALIDOS) or n in ("01", "91", "92")


def es_propia_empresa(nit: str, proveedor: str) -> bool:
    if _digits(nit) == NIT_PROPIA:
        return True
    p = _norm(proveedor)
    return bool(p) and any(prop in p for prop in (n.lower() for n in NOMBRES_PROPIA))


def _cufe_sintetico(semilla: str) -> str:
    """CUFE sintético (misma función que fakeCufe del original)."""
    return "LOCAL-" + hashlib.sha1(semilla.encode("utf-8")).hexdigest()[:32]


def claves_duplicacion(factura: dict) -> list[str]:
    """# puerto de dupKeys: cufe (si es largo) y nit+número."""
    keys = []
    cufe = str(factura.get("cufe") or "").strip().lower()
    if len(cufe) > 20:
        keys.append("cufe:" + cufe)
    n, f = _digits(factura.get("nit")), _digits(factura.get("numero_factura"))
    if n and f:
        keys.append(f"nf:{n}_{f}")
    return keys


# ── XML UBL ──────────────────────────────────────────────────────────────

def _local(tag: str) -> str:
    return tag.split("}")[-1].split(":")[-1] if tag else ""


def _hijos(elemento):
    return list(elemento) if elemento is not None else []


def _texto_directo(padre, nombre: str) -> str:
    for h in _hijos(padre):
        if _local(h.tag) == nombre:
            v = (h.text or "").strip()
            if v:
                return v
    return ""


def _primer_texto(padre, nombres) -> str:
    if padre is None:
        return ""
    for nombre in nombres:
        for el in padre.iter():
            if _local(el.tag) == nombre and (el.text or "").strip():
                return (el.text or "").strip()
    return ""


def _party_nit_nombre(contenedor) -> tuple[str, str]:
    """# puerto de parsePartyNitAndName."""
    nit, nombre = "", ""
    for el in contenedor.iter():
        if _local(el.tag) == "PartyTaxScheme" and not (nit and nombre):
            nit = nit or _primer_texto(el, ["CompanyID"])
            nombre = nombre or _primer_texto(el, ["RegistrationName"])
        if _local(el.tag) == "PartyLegalEntity":
            nit = nit or _primer_texto(el, ["CompanyID"])
            nombre = nombre or _primer_texto(el, ["RegistrationName"])
        if _local(el.tag) == "PartyIdentification" and not nit:
            nit = _primer_texto(el, ["ID"])
        if _local(el.tag) == "PartyName" and not nombre:
            nombre = _primer_texto(el, ["Name"])
    nit = re.sub(r"[^\d]", "", str(nit).split("-")[0])
    return nit, str(nombre).strip().upper()


def parse_xml_dian(texto: str) -> dict | None:
    """# puerto de parseDianXmlString. Devuelve la fila-equivalente del Excel
    (mismas claves) o None si no es un documento contable reconocible."""
    if not texto or not texto.strip():
        return None
    try:
        raiz = ET.fromstring(texto.strip().encode("utf-8"))
    except ET.ParseError:
        return None

    nombre_raiz = _local(raiz.tag)
    activo = raiz
    # AttachedDocument: la factura viene embebida (escapada) en un Description
    if nombre_raiz == "AttachedDocument":
        for el in raiz.iter():
            if _local(el.tag) != "Description":
                continue
            crudo = (el.text or "").strip()
            if "<Invoice" in crudo or "<CreditNote" in crudo or "<DebitNote" in crudo:
                try:
                    interno = ET.fromstring(crudo.encode("utf-8"))
                    activo = interno
                    break
                except ET.ParseError:
                    continue

    nombre_activo = _local(activo.tag)

    if nombre_activo == "ApplicationResponse":
        return {
            "tipo_documento": "Application response",
            "cufe": _texto_directo(activo, "UUID"),
            "prefijo": "",
            "folio": _texto_directo(activo, "ID"),
            "fecha_emision": _texto_directo(activo, "IssueDate") or _hoy_iso(),
            "nit": "", "proveedor": "", "iva": 0, "total": 0,
        }

    tipo_doc = "Factura electrónica"
    if nombre_activo == "CreditNote":
        tipo_doc = "Nota de crédito electrónica"
    elif nombre_activo == "DebitNote":
        tipo_doc = "Nota de débito electrónica"

    doc_id = _texto_directo(activo, "ID")
    if not doc_id and nombre_raiz == "AttachedDocument":
        doc_id = _texto_directo(raiz, "ParentDocumentID") or _texto_directo(raiz, "ID")
    prefijo = _primer_texto(activo, ["Prefix"])
    folio = doc_id
    if prefijo and doc_id and doc_id.upper().startswith(prefijo.upper()):
        folio = doc_id[len(prefijo):] or doc_id

    cufe = _texto_directo(activo, "UUID")
    if not cufe and nombre_raiz == "AttachedDocument":
        cufe = _primer_texto(raiz, ["UUID"])

    fecha = (_texto_directo(activo, "IssueDate") or _texto_directo(raiz, "IssueDate") or _hoy_iso())

    nit, nombre = "", ""
    for el in activo.iter():
        if _local(el.tag) == "AccountingSupplierParty":
            nit, nombre = _party_nit_nombre(el)
            break
    if (not nit or not nombre) and nombre_raiz == "AttachedDocument":
        for el in raiz.iter():
            if _local(el.tag) == "SenderParty":
                n2, nom2 = _party_nit_nombre(el)
                nit = nit or n2
                nombre = nombre or nom2
                break

    total, iva = 0.0, 0.0
    for el in activo.iter():
        if _local(el.tag) == "LegalMonetaryTotal":
            payable = _texto_directo(el, "PayableAmount") or _texto_directo(el, "TaxInclusiveAmount")
            total = float(payable or 0)
            break
    # TaxTotal de primer nivel del documento (no los de las líneas)
    for h in _hijos(activo):
        if _local(h.tag) == "TaxTotal":
            scheme = _primer_texto(h, ["ID"])
            nombre_imp = _primer_texto(h, ["Name"]).upper()
            monto = float(_texto_directo(h, "TaxAmount") or 0)
            if scheme == "01" or "IVA" in nombre_imp:
                iva = monto
                break
            iva = iva or monto

    if not nit and not nombre and not doc_id and not cufe:
        return None

    return {
        "tipo_documento": tipo_doc,
        "cufe": cufe,
        "prefijo": prefijo,
        "folio": folio or doc_id or "",
        "fecha_emision": fecha,
        "nit": nit,
        "proveedor": nombre,
        "iva": round(iva),
        "total": round(total),
        "origen": "xml",
    }


# ── Excel del portal DIAN ────────────────────────────────────────────────

_CANDIDATAS = {
    "tipo": ["tipo de documento", "tipo documento", "tipo"],
    "cufe": ["cufe/cude", "cufe", "cude"],
    "prefijo": ["prefijo"],
    "folio": ["folio", "numero", "número", "no. factura", "consecutivo"],
    "fecha": ["fecha emisión", "fecha emision", "fecha de emisión", "fecha"],
    "nit": ["nit emisor", "nit proveedor", "documento emisor", "nit"],
    "prov": ["nombre emisor", "razón social emisor", "razon social emisor", "proveedor", "emisor"],
    "iva": ["iva", "valor iva", "total iva"],
    "total": ["total", "valor total", "total factura", "valor"],
}


def _localizar_columnas(encabezados: list[str]) -> tuple[dict[str, str], dict[str, str]]:
    """# puerto de findCol: match exacto normalizado y luego por inclusión."""
    normalizados = {_norm(h): h for h in encabezados}
    col: dict[str, str] = {}
    for clave, candidatas in _CANDIDATAS.items():
        for c in candidatas:
            if c in normalizados:
                col[clave] = normalizados[c]
                break
        if clave not in col:
            for c in candidatas:
                for nk, original in normalizados.items():
                    if c in nk:
                        col[clave] = original
                        break
                if clave in col:
                    break
    return col, normalizados


def leer_excel_dian(contenido: bytes) -> list[dict]:
    """Lee el Excel exportado del portal DIAN y devuelve filas sin procesar."""
    import openpyxl
    wb = openpyxl.load_workbook(io.BytesIO(contenido), read_only=True, data_only=True)
    ws = wb.worksheets[0]
    filas = list(ws.iter_rows(values_only=True))
    wb.close()
    hi = next((i for i, f in enumerate(filas[:10]) if f and sum(1 for c in f if c not in (None, "")) >= 4), 0)
    encabezados = [str(c or "") for c in filas[hi]]
    out = []
    for f in filas[hi + 1:]:
        if not f or all(c in (None, "") for c in f):
            continue
        out.append({encabezados[i]: v for i, v in enumerate(f) if i < len(encabezados)})
    return out


def analizar_filas(filas: list[dict], existentes: set[str]) -> dict:
    """# puerto de analyzeRows: dedup, exclusión de acuses y de facturas propias.

    `existentes` trae las claves de duplicación ya presentes en la base.
    """
    if not filas:
        return {"error": "El archivo no contiene filas."}
    col, _ = _localizar_columnas(list(filas[0].keys()))
    if "nit" not in col and "cufe" not in col:
        return {"error": "No se encontraron columnas de NIT Emisor ni CUFE. Verifica que sea el reporte de la DIAN."}

    def g(fila, clave):
        nombre = col.get(clave)
        return fila.get(nombre) if nombre else None

    nuevas, duplicadas, omitidas, propias = [], [], [], []
    vistas: set[str] = set()

    for r in filas:
        tipo = str(g(r, "tipo") or "").strip() or "Factura electrónica"
        if not es_documento_valido(tipo):
            omitidas.append(tipo)
            continue
        cufe = str(g(r, "cufe") or "").strip()
        prefijo = str(g(r, "prefijo") or "").strip()
        folio = str(g(r, "folio") or "").strip().replace(".0", "")
        nit = str(g(r, "nit") or "").strip().replace(".0", "")
        prov = str(g(r, "prov") or "").strip().upper()
        iva = _num(g(r, "iva")) if "iva" in col else 0
        total = _num(g(r, "total")) if "total" in col else 0
        if not prov and not nit and not folio and not cufe:
            continue
        if es_propia_empresa(nit, prov):
            propias.append({"proveedor": prov, "nit": nit, "folio": folio, "total": total})
            continue

        numero = folio
        if prefijo and folio and not folio.upper().startswith(prefijo.upper()):
            numero = prefijo + folio
        factura = {
            "cufe": cufe or _cufe_sintetico(f"{nit}-{numero}"),
            "prefijo": prefijo,
            "folio": folio,
            "numero_factura": numero or "S/N",
            "nit": nit or "SIN NIT",
            "proveedor": prov or "PROVEEDOR SIN NOMBRE",
            "fecha_emision": parse_excel_fecha(g(r, "fecha")) if "fecha" in col else _hoy_iso(),
            "tipo_documento": tipo,
            "iva": iva,
            "total": total,
            "origen": "excel",
        }
        claves = claves_duplicacion(factura)
        if any(k in existentes or k in vistas for k in claves):
            duplicadas.append(factura)
        else:
            vistas.update(claves)
            nuevas.append(factura)

    return {"total_filas": len(filas), "nuevas": nuevas, "duplicadas": duplicadas,
            "omitidas": omitidas, "propias": propias}


# ── ZIP con XMLs (+PDF) ──────────────────────────────────────────────────

def extraer_zip_dian(contenido: bytes, max_xml: int = 500) -> dict:
    """Recorre el ZIP del proveedor: parsea cada .xml y junta el PDF del mismo
    nombre como adjunto candidato. # puerto de extractRowsFromZipFile."""
    facturas: list[dict] = []
    pdfs: dict[str, tuple[str, bytes]] = {}  # nombre base → (nombre, contenido)
    errores: list[str] = []
    with zipfile.ZipFile(io.BytesIO(contenido)) as z:
        nombres = z.namelist()
        for n in nombres:
            if n.lower().endswith(".pdf"):
                try:
                    pdfs[n.rsplit("/", 1)[-1].rsplit(".", 1)[0].lower()] = (n.rsplit("/", 1)[-1], z.read(n))
                except Exception:
                    pass
        procesados = 0
        for n in nombres:
            if not n.lower().endswith((".xml",)):
                continue
            if procesados >= max_xml:
                errores.append(f"Tope de {max_xml} XML por carga alcanzado")
                break
            procesados += 1
            try:
                fila = parse_xml_dian(z.read(n).decode("utf-8", errors="replace"))
            except Exception as exc:
                errores.append(f"{n}: {exc}")
                continue
            if fila is None:
                continue
            if fila.get("tipo_documento") == "Application response":
                continue  # acuse
            base = n.rsplit("/", 1)[-1].rsplit(".", 1)[0].lower()
            fila["_pdf"] = pdfs.get(base)  # (nombre, bytes) o None
            facturas.append(fila)
    return {"facturas": facturas, "errores": errores, "total_archivos": len(nombres)}
