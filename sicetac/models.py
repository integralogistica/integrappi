from __future__ import annotations

import re
from decimal import Decimal, InvalidOperation
from typing import Any

from pydantic import BaseModel, Field, field_validator

from .config import (CONFIGURACIONES_VALIDAS,
                     HORAS_TOTALES_CARGUE_DEFAULT,
                     HORAS_TOTALES_DESCARGUE_DEFAULT)


class ConsultaRequest(BaseModel):
    periodo: str | None = None
    dry_run: bool = Field(False, alias="dryRun")
    horas_totales_cargue: Decimal = Field(HORAS_TOTALES_CARGUE_DEFAULT, ge=1)
    horas_totales_descargue: Decimal = Field(HORAS_TOTALES_DESCARGUE_DEFAULT, ge=1)

    @field_validator("periodo")
    @classmethod
    def periodo_valido(cls, value):
        if value is not None and (not re.fullmatch(r"\d{6}", value) or not 1 <= int(value[4:]) <= 12):
            raise ValueError("periodo debe tener formato AAAAMM y un mes válido")
        return value


class ExploracionRutaRequest(BaseModel):
    consulta_id_usuario: str | None = None
    fila_original: int | None = Field(None, ge=2)
    periodo: str
    configuracion: str
    origen: str
    destino: str
    condicion_carga: str | None = "1"
    unidad_transporte_nombre: str | None = None
    tipo_carga_nombre: str | None = None
    horas_totales_cargue: Decimal = Field(HORAS_TOTALES_CARGUE_DEFAULT, ge=1)
    horas_totales_descargue: Decimal = Field(HORAS_TOTALES_DESCARGUE_DEFAULT, ge=1)
    limit: int = Field(200, ge=1, le=1000)

    @field_validator("periodo")
    @classmethod
    def periodo_valido(cls, value):
        if not re.fullmatch(r"\d{6}", value) or not 1 <= int(value[4:]) <= 12:
            raise ValueError("periodo debe tener formato AAAAMM y un mes válido")
        return value

    @field_validator("configuracion")
    @classmethod
    def configuracion_valida(cls, value):
        value = value.strip().upper()
        if value not in CONFIGURACIONES_VALIDAS:
            raise ValueError("configuracion no es válida")
        return value

    @field_validator("origen", "destino")
    @classmethod
    def divipola_valido(cls, value):
        if not re.fullmatch(r"\d{8}", value):
            raise ValueError("debe ser un código DIVIPOLA de ocho dígitos")
        return value

    @field_validator("condicion_carga")
    @classmethod
    def condicion_valida(cls, value):
        if value is not None and value not in {"1", "2"}:
            raise ValueError("condicion_carga debe ser 1 (CARGADO) o 2 (VACÍO)")
        return value

    @field_validator("unidad_transporte_nombre", "tipo_carga_nombre")
    @classmethod
    def filtro_texto_valido(cls, value):
        if value is None:
            return value
        value = value.strip()
        if not value:
            raise ValueError("el filtro no puede estar vacío")
        return value


def decimal_rndc(value: Any) -> Decimal:
    text = str(value or "0").strip().replace(" ", "")
    if "," in text and "." in text:
        text = text.replace(".", "").replace(",", ".")
    elif "," in text:
        text = text.replace(",", ".")
    try:
        return Decimal(text)
    except InvalidOperation as exc:
        raise ValueError(f"Valor decimal RNDC inválido: {value!r}") from exc


def calcular_costo_total(valor_moviliza, valor_hora, horas_totales_cargue,
                         horas_totales_descargue) -> Decimal:
    horas = sum(map(decimal_rndc, (horas_totales_cargue, horas_totales_descargue)), Decimal("0"))
    return decimal_rndc(valor_moviliza) + decimal_rndc(valor_hora) * horas
