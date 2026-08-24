from __future__ import annotations

import re
from decimal import Decimal, InvalidOperation
from typing import Any

from pydantic import BaseModel, Field, field_validator


class ConsultaRequest(BaseModel):
    periodo: str | None = None
    dry_run: bool = Field(False, alias="dryRun")

    @field_validator("periodo")
    @classmethod
    def periodo_valido(cls, value):
        if value is not None and (not re.fullmatch(r"\d{6}", value) or not 1 <= int(value[4:]) <= 12):
            raise ValueError("periodo debe tener formato AAAAMM y un mes válido")
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


def calcular_costo_total(valor_moviliza, valor_hora, horas_cargue, horas_descargue, horas_espera) -> Decimal:
    horas = sum(map(decimal_rndc, (horas_cargue, horas_descargue, horas_espera)), Decimal("0"))
    return decimal_rndc(valor_moviliza) + decimal_rndc(valor_hora) * horas

