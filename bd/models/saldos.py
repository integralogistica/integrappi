from bson import ObjectId

def modelo_manifesto_pago(manifesto_pago) -> dict:
    return {
        "id": str(manifesto_pago["_id"]), 
        "Tenedor": manifesto_pago["Tenedor"],
        "Manifiesto": manifesto_pago["Manifiesto"],
        "Saldo": manifesto_pago["Saldo"],
        "Fecha_saldo": manifesto_pago["Fecha_saldo"],
        "Deducciones": manifesto_pago["Deducciones"],
        "causal": manifesto_pago["causal"],
    }

def modelo_manifestos_pagos(manifestos_pagos) -> list:
    return [modelo_manifesto_pago(manifiesto) for manifiesto in manifestos_pagos]
