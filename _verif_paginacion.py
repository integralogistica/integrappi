import io
import sys
import unicodedata

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.path.insert(0, ".")

import pdfplumber

pdf_bytes = open("descargas_sena/_paginacion_test.pdf", "rb").read()


def norm(s: str) -> str:
    s = unicodedata.normalize("NFD", s)
    return "".join(c for c in s if unicodedata.category(c) != "Mn").replace(" ", "")


TITULOS = {
    "Manifiestos de carga": "RNDC",
    "Antecedentes disciplinarios": "Procuraduría",
    "Antecedentes judiciales": "Policía",
    "Vehículo": "RUNT (título de sección)",
    "Comparendos": "SIMIT",
    "Formación SENA": "SENA",
    "Trazabilidad y auditoría": "Trazabilidad",
}
with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
    for i, page in enumerate(pdf.pages, 1):
        texto = norm(page.extract_text() or "")
        for frag, etiqueta in TITULOS.items():
            n = norm(frag)
            if n in texto:
                # posición: busca la palabra clave y usa su top
                clave = frag.split()[0]
                ws = [w for w in page.extract_words() if norm(w["text"]) == norm(clave)]
                if ws:
                    print(f"p{i} top={ws[0]['top']:.0f} · {etiqueta}")
