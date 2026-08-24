import unittest
import xml.etree.ElementTree as ET
from decimal import Decimal

from sicetac.config import COMBINACIONES, validar_combinaciones
from sicetac.errors import RNDCCredentialsError, RNDCSoapFaultError
from sicetac.models import calcular_costo_total
from sicetac.rndc_client import SOAP_ACTION, construir_envolvente_soap, construir_xml_rndc, interpretar_respuesta_soap, sanitizar
from sicetac.service import SicetacService, consulta_id, periodo_anterior


def soap(inner, prefix="soapenv"):
    escaped = inner.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    return f'<{prefix}:Envelope xmlns:{prefix}="http://schemas.xmlsoap.org/soap/envelope/"><{prefix}:Body><m:AtenderMensajeRNDCResponse xmlns:m="urn:x"><return>{escaped}</return></m:AtenderMensajeRNDCResponse></{prefix}:Body></{prefix}:Envelope>'.encode()


class XmlTests(unittest.TestCase):
    def test_xml_actual_comillas_y_escape(self):
        combo = dict(COMBINACIONES[0]); combo["origen_codigo"] = "12345678"
        data = construir_xml_rndc("u<&", "p&", "202608", combo)
        root = ET.fromstring(data)
        self.assertEqual(root.findtext("solicitud/tipo"), "6")
        self.assertEqual(root.findtext("solicitud/procesoid"), "26")
        self.assertEqual(root.findtext("documento/PERIODO"), "'202608'")
        self.assertEqual(root.findtext("acceso/username"), "u<&")

    def test_envolvente_y_action(self):
        body = construir_envolvente_soap(construir_xml_rndc("u", "p", "202608", COMBINACIONES[0]))
        self.assertIn(b"AtenderMensajeRNDC", body)
        self.assertIn(b"Request", body)
        self.assertEqual(SOAP_ACTION, "urn:BPMServicesIntf-IBPMServices#AtenderMensajeRNDC")

    def test_return_namespaces_y_multiples_documentos(self):
        inner = "<root><documento><periodo>202608</periodo><valormoviliza>1</valormoviliza></documento><documento><periodo>202608</periodo><valormoviliza>2</valormoviliza></documento></root>"
        self.assertEqual(len(interpretar_respuesta_soap(soap(inner, "s"))), 2)

    def test_error_credentials(self):
        with self.assertRaises(RNDCCredentialsError):
            interpretar_respuesta_soap(soap("<root><ErrorMSG>Usuario o contraseña inválida</ErrorMSG></root>"))

    def test_fault(self):
        data = b'<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/"><s:Body><s:Fault><faultcode>x</faultcode><faultstring>boom</faultstring></s:Fault></s:Body></s:Envelope>'
        with self.assertRaises(RNDCSoapFaultError): interpretar_respuesta_soap(data)

    def test_sanitizacion(self):
        self.assertEqual(sanitizar("user=juan password=secreto", "juan", "secreto"), "user=*** password=***")


class DomainTests(unittest.TestCase):
    def test_decimal_y_formula(self):
        value = calcular_costo_total("3873858", "101509", 2, 2, 2)
        self.assertIsInstance(value, Decimal)
        self.assertEqual(value, Decimal("4482912"))

    def test_id_deterministico(self):
        doc = {"periodo_aplicado": "202608", "origen": "1", "destino": "2", "configuracion": "3S3", "condicion_carga": "1", "tipo_carga": "General", "unidad_transporte": "Furgón", "rutasid": "9"}
        self.assertEqual(consulta_id(doc), consulta_id(dict(doc)))
        changed = dict(doc, rutasid="10")
        self.assertNotEqual(consulta_id(doc), consulta_id(changed))

    def test_configuracion_completa(self):
        validar_combinaciones()
        self.assertEqual(len(COMBINACIONES), 5)
        self.assertEqual({x["configuracion_codigo"] for x in COMBINACIONES}, {"3S3", "2", "2L3"})

    def test_periodo_anterior(self):
        self.assertEqual(periodo_anterior("202601"), "202512")


class FakeRepo:
    def comprobar(self): pass
    def upsert_many(self, docs): return len(docs), 0


class FakeClient:
    def __init__(self, answers): self.answers, self.periods = answers, []
    def consultar(self, periodo, combination):
        self.periods.append(periodo)
        answer = self.answers.pop(0) if self.answers else []
        if isinstance(answer, Exception): raise answer
        return answer


class ServiceTests(unittest.TestCase):
    def test_retrocede_solo_sin_documentos(self):
        client = FakeClient([[], [{"periodo":"202607", "origen":"08296000", "destino":"76892000", "configuracion":"2L3", "condicioncarga":"1", "valormoviliza":"1", "valorhora":"1"}]])
        SicetacService(client, FakeRepo()).ejecutar("202608", dry_run=True)
        self.assertEqual(client.periods[:2], ["202608", "202607"])

    def test_no_retrocede_autenticacion(self):
        client = FakeClient([RNDCCredentialsError("rechazada")])
        with self.assertRaises(RNDCCredentialsError): SicetacService(client, FakeRepo()).ejecutar("202608", dry_run=True)
        self.assertEqual(client.periods, ["202608"])


if __name__ == "__main__": unittest.main()
