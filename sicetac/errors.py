class SicetacError(Exception):
    """Error base seguro de la integración."""


class ConfigurationError(SicetacError): pass
class RNDCCredentialsError(SicetacError): pass
class RNDCTransportError(SicetacError): pass
class RNDCSoapFaultError(SicetacError): pass
class RNDCBusinessError(SicetacError): pass
class RNDCNoDataError(RNDCBusinessError): pass
class RNDCResponseParseError(SicetacError): pass
class MongoPersistenceError(SicetacError): pass
