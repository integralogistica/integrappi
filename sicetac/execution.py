import threading

# Un solo flujo RNDC por proceso: consultas configuradas, Excel síncrono o job masivo.
RNDC_EXECUTION_LOCK = threading.Lock()
