import itertools
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import logging

DEFAULT_TIMEOUT = 4
logger = logging.getLogger("retry_logger")

class RotatingProxyHTTPAdapter(HTTPAdapter):
    def __init__(self, proxies, timeout=DEFAULT_TIMEOUT, *args, **kwargs):
        self.timeout = timeout
        ca_cert_kw = kwargs.pop("ca_cert", None)

        if not proxies:
            raise ValueError("Please provide a non-empty list of proxies.")

        if all(isinstance(p, (list, tuple)) for p in proxies):
            self.proxies_cycle = itertools.cycle(proxies)
        else:
            ca_cert = kwargs.pop("ca_cert", None)
            if ca_cert is None:
                raise ValueError("When proxies is a list of strings, please provide a 'ca_cert'.")
            self.proxies_cycle = itertools.cycle([(p, ca_cert) for p in proxies])
        super().__init__(*args, **kwargs)

    def send(self, request, **kwargs):
        proxy, ca_cert = next(self.proxies_cycle)
        kwargs["timeout"] = kwargs.get("timeout", self.timeout)
        kwargs["proxies"] = {"http": proxy, "https": proxy}
        kwargs["verify"] = ca_cert
        return super().send(request, **kwargs)

def requests_retry_session(retries=3, backoff_factor=0.3, status_forcelist=(500, 502, 503, 504), proxies=None, ca_cert=None, session=None):
    session = session or requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        allowed_methods=frozenset(["GET", "POST"])
    )

    if proxies is None:
        raise ValueError("Please provide a list of proxies to rotate.")

    adapter = RotatingProxyHTTPAdapter(proxies=proxies, timeout=DEFAULT_TIMEOUT, ca_cert=ca_cert, max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session