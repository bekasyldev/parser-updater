from urllib.parse import urlparse
from typing import Optional
from .base_parser import BaseParser
from .kaspi_parser import KaspiParser
from .wildberries_parser import WildberriesParser
from .ozon_parser import OzonParser
from .alibaba_parser import AlibabaParser

class ParserFactory:
    @staticmethod
    def get_parser(url: str, context, semaphore) -> Optional[BaseParser]:
        domain = urlparse(url).netloc.lower()
        
        if 'kaspi.kz' in domain:
            return KaspiParser(context, semaphore)
        elif 'wildberries' in domain:
            return WildberriesParser(context, semaphore)
        elif 'ozon' in domain:
            return OzonParser(context, semaphore)
        elif 'alibaba' in domain:
            return AlibabaParser(context, semaphore)
        else:
            return None 