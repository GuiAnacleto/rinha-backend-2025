import os
import asyncio
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from typing import Literal, Optional
import aiohttp
import backoff
import uvicorn
from fastapi import FastAPI, BackgroundTasks, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, field_validator
from sortedcontainers import SortedList
import sys

# Tenta importar orjson, se falhar usa ujson ou json padrão
try:
    import orjson
    from fastapi.responses import ORJSONResponse as ResponseClass
except ImportError:
    try:
        import ujson as json

        ResponseClass = JSONResponse
    except ImportError:
        import json

        ResponseClass = JSONResponse

# Tenta importar uvloop (não disponível no PyPy)
try:
    import uvloop

    UVLOOP_AVAILABLE = True
except ImportError:
    UVLOOP_AVAILABLE = False

# Tenta importar módulos Cython
try:
    from payment_core import FastMemoryDB

    print("✓ Usando implementação Cython otimizada")
    USE_CYTHON = True
except ImportError:
    print("⚠ Cython não disponível, usando Python puro")
    USE_CYTHON = False


# -------------------------------------------------------------------
# Modelo Pydantic
# -------------------------------------------------------------------
class Payment(BaseModel):
    correlationId: str = Field(default_factory=lambda: str(uuid.uuid4()))
    amount: Decimal = Field(..., gt=0, description="Valor do pagamento, deve ser positivo.")
    requestedAt: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    @field_validator('requestedAt', mode='before')
    @classmethod
    def format_datetime(cls, v):
        if isinstance(v, str):
            return datetime.fromisoformat(v.replace('Z', '+00:00'))
        return v

    class Config:
        json_encoders = {
            datetime: lambda dt: dt.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        }


# -------------------------------------------------------------------
# MemoryDB com suporte a Cython
# -------------------------------------------------------------------
class MemoryDB:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            if USE_CYTHON:
                cls._instance._impl = FastMemoryDB()
            else:
                cls._instance._data = {"default": SortedList(), "fallback": SortedList()}
                cls._instance._lock = asyncio.Lock()
                cls._instance._processed = set()
        return cls._instance

    async def set(self, correlation_id: str, amount_value: Decimal, timestamp: datetime,
                  tipo: Literal["default", "fallback"] = "default"):
        """Adiciona pagamento ao banco"""
        if USE_CYTHON:
            # Converte para tipos que Cython entende
            return await self._impl.add_payment(
                correlation_id,
                float(amount_value),
                timestamp.timestamp(),
                tipo
            )
        else:
            # Implementação Python pura
            async with self._lock:
                if correlation_id in self._processed:
                    return False

                self._data[tipo].add((timestamp, amount_value))
                self._processed.add(correlation_id)
                return True

    async def get_between(self, from_dt: datetime, to_dt: datetime, tipo: Literal["default", "fallback"] = "default"):
        """Obtém resumo de pagamentos no período"""
        if USE_CYTHON:
            # Usa implementação Cython otimizada
            result = await self._impl.get_summary_fast(
                from_dt.timestamp(),
                to_dt.timestamp(),
                tipo
            )
            # Garante que totalAmount seja Decimal
            return {
                "totalRequests": result["totalRequests"],
                "totalAmount": Decimal(str(result["totalAmount"]))
            }
        else:
            # Implementação Python pura
            async with self._lock:
                start_key = (from_dt, float('-inf'))
                end_key = (to_dt, float('inf'))
                items_in_range = self._data[tipo].irange(start_key, end_key)

                total = Decimal(0)
                count = 0
                for _, value in items_in_range:
                    total += value
                    count += 1

                return {"totalRequests": count, "totalAmount": total}

    async def clear(self):
        """Limpa todos os dados"""
        if USE_CYTHON:
            await self._impl.clear()
        else:
            async with self._lock:
                self._data = {"default": SortedList(), "fallback": SortedList()}
                self._processed.clear()


# -------------------------------------------------------------------
# Configuração e estado global
# -------------------------------------------------------------------
app = FastAPI(
    title="High-Performance Payment Processor",
    description="API otimizada para processamento e consulta de pagamentos.",
    version="2.0.0",
    default_response_class=ResponseClass,
)

db = MemoryDB()
MAX_CONCURRENT_WORKERS = int(os.getenv("MAX_CONCURRENT_WORKERS", 100))
semaphore = asyncio.Semaphore(MAX_CONCURRENT_WORKERS)
session: aiohttp.ClientSession = None


@app.on_event("startup")
async def startup_event():
    global session
    timeout = aiohttp.ClientTimeout(total=10)
    connector = aiohttp.TCPConnector(limit_per_host=100)
    session = aiohttp.ClientSession(connector=connector, timeout=timeout)


@app.on_event("shutdown")
async def shutdown_event():
    if session:
        await session.close()


# -------------------------------------------------------------------
# Processamento de pagamentos
# -------------------------------------------------------------------
@backoff.on_exception(backoff.expo, Exception, max_tries=5, jitter=None)
async def process_payment(payment_data: dict):
    """Processa pagamento com retry e registra apenas após sucesso"""
    url = os.getenv("PAYMENT_PROCESSOR_URL_DEFAULT", "http://localhost:8001/payments")
    url_fallback = os.getenv("PAYMENT_PROCESSOR_URL_FALLBACK", "http://localhost:8002/payments")

    correlation_id = payment_data["correlationId"]
    amount = Decimal(payment_data["amount"])

    # Converte timestamp para datetime
    if isinstance(payment_data["requestedAt"], str):
        requested_at = datetime.fromisoformat(payment_data["requestedAt"].replace('Z', '+00:00'))
    else:
        requested_at = payment_data["requestedAt"]

    try:
        async with session.post(url, json=payment_data) as response:
            response.raise_for_status()
            # Só registra após sucesso confirmado
            await db.set(correlation_id, amount, requested_at, tipo="default")
            return
    except Exception as e:
        print(f"Falha no processador principal: {e}. Tentando fallback... ({correlation_id})")
        try:
            async with session.post(url_fallback, json=payment_data) as response:
                response.raise_for_status()
                # Só registra após sucesso confirmado
                await db.set(correlation_id, amount, requested_at, tipo="fallback")
                return
        except Exception as fallback_error:
            print(f"Falha crítica no fallback: {fallback_error} ({correlation_id})")
            # NÃO registra se falhou em ambos
            raise fallback_error


async def worker(payment: Payment):
    """Worker que processa pagamento com controle de concorrência"""
    async with semaphore:
        await process_payment(payment.model_dump(mode="json"))


# -------------------------------------------------------------------
# Endpoints da API
# -------------------------------------------------------------------
@app.post("/payments", status_code=202)
async def payments(payment: Payment, background_tasks: BackgroundTasks):
    """Recebe pagamento e processa em background"""
    background_tasks.add_task(worker, payment)
    return {"status": "Payment received and is being processed.", "correlationId": payment.correlationId}


class SummaryItem(BaseModel):
    totalRequests: int
    totalAmount: Decimal


class PaymentsSummaryResponse(BaseModel):
    default: SummaryItem
    fallback: SummaryItem


@app.get("/payments-summary", response_model=PaymentsSummaryResponse)
async def payments_summary(
        from_: Optional[datetime] = Query(None, alias="from", description="Timestamp ISO UTC inicial."),
        to: Optional[datetime] = Query(None, description="Timestamp ISO UTC final.")
):
    """Retorna um sumário dos pagamentos processados"""
    from_dt = from_ or datetime.min.replace(tzinfo=timezone.utc)
    to_dt = to or datetime.now(timezone.utc)

    if from_dt.tzinfo is None:
        from_dt = from_dt.replace(tzinfo=timezone.utc)

    if to_dt.tzinfo is None:
        to_dt = to_dt.replace(tzinfo=timezone.utc)

    default_summary, fallback_summary = await asyncio.gather(
        db.get_between(from_dt, to_dt, tipo="default"),
        db.get_between(from_dt, to_dt, tipo="fallback")
    )

    return {"default": default_summary, "fallback": fallback_summary}


@app.post("/purge-payments", status_code=204)
async def purge_payments():
    """Limpa todos os pagamentos (endpoint para testes)"""
    await db.clear()
    return


# -------------------------------------------------------------------
# Runner com uvloop
# -------------------------------------------------------------------
if __name__ == "__main__":
    print("Iniciando servidor com otimizações de performance...")
    print(f"Cython disponível: {USE_CYTHON}")
    print(f"PyPy: {'Sim' if hasattr(sys, 'pypy_version_info') else 'Não'}")

    if UVLOOP_AVAILABLE:
        try:
            uvloop.install()
            print("✓ uvloop instalado com sucesso")
        except NotImplementedError:
            print("⚠ uvloop não disponível no PyPy")
    else:
        print("⚠ uvloop não disponível, usando event loop padrão")

    workers_count = int(os.getenv("UVICORN_WORKERS", 4))

    uvicorn.run(
        "run:app",
        host="0.0.0.0",
        port=9999,
        log_level="info",
        reload=False,
        workers=workers_count,
        http="h11",
        loop="uvloop" if UVLOOP_AVAILABLE else "asyncio"
    )