import os
import asyncio
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from typing import Literal, Optional
import aiohttp
import backoff
import uvicorn
import uvloop
from fastapi import FastAPI, BackgroundTasks, Query
from fastapi.responses import ORJSONResponse
from pydantic import BaseModel, Field, field_validator
from sortedcontainers import SortedList


# -------------------------------------------------------------------
# CORREÇÃO PRINCIPAL: Modelo Pydantic sincronizado com o serviço de destino
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

class MemoryDB:
    _instance = None
    _data = {"default": SortedList(), "fallback": SortedList()}
    __last_amount = Decimal(0)

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    async def set(self, amount_value: Decimal, tipo: Literal["default", "fallback"] = "default"):
        now = datetime.now(timezone.utc)
        self._data[tipo].add((now, amount_value))
        self.__last_amount = amount_value

    async def get_between(self, from_dt: datetime, to_dt: datetime, tipo: Literal["default", "fallback"] = "default"):
        start_key = (from_dt, float('-inf'))
        end_key = (to_dt, float('inf'))
        items_in_range = self._data[tipo].irange(start_key, end_key)

        total = Decimal(0)
        count = 0
        for _, value in items_in_range:
            total += value
            count += 1

        return {"totalRequests": count, "totalAmount": total}

    @property
    def last_amount(self) -> Decimal:
        return self.__last_amount


app = FastAPI(
    title="High-Performance Payment Processor",
    description="API otimizada para processamento e consulta de pagamentos.",
    version="1.2.0",  # Versão incrementada após correção do modelo
    default_response_class=ORJSONResponse,
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
@backoff.on_exception(backoff.expo, Exception, max_tries=5, jitter=None)
async def process_payment(payment_data: dict):
    url = os.getenv("PAYMENT_PROCESSOR_URL_DEFAULT", "http://localhost:8001/payments")
    url_fallback = os.getenv("PAYMENT_PROCESSOR_URL_FALLBACK", "http://localhost:8002/payments")

    try:
        async with session.post(url, json=payment_data) as response:
            response.raise_for_status()
        await db.set(Decimal(payment_data["amount"]), tipo="default")
    except Exception as e:
        print(f"Falha no processador principal: {e}. Tentando fallback... ({payment_data['correlationId']})")
        try:
            async with session.post(url_fallback, json=payment_data) as response:
                response.raise_for_status()
            await db.set(Decimal(payment_data["amount"]), tipo="fallback")
        except Exception as fallback_error:
            print(f"Falha crítica no fallback: {fallback_error} ({payment_data['correlationId']})")
            raise fallback_error


async def worker(payment: Payment):
    async with semaphore:
        # model_dump agora criará um dicionário com todos os campos necessários
        await process_payment(payment.model_dump(mode="json"))


# Endpoints da API
@app.post("/payments", status_code=202)
async def payments(payment: Payment, background_tasks: BackgroundTasks):
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
    """
    Retorna um sumário dos pagamentos processados. Garante que todos os
    datetimes usados para a comparação de intervalo sejam 'timezone-aware'.
    """
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

# Runner com uvloop
if __name__ == "__main__":
    print("Iniciando servidor com otimizações de performance...")
    try:
        uvloop.install()
    except NotImplementedError:
        print("-> uvloop não disponível, usando event loop padrão do asyncio.")

    workers_count = int(os.getenv("UVICORN_WORKERS", 4))

    uvicorn.run(
        "run:app",
        host="0.0.0.0",
        port=9999,
        log_level="info",
        reload=False,
        workers=workers_count,
        http="h11",
        loop="uvloop" if 'uvloop' in locals() else "asyncio"
    )
