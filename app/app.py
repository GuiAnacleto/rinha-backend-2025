import os
import uvicorn
import logging
import falcon.asgi
import asyncio
import aiohttp
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Optional

# Configurações de endpoints
CHEAP_ENDPOINT = os.getenv("CHEAP_ENDPOINT", "http://localhost:8001/payments")
CHEAP_ENDPOINT_HEALTHCHECK = os.getenv("CHEAP_ENDPOINT_HEALTHCHECK", "http://localhost:8001/payments/service-health")
EXPENSIVE_ENDPOINT = os.getenv("EXPENSIVE_ENDPOINT", "http://localhost:8002/payments")
EXPENSIVE_ENDPOINT_HEALTHCHECK = os.getenv("EXPENSIVE_ENDPOINT_HEALTHCHECK", "http://localhost:8002/payments/service-health")

# Variáveis globais
PAYMENT_ENDPOINT = None

# Armazena apenas (timestamp_utc, amount) para cada tipo de endpoint
processed_payments = {
    "default": [],   # lista de tuplas (datetime UTC-aware, float)
    "fallback": []
}

payments_queue = asyncio.Queue()

@dataclass
class PaymentModel:
    correlationId: str
    amount: float
    timestamp: datetime = None  # preenchido pelo worker

# Loop assíncrono para ajustar endpoint dinamicamente
async def set_endpoint_loop():
    global PAYMENT_ENDPOINT
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                async with session.get(CHEAP_ENDPOINT_HEALTHCHECK) as response:
                    data = await response.json()
                    if not data.get("failing"):
                        PAYMENT_ENDPOINT = (CHEAP_ENDPOINT, "default")
                        logging.info("Using CHEAP_ENDPOINT")
                    else:
                        PAYMENT_ENDPOINT = (EXPENSIVE_ENDPOINT, "fallback")
                        logging.info("Using EXPENSIVE_ENDPOINT")
            except Exception as e:
                logging.warning(f"Healthcheck failed: {e}")
                PAYMENT_ENDPOINT = (EXPENSIVE_ENDPOINT, "fallback")
            await asyncio.sleep(5)

# Worker que processa pagamentos da fila
async def worker(name):
    global PAYMENT_ENDPOINT
    async with aiohttp.ClientSession() as session:
        while True:
            payment: PaymentModel = await payments_queue.get()
            try:
                async with session.post(PAYMENT_ENDPOINT[0], json={"correlationId": payment.correlationId, "amount": payment.amount}) as response:
                    response.raise_for_status()
                # Timestamp UTC-aware
                payment.timestamp = datetime.now(timezone.utc)
                processed_payments[PAYMENT_ENDPOINT[1]].append((payment.timestamp, payment.amount))
            except aiohttp.ClientError:
                # alterna endpoint em caso de falha
                if PAYMENT_ENDPOINT[1] == "default":
                    PAYMENT_ENDPOINT = (EXPENSIVE_ENDPOINT, "fallback")
                else:
                    PAYMENT_ENDPOINT = (CHEAP_ENDPOINT, "default")
            finally:
                payments_queue.task_done()

# Remove pagamentos antigos (>1 dia) para não crescer indefinidamente
async def cleanup_old_payments():
    while True:
        cutoff = datetime.now(timezone.utc) - timedelta(days=1)
        for key in ["default", "fallback"]:
            processed_payments[key] = [(ts, amt) for ts, amt in processed_payments[key] if ts >= cutoff]
        await asyncio.sleep(300)  # a cada 5 minutos

# Inicializa workers e cleanup
async def start_workers(num_workers=10):
    for i in range(num_workers):
        asyncio.create_task(worker(f"worker-{i}"))
    asyncio.create_task(cleanup_old_payments())

# POST /payments
class Payment:
    async def on_post(self, req, resp):
        try:
            raw = await req.media
            payment = PaymentModel(**raw)
            await payments_queue.put(payment)
        except Exception as e:
            resp.status = falcon.HTTP_400
            resp.media = {"error": "Invalid request body", "details": str(e)}
            return
        resp.status = falcon.HTTP_202
        resp.media = {"status": "queued"}

# GET /payments-summary?from=...&to=...
class PaymentSummary:
    async def on_get(self, req, resp):
        from_ts: Optional[str] = req.get_param("from")
        to_ts: Optional[str] = req.get_param("to")
        from_dt = datetime.fromisoformat(from_ts.replace("Z", "+00:00")).astimezone(timezone.utc) if from_ts else None
        to_dt = datetime.fromisoformat(to_ts.replace("Z", "+00:00")).astimezone(timezone.utc) if to_ts else None

        summary = {}
        for key in ["default", "fallback"]:
            payments = processed_payments[key]
            filtered = []
            for ts, amt in payments:
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
                if (from_dt is None or ts >= from_dt) and (to_dt is None or ts <= to_dt):
                    filtered.append((ts, amt))
            total_amount = sum(amt for _, amt in filtered)
            total_requests = len(filtered)
            summary[key] = {
                "totalRequests": total_requests,
                "totalAmount": total_amount
            }

        resp.status = falcon.HTTP_200
        resp.media = summary

# Inicializa o app Falcon
base_app = falcon.asgi.App()
base_app.add_route('/payments', Payment())
base_app.add_route('/payments-summary', PaymentSummary())

# Lifespan wrapper para tasks de background
class Lifespan:
    def __init__(self, app):
        self.app = app
        self.tasks_started = False

    async def __call__(self, scope, receive, send):
        if scope["type"] == "lifespan" and not self.tasks_started:
            self.tasks_started = True
            asyncio.create_task(set_endpoint_loop())
            asyncio.create_task(start_workers(num_workers=50))
            logging.info("Background tasks started")
        await self.app(scope, receive, send)

app = Lifespan(base_app)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    uvicorn.run("app:app", host="127.0.0.1", port=9999, reload=True)
