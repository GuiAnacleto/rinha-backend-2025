import os
import uvicorn
import logging
import falcon.asgi
import asyncio
import aiohttp
import bisect
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Optional, List, Tuple
from functools import lru_cache
import threading

# Configurações de endpoints
CHEAP_ENDPOINT = os.getenv("PAYMENT_PROCESSOR_URL_DEFAULT", "http://localhost:8001/payments")
CHEAP_ENDPOINT_HEALTHCHECK = os.getenv("CHEAP_ENDPOINT_HEALTHCHECK", f"{CHEAP_ENDPOINT}/service-health")
EXPENSIVE_ENDPOINT = os.getenv("PAYMENT_PROCESSOR_URL_FALLBACK", "http://localhost:8002/payments")
EXPENSIVE_ENDPOINT_HEALTHCHECK = os.getenv("EXPENSIVE_ENDPOINT_HEALTHCHECK", f"{EXPENSIVE_ENDPOINT}/service-health")

# Variáveis globais para métricas de retry
retry_metrics = {
    "total_retries": 0,
    "permanent_failures": 0,
    "retry_success": 0
}

PAYMENT_ENDPOINT = CHEAP_ENDPOINT


# Estrutura otimizada para busca binária
class OptimizedPaymentStorage:
    def __init__(self):
        self.data = {
            "default": {
                "timestamps": [],
                "amounts": [],
                "cumulative": [],
                "total_count": 0
            },
            "fallback": {
                "timestamps": [],
                "amounts": [],
                "cumulative": [],
                "total_count": 0
            }
        }
        self._lock = threading.RLock()  # Thread-safe para workers concorrentes
        self._needs_rebuild = {"default": False, "fallback": False}
        self._raw_payments = {"default": [], "fallback": []}  # Buffer temporário
        self._last_rebuild = {"default": datetime.now(), "fallback": datetime.now()}

    def add_payment(self, key: str, timestamp: datetime, amount: float):
        """Adiciona pagamento de forma thread-safe"""
        with self._lock:
            # Adiciona ao buffer temporário
            self._raw_payments[key].append((timestamp, amount))
            self._needs_rebuild[key] = True

            # Rebuild automático a cada 1000 pagamentos ou 30 segundos
            should_rebuild = (
                    len(self._raw_payments[key]) >= 1000 or
                    (datetime.now() - self._last_rebuild[key]).total_seconds() > 30
            )

            if should_rebuild:
                self._rebuild_index(key)

    def _rebuild_index(self, key: str):
        """Reconstrói índices otimizados para busca binária"""
        if not self._needs_rebuild[key]:
            return

        # Combina dados existentes com buffer
        all_payments = []

        # Adiciona dados já indexados
        existing_data = self.data[key]
        for i in range(len(existing_data["timestamps"])):
            all_payments.append((existing_data["timestamps"][i], existing_data["amounts"][i]))

        # Adiciona novos pagamentos do buffer
        all_payments.extend(self._raw_payments[key])

        if not all_payments:
            return

        # Normaliza timezone e ordena
        normalized = []
        for ts, amt in all_payments:
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            else:
                ts = ts.astimezone(timezone.utc)
            normalized.append((ts, amt))

        # Ordena por timestamp
        normalized.sort(key=lambda x: x[0])

        # Separa arrays
        timestamps = [ts for ts, _ in normalized]
        amounts = [amt for _, amt in normalized]

        # Calcula somas cumulativas
        cumulative = []
        running_sum = 0.0
        for amt in amounts:
            running_sum += amt
            cumulative.append(running_sum)

        # Atualiza dados de forma atômica
        self.data[key] = {
            "timestamps": timestamps,
            "amounts": amounts,
            "cumulative": cumulative,
            "total_count": len(timestamps)
        }

        # Limpa buffer e marca como atualizado
        self._raw_payments[key].clear()
        self._needs_rebuild[key] = False
        self._last_rebuild[key] = datetime.now()

        logging.debug(f"Rebuilt index for {key}: {len(timestamps)} payments")

    def get_data(self, key: str):
        """Retorna dados otimizados, rebuilding se necessário"""
        with self._lock:
            if self._needs_rebuild[key]:
                self._rebuild_index(key)
            return self.data[key]

    def force_rebuild_all(self):
        """Força rebuild de todos os índices"""
        with self._lock:
            for key in ["default", "fallback"]:
                self._rebuild_index(key)


# Instância global do storage otimizado
optimized_storage = OptimizedPaymentStorage()

payments_queue = asyncio.Queue()


@dataclass
class PaymentModel:
    correlationId: str
    amount: float
    timestamp: datetime = None
    retry_count: int = 0
    max_retries: int = 3


# Loop assíncrono para ajustar endpoint dinamicamente
async def set_endpoint_loop():
    global PAYMENT_ENDPOINT
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=5)) as session:
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
                logging.warning(f"Health check failed: {e}")
                PAYMENT_ENDPOINT = (EXPENSIVE_ENDPOINT, "fallback")

            await asyncio.sleep(10)  # Check a cada 10 segundos


# Worker otimizado que processa pagamentos da fila com retry
async def worker(name):
    global PAYMENT_ENDPOINT
    async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=10),
            connector=aiohttp.TCPConnector(limit=100)
    ) as session:
        while True:
            try:
                payment: PaymentModel = await payments_queue.get()

                # Tenta processar pagamento
                current_endpoint, endpoint_type = PAYMENT_ENDPOINT
                payment_successful = False
                successful_endpoint_type = None

                payload = {
                    "correlationId": payment.correlationId,
                    "amount": payment.amount
                }

                # Log do retry se não for primeira tentativa
                if payment.retry_count > 0:
                    logging.info(
                        f"Payment {payment.correlationId} retry attempt {payment.retry_count}/{payment.max_retries}")

                # Primeira tentativa no endpoint atual
                try:
                    async with session.post(current_endpoint, json=payload) as response:
                        response.raise_for_status()
                        # SUCESSO: registra o pagamento apenas se response for 2xx
                        payment_successful = True
                        successful_endpoint_type = endpoint_type
                        logging.debug(
                            f"Payment {payment.correlationId} succeeded on {endpoint_type} (attempt {payment.retry_count + 1})")

                except aiohttp.ClientError as e:
                    logging.warning(f"Payment {payment.correlationId} failed on {endpoint_type}: {e}")

                    # Tenta fallback apenas se primeira tentativa falhou
                    fallback_endpoint = EXPENSIVE_ENDPOINT if endpoint_type == "default" else CHEAP_ENDPOINT
                    fallback_type = "fallback" if endpoint_type == "default" else "default"

                    try:
                        async with session.post(fallback_endpoint, json=payload) as response:
                            response.raise_for_status()
                            # SUCESSO NO FALLBACK: registra o pagamento
                            payment_successful = True
                            successful_endpoint_type = fallback_type
                            logging.info(
                                f"Payment {payment.correlationId} succeeded on fallback {fallback_type} (attempt {payment.retry_count + 1})")

                    except aiohttp.ClientError as fallback_error:
                        logging.warning(
                            f"Payment {payment.correlationId} failed on both endpoints. Primary: {e}, Fallback: {fallback_error}")

                # Se pagamento foi bem-sucedido, registra
                if payment_successful and successful_endpoint_type:
                    payment.timestamp = datetime.now(timezone.utc)
                    optimized_storage.add_payment(successful_endpoint_type, payment.timestamp, payment.amount)

                    # Atualiza métricas se foi retry bem-sucedido
                    if payment.retry_count > 0:
                        retry_metrics["retry_success"] += 1
                        logging.info(f"Payment {payment.correlationId} succeeded after {payment.retry_count} retries")

                    logging.debug(
                        f"Recorded payment {payment.correlationId}: ${payment.amount} via {successful_endpoint_type}")

                # Se falhou E ainda tem tentativas, reencadeia na fila
                elif payment.retry_count < payment.max_retries:
                    payment.retry_count += 1
                    retry_metrics["total_retries"] += 1

                    # Delay exponencial: 2^retry_count segundos
                    #retry_delay = min(2 ** payment.retry_count, 60)  # Max 60 segundos

                    # Agenda retry depois do delay
                    asyncio.create_task(retry_payment_after_delay(payment))

                # Esgotou todas as tentativas
                else:
                    retry_metrics["permanent_failures"] += 1
                    logging.error(
                        f"Payment {payment.correlationId} permanently failed after {payment.max_retries} attempts")
                    # Aqui você pode implementar dead letter queue, notificação, etc.
                    # Exemplo: salvar em banco para análise posterior
                    await handle_permanently_failed_payment(payment)

            except Exception as e:
                logging.error(f"Worker {name} unexpected error: {e}")
            finally:
                payments_queue.task_done()


# Função para agendar retry com delay
async def retry_payment_after_delay(payment: PaymentModel, delay_seconds: int = 0):
    """Aguarda delay e recoloca payment na fila"""
    try:
        await asyncio.sleep(delay_seconds)
        await payments_queue.put(payment)
        logging.debug(f"Payment {payment.correlationId} re-queued after {delay_seconds}s delay")
    except Exception as e:
        logging.error(f"Failed to retry payment {payment.correlationId}: {e}")


# Handler para pagamentos que falharam permanentemente
async def handle_permanently_failed_payment(payment: PaymentModel):
    """Trata pagamentos que falharam em todas as tentativas"""
    try:
        # Aqui você pode:
        # 1. Salvar em dead letter queue/database
        # 2. Enviar notificação
        # 3. Gerar alerta
        # 4. Marcar para análise manual

        failed_payment_data = {
            "correlationId": payment.correlationId,
            "amount": payment.amount,
            "failed_at": datetime.now(timezone.utc).isoformat(),
            "retry_attempts": payment.retry_count,
            "reason": "max_retries_exceeded"
        }

        # Exemplo: log estruturado para monitoramento
        logging.error(f"PERMANENT_FAILURE: {failed_payment_data}")

        # Você pode implementar aqui:
        # await save_to_dead_letter_queue(failed_payment_data)
        # await notify_admin(failed_payment_data)
        # await update_metrics("permanent_failures", 1)

    except Exception as e:
        logging.error(f"Error handling permanent failure for {payment.correlationId}: {e}")


# Inicializa workers
async def start_workers(num_workers=50):  # Reduzido de 1000 para 50
    tasks = []
    for i in range(num_workers):
        task = asyncio.create_task(worker(f"worker-{i}"))
        tasks.append(task)
    logging.info(f"Started {num_workers} payment workers")
    return tasks


# POST /payments
class Payment:
    async def on_post(self, req, resp):
        try:
            raw = await req.media

            # Validação básica
            if not isinstance(raw, dict):
                raise ValueError("Request body must be JSON object")

            if "correlationId" not in raw or "amount" not in raw:
                raise ValueError("Missing required fields: correlationId, amount")

            if not isinstance(raw["amount"], (int, float)) or raw["amount"] <= 0:
                raise ValueError("Amount must be positive number")

            payment = PaymentModel(
                correlationId=str(raw["correlationId"]),
                amount=float(raw["amount"]),
                max_retries=int(raw.get("maxRetries", 3))  # Permite customizar retries por request
            )

            # Adiciona à fila de forma não-bloqueante
            try:
                payments_queue.put_nowait(payment)
            except asyncio.QueueFull:
                resp.status = falcon.HTTP_503
                resp.media = {"error": "Payment queue is full, try again later"}
                return

        except (ValueError, TypeError, KeyError) as e:
            resp.status = falcon.HTTP_400
            resp.media = {"error": "Invalid request body", "details": str(e)}
            return
        except Exception as e:
            logging.error(f"Unexpected error in payment endpoint: {e}")
            resp.status = falcon.HTTP_500
            resp.media = {"error": "Internal server error"}
            return

        resp.status = falcon.HTTP_202
        resp.media = {"status": "queued", "queueSize": payments_queue.qsize()}


# GET /payments-summary?from=...&to=... (COM BINARY SEARCH OTIMIZADA)
class PaymentSummaryBinarySearch:
    def __init__(self):
        self._cache = {}
        self._cache_max_size = 1000

    def _get_range_bounds(self, timestamps: List[datetime], from_dt: Optional[datetime], to_dt: Optional[datetime]) -> \
    Tuple[int, int]:
        """Encontra os índices de início e fim usando busca binária"""
        if not timestamps:
            return 0, 0

        # Busca binária para início
        if from_dt is None:
            start_idx = 0
        else:
            start_idx = bisect.bisect_left(timestamps, from_dt)

        # Busca binária para fim
        if to_dt is None:
            end_idx = len(timestamps)
        else:
            end_idx = bisect.bisect_right(timestamps, to_dt)

        return max(0, start_idx), min(len(timestamps), end_idx)

    def _calculate_range_sum(self, cumulative: List[float], start_idx: int, end_idx: int) -> float:
        """Calcula soma do range usando array cumulativo em O(1)"""
        if start_idx >= end_idx or not cumulative:
            return 0.0

        end_sum = cumulative[end_idx - 1]
        start_sum = cumulative[start_idx - 1] if start_idx > 0 else 0.0

        return end_sum - start_sum

    async def on_get(self, req, resp):
        try:
            # Parse dos parâmetros
            from_ts = req.get_param("from")
            to_ts = req.get_param("to")

            # Conversão de timestamps
            from_dt = None
            to_dt = None

            if from_ts:
                from_dt = datetime.fromisoformat(from_ts.replace("Z", "+00:00")).astimezone(timezone.utc)
            if to_ts:
                to_dt = datetime.fromisoformat(to_ts.replace("Z", "+00:00")).astimezone(timezone.utc)

            # Validação de range
            if from_dt and to_dt and from_dt > to_dt:
                resp.status = falcon.HTTP_400
                resp.media = {"error": "from_date cannot be greater than to_date"}
                return

            # Cache key
            cache_key = f"{from_ts or 'None'}_{to_ts or 'None'}"

            # Força rebuild dos índices para garantir dados atuais
            optimized_storage.force_rebuild_all()

            summary = {}

            # Processa cada categoria com binary search
            for key in ["default", "fallback"]:
                data = optimized_storage.get_data(key)
                timestamps = data["timestamps"]
                cumulative = data["cumulative"]

                if not timestamps:
                    summary[key] = {
                        "totalRequests": 0,
                        "totalAmount": 0.0
                    }
                    continue

                # Sem filtros - usa dados pré-calculados
                if from_dt is None and to_dt is None:
                    total_requests = data["total_count"]
                    total_amount = cumulative[-1] if cumulative else 0.0
                else:
                    # Binary search para range
                    start_idx, end_idx = self._get_range_bounds(timestamps, from_dt, to_dt)
                    total_requests = max(0, end_idx - start_idx)
                    total_amount = self._calculate_range_sum(cumulative, start_idx, end_idx)

                summary[key] = {
                    "totalRequests": total_requests,
                    "totalAmount": round(float(total_amount), 2)  # Arredonda para 2 casas decimais
                }

            # Adiciona metadata útil
            summary["_metadata"] = {
                "totalSuccessfulPayments": sum(data["total_count"] for data in [optimized_storage.get_data("default"),
                                                                                optimized_storage.get_data(
                                                                                    "fallback")]),
                "queueSize": payments_queue.qsize(),
                "queriedAt": datetime.now(timezone.utc).isoformat()
            }

            resp.status = falcon.HTTP_200
            resp.media = summary

        except ValueError as e:
            resp.status = falcon.HTTP_400
            resp.media = {"error": f"Invalid date format: {str(e)}"}
        except Exception as e:
            logging.error(f"Error in payment summary: {e}")
            resp.status = falcon.HTTP_500
            resp.media = {"error": "Internal server error"}


# Health check endpoint
class HealthCheck:
    async def on_get(self, req, resp):
        global PAYMENT_ENDPOINT

        health_data = {
            "status": "healthy",
            "currentEndpoint": PAYMENT_ENDPOINT[1] if isinstance(PAYMENT_ENDPOINT, tuple) else "unknown",
            "queueSize": payments_queue.qsize(),
            "successfulPaymentCounts": {
                "default": optimized_storage.get_data("default")["total_count"],
                "fallback": optimized_storage.get_data("fallback")["total_count"]
            },
            "retryMetrics": {
                "totalRetries": retry_metrics["total_retries"],
                "retrySuccesses": retry_metrics["retry_success"],
                "permanentFailures": retry_metrics["permanent_failures"],
                "retrySuccessRate": round(retry_metrics["retry_success"] / max(retry_metrics["total_retries"], 1) * 100,
                                          2)
            },
            "timestamp": datetime.now(timezone.utc).isoformat()
        }

        resp.status = falcon.HTTP_200
        resp.media = health_data


# Inicializa o app Falcon
base_app = falcon.asgi.App(
    cors_enable=True,  # Enable CORS se necessário
    media_type=falcon.MEDIA_JSON
)

base_app.add_route('/payments', Payment())
base_app.add_route('/payments-summary', PaymentSummaryBinarySearch())
base_app.add_route('/health', HealthCheck())


# Lifespan wrapper otimizado para tasks de background
class Lifespan:
    def __init__(self, app):
        self.app = app
        self.tasks_started = False
        self.background_tasks = []

    async def __call__(self, scope, receive, send):
        if scope["type"] == "lifespan":
            message = await receive()
            if message["type"] == "lifespan.startup" and not self.tasks_started:
                self.tasks_started = True

                # Inicia tasks de background
                endpoint_task = asyncio.create_task(set_endpoint_loop())
                worker_tasks = await start_workers(num_workers=50)

                self.background_tasks = [endpoint_task] + worker_tasks

                logging.info("Background tasks started successfully")
                await send({"type": "lifespan.startup.complete"})

            elif message["type"] == "lifespan.shutdown":
                # Graceful shutdown
                logging.info("Shutting down background tasks...")

                for task in self.background_tasks:
                    task.cancel()

                # Aguarda tasks terminarem
                await asyncio.gather(*self.background_tasks, return_exceptions=True)

                logging.info("Background tasks shut down complete")
                await send({"type": "lifespan.shutdown.complete"})
        else:
            await self.app(scope, receive, send)


app = Lifespan(base_app)

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # Configurações otimizadas do uvicorn
    uvicorn.run(
        "app:app",
        host="127.0.0.1",
        port=9999,
        reload=False,  # Desabilitado para produção
        workers=1,  # ASGI app já é async
        loop="uvloop",  # Loop mais rápido (pip install uvloop)
        access_log=False  # Desabilita access log para performance
    )