import os
import json
from datetime import datetime
from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel
from aiokafka import AIOKafkaProducer
import asyncio

app = FastAPI(title="ERP Third-Party Real-Time Bridge API", version="2.0")

# Konfigurasi Kafka dari Environment Variables
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")

# Global Kafka Producer Instance
producer = None

@app.on_event("startup")
async def startup_event():
    global producer
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8')
    )
    await producer.start()
    print("🚀 Kafka Producer berhasil dijalankan dan terhubung ke cluster.")

@app.on_event("shutdown")
async def shutdown_event():
    global producer
    if producer:
        await producer.stop()
        print("🛑 Kafka Producer berhasil dimatikan secara aman.")

# ==========================================
# PYDANTIC SCHEMAS (Synchronized with generator.py & models.py)
# ==========================================

class SalesPayload(BaseModel):
    id_transaksi: str
    datetime: str
    id_cabang: int
    nama_cabang: str
    kota: str            # 💡 UPDATE: Kota cabang operasional
    alamat: str          # 💡 UPDATE: Alamat fisik cabang
    id_menu: int
    nama_menu: str
    kategori_menu: str
    qty: int
    subtotal: float
    waktu_transaksi: str
    kategori_payment: str # 💡 UPDATE: Kanal pembayaran (CASH, QRIS, GOFOOD, dll)

class PurchasePayload(BaseModel):
    datetime: str
    id_cabang: int
    nama_cabang: str
    nomor_po: str
    nama_supplier: str
    kota: str    # 💡 UPDATE: Kota basis legalitas vendor
    alamat: str  # 💡 UPDATE: Alamat fisik gudang vendor
    tanggal_dipesan: str
    tanggal_diterima: str | None = None
    id_item: int
    nama_item: str
    satuan: str
    kuantitas: float      # 💡 UPDATE: Diubah ke float untuk mendukung gramasi/desimal volume SCM
    total_biaya: float
    status: str           # 💡 UPDATE: Status pengiriman PO (DIPROSES / DITERIMA)

class CostOfMaterialPayload(BaseModel):
    date: str
    id_cabang: int
    nama_cabang: str
    id_menu: int
    nama_menu: str
    id_item: int
    nama_item: str
    kategori: str         # 💡 UPDATE: Segmentasi bahan baku (BUMBU, RAW MATERIAL, dll)
    quantity: float
    harga: float
    total_biaya: float
    bom_quantity: float
    bom_total_biaya: float

class NilaiPersediaanPayload(BaseModel):
    date: str
    id_cabang: int
    nama_cabang: str
    ref_doc: str
    id_item: int
    nama_item: str
    quantity_awal: float
    quantity_masuk: float
    total_biaya_masuk: float
    quantity_keluar: float
    total_biaya_keluar: float
    quantity_akhir: float
    total_biaya_akhir: float

# ==========================================
# HELPER FUNCTION UNTUK KIRIM DATA KE KAFKA
# ==========================================
async def send_to_kafka(topic: str, payload: dict):
    try:
        await producer.send_and_wait(topic, payload)
    except Exception as e:
        print(f"❌ Gagal mengirim pesan ke Kafka topic {topic}: {str(e)}")

# ==========================================
# API ENDPOINTS
# ==========================================

@app.post("/api/v1/sales", status_code=202)
async def ingest_sales(payload: SalesPayload, background_tasks: BackgroundTasks):
    data = payload.dict()
    background_tasks.add_task(send_to_kafka, "erp.public.sales", data)
    return {"status": "Queued", "topic": "erp.public.sales"}

@app.post("/api/v1/purchases", status_code=202)
async def ingest_purchases(payload: PurchasePayload, background_tasks: BackgroundTasks):
    data = payload.dict()
    background_tasks.add_task(send_to_kafka, "erp.public.purchases", data)
    return {"status": "Queued", "topic": "erp.public.purchases"}

@app.post("/api/v1/cost-of-material", status_code=202)
async def ingest_com(payload: CostOfMaterialPayload, background_tasks: BackgroundTasks):
    data = payload.dict()
    background_tasks.add_task(send_to_kafka, "erp.public.cost_of_material", data)
    return {"status": "Queued", "topic": "erp.public.cost_of_material"}

@app.post("/api/v1/nilai-persediaan", status_code=202)
async def ingest_inventory(payload: NilaiPersediaanPayload, background_tasks: BackgroundTasks):
    data = payload.dict()
    background_tasks.add_task(send_to_kafka, "erp.public.nilai_persediaan", data)
    return {"status": "Queued", "topic": "erp.public.nilai_persediaan"}