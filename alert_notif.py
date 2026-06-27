import os
import json
import smtplib
from datetime import datetime, timedelta
from kafka import KafkaConsumer
from clickhouse_driver import Client
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from pathlib import Path

KAFKA_BROKER = 'kafka:29092'
TOPIC_NAME = "erp.public.nilai_persediaan"
KAFKA_GROUP_ID = "alert-engine-group"

consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=[KAFKA_BROKER],
    group_id=KAFKA_GROUP_ID,
    auto_offset_reset='latest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

ch_client = Client(
    host=os.getenv("CLICKHOUSE_HOST", "clickhouse"), 
    user='clickhouseadmin', 
    password='clickhouseadmin'
)

JSON_CONFIG_PATH = "config_email.json"

with open(JSON_CONFIG_PATH, "r") as file:
    config_data = json.load(file)

SMTP_SERVER     = config_data["SMTP_SERVER"]
SMTP_PORT       = int(config_data["SMTP_PORT"])
SMTP_USER       = config_data["SMTP_USER"]
SMTP_PASSWORD   = config_data["SMTP_PASSWORD"]
SENDER_EMAIL    = config_data["SENDER_EMAIL"]
ALERT_RECEIVER = config_data["RECEIVER_EMAIL"]

ALERT_COOLDOWN_CACHE = {}
COOLDOWN_DURATION = timedelta(hours=6)

SEED_DIR = Path("seed_data")

def _load(filename: str) -> dict:
    with open(SEED_DIR / filename, "r", encoding="utf-8") as f:
        return json.load(f)

ITEM_SCM_SETTINGS = _load("items.json")

def get_branch_name(id_cabang):
    """Mendapatkan nama cabang dari database master ClickHouse"""
    try:
        q_branch = f"SELECT nama_cabang FROM default.dim_cabang WHERE id_cabang = {id_cabang} LIMIT 1"
        res = ch_client.execute(q_branch)
        return res[0][0] if res else f"Cabang ID-{id_cabang}"
    except Exception:
        return f"Cabang ID-{id_cabang}"

def get_item_name_db(id_item):
    """Mendapatkan nama item mentah dari database untuk dicocokkan ke ITEM_SCM_SETTINGS"""
    try:
        q_item = f"SELECT nama_item FROM default.dim_item WHERE id_item = {id_item} LIMIT 1"
        res = ch_client.execute(q_item)
        return res[0][0] if res else None
    except Exception:
        return None

def send_email_alert(nama_cabang, nama_item, info_scm, stok_akhir):
    """Mengirim email HTML informatif berdasarkan data riil dan target restock"""
    now = datetime.now()
    subject = f"⚠️ [CRITICAL STOCK ALERT] - {nama_cabang}: {nama_item}"
    
    body = f"""
    <h3>🚨 Peringatan Batas Minimum Stok SCM 🚨</h3>
    <hr>
    <p>Halo Tim Purchasing & Gudang,</p>
    <p>Sistem streaming mendeteksi adanya mutasi keluar yang menyebabkan stok item berikut berada di bawah batas aman:</p>
    
    <table border="1" cellpadding="8" style="border-collapse: collapse; border-color: #cbd5e1; font-family: Arial, sans-serif;">
        <tr style="background-color: #f1f5f9;">
            <td><b>Unit / Cabang</b></td>
            <td>{nama_cabang}</td>
        </tr>
        <tr>
            <td><b>Nama Bahan Baku</b></td>
            <td>{nama_item}</td>
        </tr>
        <tr>
            <td><b>Kategori</b></td>
            <td>{info_scm['kategori']}</td>
        </tr>
        <tr style="color: #ef4444; font-weight: bold; background-color: #fef2f2;">
            <td><b>Stok Riil Saat Ini</b></td>
            <td>{stok_akhir:,} {info_scm['satuan']}</td>
        </tr>
    </table>
    
    <p><b>Catatan Logistik:</b> Item ini berstatus <i>Perishable (Mudah Rusak)</i>: <b>{'YA' if info_scm['perishable'] else 'TIDAK'}</b>. Mohon sesuaikan penanganan pengiriman armada logistik.</p>
    <br>
    <p><i>Sistem Automasi Alerting SCM Ingestion - {now.strftime('%Y-%m-%d %H:%M:%S')}</i></p>
    """

    try:
        msg = MIMEMultipart()
        # CRITICAL CHANGE HERE: Gunakan SENDER_EMAIL untuk header 'From'
        msg['From'] = SENDER_EMAIL 
        msg['To'] = ALERT_RECEIVER
        msg['Subject'] = subject
        msg.attach(MIMEText(body, 'html'))  

        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            # Login tetap menggunakan username resmi dari Brevo
            server.login(SMTP_USER, SMTP_PASSWORD) 
            server.sendmail(SENDER_EMAIL, ALERT_RECEIVER, msg.as_string())
            
        print(f"📧 [SUCCESS] Email terkirim ke {ALERT_RECEIVER}: {nama_cabang} - {nama_item}")
    except Exception as e:
        print(f"❌ [SMTP Error] Gagal mengirim email: {str(e)}")

print(f"🚀 Alerting Worker berbasis Item Settings Aktif. Mendengarkan '{TOPIC_NAME}'...")

for message in consumer:
    try:
        payload = message.value
        data = payload.get('after', payload)
        
        if not data:
            continue
            
        id_cabang = int(data['id_cabang'])
        id_item = int(data['id_item'])
        stok_akhir = float(data['quantity_akhir'])
        
        # 1. Ambil nama asli item dari database master
        nama_item_raw = get_item_name_db(id_item)
        if not nama_item_raw:
            continue
            
        nama_item_upper = nama_item_raw.upper().strip()
        
        # 2. Validasi: Apakah item terdaftar di kamus ITEM_SCM_SETTINGS?
        if nama_item_upper in ITEM_SCM_SETTINGS:
            info_scm = ITEM_SCM_SETTINGS[nama_item_upper]
            threshold_trigger = float(info_scm['trigger'])
            
            # 3. Bandingkan dengan parameter trigger eksak
            if stok_akhir < threshold_trigger:
                nama_cabang = get_branch_name(id_cabang)
                now = datetime.now()
                cache_key = (nama_cabang, nama_item_upper)
                
                # 4. Mekanisme Cooldown Cache (Mencegah banjir email akibat hantaman CDC)
                if cache_key in ALERT_COOLDOWN_CACHE:
                    last_sent = ALERT_COOLDOWN_CACHE[cache_key]
                    if now - last_sent < COOLDOWN_DURATION:
                        continue # Lewati jika belum melewati 6 jam
                
                # Eksekusi pengiriman alert jika lolos seluruh validasi
                send_email_alert(nama_cabang, nama_item_upper, info_scm, stok_akhir)
                ALERT_COOLDOWN_CACHE[cache_key] = now
                
    except Exception as e:
        print(f"❌ [Worker Error] Gagal memproses data log stream: {str(e)}")