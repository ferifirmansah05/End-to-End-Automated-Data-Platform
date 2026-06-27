import os
import json
import time
import pandas as pd
from datetime import datetime
from kafka import KafkaConsumer
from clickhouse_driver import Client

RESET_CLICKHOUSE_TABLES = True  
SQL_SETUP_FILE = 'clickhouse_setup.sql'
KAFKA_BROKER = 'kafka:29092'
KAFKA_GROUP_ID = 'etl-clickhouse-group'
CLICKHOUSE_HOST = 'clickhouse'
TARGET_TZ = 'Asia/Jakarta'

KAFKA_TOPICS = [
    'erp.public.sales',
    'erp.public.purchases',
    'erp.public.cost_of_material',
    'erp.public.nilai_persediaan'
]

ch_client = Client(
    host=CLICKHOUSE_HOST, 
    user='clickhouseadmin', 
    password='clickhouseadmin'
)

def run_clickhouse_setup():
    """Membaca file SQL eksternal dan mengeksekusi DDL di ClickHouse"""
    print(f"🏗️  Mengeksekusi skema arsitektur Star Schema Baru dari {SQL_SETUP_FILE}...")
    if not os.path.exists(SQL_SETUP_FILE):
        raise FileNotFoundError(f"File {SQL_SETUP_FILE} tidak ditemukan!")
    with open(SQL_SETUP_FILE, 'r') as file:
        sql_content = file.read()
    for query in [q.strip() for q in sql_content.split(';') if q.strip()]:
        ch_client.execute(query)
    print("✅ Inisialisasi Database ClickHouse Berhasil.\n")

def start_etl_stream():
    """Memulai streaming ETL dengan Stateful Caching untuk Eliminasi Duplikat & Pengayaan Mart"""
    consumer = KafkaConsumer(
        bootstrap_servers=[KAFKA_BROKER],
        group_id=KAFKA_GROUP_ID,
        auto_offset_reset='earliest', 
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    consumer.subscribe(KAFKA_TOPICS)
    
    print("🧠 Memuat database state ke In-Memory Cache...")
    
    branch_cache = {}
    for row in ch_client.execute("SELECT id_cabang, nama_cabang, kota FROM default.dim_cabang"):
        branch_cache[row[0]] = {"nama_cabang": row[1], "kota": row[2]}

    existing_menu = {row[0] for row in ch_client.execute("SELECT id_menu FROM default.dim_menu")}
    
    existing_items = {}
    for row in ch_client.execute("SELECT id_item, nama_item, satuan, kategori FROM default.dim_item"):
        existing_items[row[0]] = {"nama_item": row[1], "satuan": row[2], "kategori": row[3]}
        
        
    existing_suppliers = ch_client.execute("SELECT nama_supplier, id_supplier FROM default.dim_supplier")
    supplier_map = {name: idx for name, idx in existing_suppliers}
    max_supplier_id = max(supplier_map.values()) if supplier_map else 0


    for message in consumer:
        try:
            raw_data = message.value
            topic = message.topic
            df = pd.DataFrame([raw_data] if isinstance(raw_data, dict) else raw_data)
            if df.empty:
                continue

            if topic == 'erp.public.sales':
                df['datetime'] = pd.to_datetime(df['datetime'])
                df['tanggal_transaksi'] = df['datetime'].dt.date
                
                id_cabang = int(df['id_cabang'].iloc[0])
                id_menu = int(df['id_menu'].iloc[0])
                nama_cabang = str(df['nama_cabang'].iloc[0])
                kota_cabang = str(df['kota'].iloc[0])
                nama_menu = str(df['nama_menu'].iloc[0])
                qty_sold = int(df['qty'].iloc[0])
                subtotal = float(df['subtotal'].iloc[0])
                tgl = df['tanggal_transaksi'].iloc[0]
                
                if id_cabang not in branch_cache:
                    df_cabang = df[['id_cabang', 'nama_cabang', 'kota', 'alamat']].drop_duplicates()
                    ch_client.execute('INSERT INTO default.dim_cabang (id_cabang, nama_cabang, kota, alamat) VALUES', 
                                      [tuple(x) for x in df_cabang.to_numpy()])
                    branch_cache[id_cabang] = {"nama_cabang": nama_cabang, "kota": kota_cabang}
                
                if id_menu not in existing_menu:
                    kat_menu_col = 'kategori' if 'kategori' in df.columns else 'kategori_menu'
                    kategori_menu = str(df[kat_menu_col].iloc[0]) if kat_menu_col in df.columns else 'General'
                    ch_client.execute('INSERT INTO default.dim_menu (id_menu, nama_menu, kategori) VALUES', 
                                      [(id_menu, nama_menu, kategori_menu)])
                    existing_menu.add(id_menu)
                
                df_fact = df[['id_transaksi', 'tanggal_transaksi', 'id_cabang', 'id_menu', 'qty', 'subtotal', 'waktu_transaksi', 'kategori_payment']]
                ch_client.execute('INSERT INTO default.fact_sales VALUES', [tuple(x) for x in df_fact.to_numpy()])
                
            elif topic == 'erp.public.purchases':
                df['datetime'] = pd.to_datetime(df['datetime'])
                df['tanggal_dipesan'] = pd.to_datetime(df['tanggal_dipesan'])
                
                # Proteksi Berlapis: Konversi kolom tanggal_diterima yang berpotensi NaT/Null (In-Transit)
                df['tanggal_diterima'] = pd.to_datetime(df['tanggal_diterima'], errors='coerce')
                df['tanggal_diterima'] = df['tanggal_diterima']
                # Note: baris .where(..., None) dihapus karena tidak efektif di level dataframe datetime
                
                # 1. Mapping ID Supplier
                supp_kota_col = 'kota' if 'kota' in df.columns else ('kota_supplier' if 'kota_supplier' in df.columns else 'supplier_kota')
                supp_alamat_col = 'alamat' if 'alamat' in df.columns else ('alamat_supplier' if 'alamat_supplier' in df.columns else 'supplier_alamat')
                
                name = str(df['nama_supplier'].iloc[0])
                if name in supplier_map:
                    emp_id = supplier_map[name]
                else:
                    max_supplier_id += 1
                    emp_id = max_supplier_id
                    supplier_map[name] = emp_id
                    ch_client.execute(
                        'INSERT INTO default.dim_supplier (id_supplier, nama_supplier, kota, alamat) VALUES', 
                        [(int(emp_id), name, str(df[supp_kota_col].iloc[0]), str(df[supp_alamat_col].iloc[0]))]
                    )
                df['id_supplier'] = emp_id
                
                # 2. Dimensi Item dari API Purchase
                id_item = int(df['id_item'].iloc[0])
                nama_item = str(df['nama_item'].iloc[0])
                satuan = str(df['satuan'].iloc[0])
                
                if id_item not in existing_items:
                    ch_client.execute('INSERT INTO default.dim_item (id_item, nama_item, satuan, kategori) VALUES', [(id_item, nama_item, satuan, '')])
                    existing_items[id_item] = {"nama_item": nama_item, "satuan": satuan, "kategori": ''}
                else:
                    current = existing_items[id_item]
                    if current["nama_item"] != nama_item or current["satuan"] != satuan:
                        ch_client.execute('INSERT INTO default.dim_item (id_item, nama_item, satuan, kategori) VALUES', [(id_item, nama_item, satuan, current["kategori"])])
                        existing_items[id_item]["nama_item"] = nama_item
                        existing_items[id_item]["satuan"] = satuan
                
                # 3. Ekstraksi dan Sanitasi Data Fact PO (SULAP NaT -> None) 🚀
                df_fact_po = df[['tanggal_dipesan', 'tanggal_diterima', 'id_cabang', 'nomor_po', 'id_supplier', 'id_item', 'kuantitas', 'total_biaya', 'status']]
                
                records_purchases = [
                    (
                        r.tanggal_dipesan.to_pydatetime() if pd.notnull(r.tanggal_dipesan) else None,
                        r.tanggal_diterima.to_pydatetime() if pd.notnull(r.tanggal_diterima) else None,
                        int(r.id_cabang),
                        str(r.nomor_po),
                        int(r.id_supplier),
                        int(r.id_item),
                        float(r.kuantitas),
                        float(r.total_biaya),
                        str(r.status)
                    )
                    for r in df_fact_po.itertuples(index=False)
                ]
                
                ch_client.execute('INSERT INTO default.fact_purchases VALUES', records_purchases)
        
            elif topic == 'erp.public.cost_of_material':
                df['tanggal'] = pd.to_datetime(df['date']).dt.date
                
                id_menu = int(df['id_menu'].iloc[0])
                nama_menu = str(df['nama_menu'].iloc[0])
                id_item = int(df['id_item'].iloc[0])
                nama_item = str(df['nama_item'].iloc[0])
                kategori_item = str(df['kategori'].iloc[0])
                
                # 1. Ambil & Perbarui Kategori Item ke dim_item murni dari API COM
                if id_item in existing_items:
                    current = existing_items[id_item]
                    if current["kategori"] != kategori_item:
                        ch_client.execute('INSERT INTO default.dim_item (id_item, nama_item, satuan, kategori) VALUES', [(id_item, nama_item, current["satuan"], kategori_item)])
                        existing_items[id_item]["kategori"] = kategori_item
                else:
                    ch_client.execute('INSERT INTO default.dim_item (id_item, nama_item, satuan, kategori) VALUES', [(id_item, nama_item, '-', kategori_item)])
                    existing_items[id_item] = {"nama_item": nama_item, "satuan": '-', "kategori": kategori_item}

                if id_menu not in existing_menu:
                    ch_client.execute('INSERT INTO default.dim_menu (id_menu, nama_menu, kategori) VALUES', [(id_menu, nama_menu, 'General')])
                    existing_menu.add(id_menu)

                # 2. Simpan Fact COM Murni (UPDATE: Termasuk kolom denormalisasi nama dan nilai BOM Teoretis) 🚀
                df_fact_com = df[[
                    'tanggal', 'id_cabang', 'id_menu',
                    'id_item', 'quantity', 'harga', 'total_biaya', 
                    'bom_quantity', 'bom_total_biaya'
                ]]
                ch_client.execute('INSERT INTO default.fact_cost_of_material VALUES', [tuple(x) for x in df_fact_com.to_numpy()])
                
            elif topic == 'erp.public.nilai_persediaan':
                df['date'] = pd.to_datetime(df['date']).dt.date
                
                # Simpan Fact Inventory Murni (Mendukung record TX maupun jurnal penyesuaian EOD-DEV)
                df_fact_inv = df[['date', 'id_cabang', 'ref_doc', 'id_item', 'quantity_awal', 'quantity_masuk', 'total_biaya_masuk', 'quantity_keluar', 'total_biaya_keluar', 'quantity_akhir', 'total_biaya_akhir']]
                ch_client.execute('INSERT INTO default.fact_nilai_persediaan VALUES', [tuple(x) for x in df_fact_inv.to_numpy()])
                
        except Exception as e:
            print(f"❌ [ETL Stream Error] Gagal memproses record {topic} stream: {str(e)}")

def main():
    if RESET_CLICKHOUSE_TABLES:
        print("⚠️  [CLEAN RESET] Membersihkan seluruh arsitektur tabel...")
        tables = [
            'default.dm_sales_hourly', 'default.dm_inventory_daily',
            'default.dm_outlet_daily', 'default.dm_purchase_daily',
            'default.fact_nilai_persediaan', 'default.fact_cost_of_material',
            'default.fact_purchases', 'default.fact_sales',
            'default.dim_supplier', 'default.dim_item', 'default.dim_menu', 'default.dim_cabang'
        ]
        views = [
            'default.mv_sales_hourly', 'default.mv_inventory_daily',
            'default.mv_outlet_daily', 'default.mv_purchase_daily'
        ]
        for t in tables: 
            ch_client.execute(f"DROP TABLE IF EXISTS {t}")
        for v in views: 
            ch_client.execute(f"DROP VIEW IF EXISTS {v}")
            
    run_clickhouse_setup()
    start_etl_stream()

if __name__ == '__main__':
    main()