import json
import time
import random
import threading
from datetime import datetime, timedelta
from collections import defaultdict
from pathlib import Path
import requests
from postgre_setup import init_db, SessionLocal, engine, Base, Branch, ProductMenu, ItemBahanBaku, BillOfMaterial, Sales, Purchase, CostOfMaterial, NilaiPersediaan
import pendulum

lokal_tz = pendulum.timezone("Asia/Jakarta")

RESET_DATABASE = True
#START_DATETIME = datetime(2026, 6, 1, 7, 0, 0)
START_DATETIME = datetime.now(lokal_tz)
TIME_SPEEDUP_FACTOR = 24

VIRTUAL_CLOCK_MODIFIER = 0
clock_lock = threading.Lock()
ENABLE_NIGHT_TIMESKIP = False

START_REAL_TIME = time.time()


EOD_TRACKER = defaultdict(set) 
TOTAL_BRANCHES = 0         

API_GATEWAY_URL = "http://ingestion:8000/api/v1"

SEED_DIR = Path("seed_data")

def _load(filename: str) -> dict:
    with open(SEED_DIR / filename, "r", encoding="utf-8") as f:
        return json.load(f)

BRANCH_DATA      = _load("branches.json")
ITEM_SCM_SETTINGS = _load("items.json")
SUPPLIER_DATA    = _load("suppliers.json")
BASE_MENU_DATA   = _load("menus.json")
BOM_RECIPES      = _load("bom_recipes.json")
BOM_RECIPES = {
    menu: [tuple(pair) for pair in ingredients]
    for menu, ingredients in BOM_RECIPES.items()
}

def push_to_api(endpoint: str, payload: dict):
    try:
        url = f"{API_GATEWAY_URL}/{endpoint}"
        requests.post(url, json=payload, timeout=1.5)
    except Exception as e:
        print(f"⚠️  [Pipeline Warning] Gagal streaming ke API Bridge: {str(e)}")

def get_simulated_datetime():
    global VIRTUAL_CLOCK_MODIFIER
    elapsed_real_seconds = time.time() - START_REAL_TIME
    elapsed_virtual_seconds = (elapsed_real_seconds * TIME_SPEEDUP_FACTOR) + VIRTUAL_CLOCK_MODIFIER
    return START_DATETIME + timedelta(seconds=elapsed_virtual_seconds)

def get_supplier_details(item_name, branch_city):
    city = branch_city.upper()
    item_key = item_name.upper()

    if "AYAM" in item_key:
        group = "AYAM"
    elif "SUSU" in item_key:
        group = "SUSU"
    elif "KOPI" in item_key:
        group = "KOPI"
    elif "MATCHA" in item_key:
        group = "MATCHA"
    elif "BERAS" in item_key:
        group = "BERAS"
    elif ITEM_SCM_SETTINGS[item_name]["perishable"]:
        group = "FRESH_FOOD"
    else:
        group = "GROCERY"

    pool = SUPPLIER_DATA.get(group, SUPPLIER_DATA["GROCERY"])
    return pool.get(city, pool.get("DEFAULT"))

def seed_data_master(db):
    if db.query(Branch).count() == 0:
        db.add_all([Branch(nama_cabang=name.upper(), kota=meta["kota"].upper(), alamat=meta["alamat"].upper()) for name, meta in BRANCH_DATA.items()])
        db.commit()

    if db.query(ItemBahanBaku).count() == 0:
        db.add_all([ItemBahanBaku(nama_item=name.upper(), satuan=meta["satuan"].upper()) for name, meta in ITEM_SCM_SETTINGS.items()])
        db.commit()

    if db.query(ProductMenu).count() == 0:
        menus = []
        for menu_name, meta in BASE_MENU_DATA.items():
            menus.append(ProductMenu(
                nama_menu=menu_name.upper(),
                kategori=meta["kategori"].upper(),
                harga_jual=meta["harga_offline"],
                kategori_channel="MASTER"
            ))
        db.add_all(menus)
        db.commit()
        print(f"✅ [SEED SYSTEM] Berhasil mendaftarkan {len(menus)} item ProductMenu.")

    if db.query(BillOfMaterial).count() == 0:
        m_dict = defaultdict(list)
        for m in db.query(ProductMenu).all():
            m_dict[m.nama_menu.upper()].append(m)

        i_dict = {i.nama_item.upper(): i for i in db.query(ItemBahanBaku).all()}

        bom_formulas = []
        for menu_name, ingredients in BOM_RECIPES.items():
            menu_key = menu_name.upper()
            if menu_key not in m_dict:
                continue
            menu_obj = m_dict[menu_key][0]
            for item_name, qty in ingredients:
                item_key = item_name.upper()
                if item_key not in i_dict:
                    continue
                item_obj = i_dict[item_key]
                bom_formulas.append(BillOfMaterial(
                    id_menu=menu_obj.id_menu, nama_menu=menu_obj.nama_menu.upper(),
                    id_item=item_obj.id_item, nama_item=item_obj.nama_item.upper(),
                    quantity=qty, satuan=item_obj.satuan.upper()
                ))
        db.add_all(bom_formulas)
        db.commit()

    if db.query(NilaiPersediaan).count() == 0:
        print(f"[SEED] Inisialisasi Stok Awal ke Kartu Persediaan secara Proporsional...")
        branches = db.query(Branch).all()
        items = db.query(ItemBahanBaku).all()
        tgl_pesan_awal = START_DATETIME - timedelta(days=2)
        for b in branches:
            branch_meta = BRANCH_DATA.get(b.nama_cabang.upper())
            weight = branch_meta["sales_weight"] if branch_meta else 1.0

            for item in items:
                scm_meta = ITEM_SCM_SETTINGS.get(item.nama_item)
                if not scm_meta:
                    continue

                qty_awal = int(scm_meta["init_qty"] * weight)
                total_biaya = qty_awal * scm_meta["price"]

                vendor_info = get_supplier_details(item.nama_item, b.kota)

                po_entry = Purchase(
                    datetime=tgl_pesan_awal, tanggal_dipesan=tgl_pesan_awal, tanggal_diterima=START_DATETIME,
                    id_cabang=b.id_cabang, nama_cabang=b.nama_cabang,
                    nama_supplier=vendor_info["nama"],
                    kota=vendor_info["kota"],
                    alamat=vendor_info["alamat"],
                    id_item=item.id_item, nama_item=item.nama_item,
                    satuan=item.satuan, kuantitas=qty_awal, total_biaya=total_biaya,
                    status="DITERIMA"
                )
                db.add(po_entry)
                db.flush()
                db.refresh(po_entry)

                payload_po_seed = {
                    "datetime": po_entry.datetime.isoformat(), "id_cabang": b.id_cabang, "nama_cabang": b.nama_cabang,
                    "nomor_po": po_entry.nomor_po,
                    "nama_supplier": po_entry.nama_supplier,
                    "kota": po_entry.kota,
                    "alamat": po_entry.alamat,
                    "tanggal_dipesan": po_entry.tanggal_dipesan.isoformat(),
                    "tanggal_diterima": po_entry.tanggal_diterima.isoformat() if po_entry.tanggal_diterima else None,
                    "id_item": po_entry.id_item, "nama_item": po_entry.nama_item, "satuan": po_entry.satuan,
                    "kuantitas": float(po_entry.kuantitas), "total_biaya": float(po_entry.total_biaya), "status": "DITERIMA"
                }
                push_to_api("purchases", payload_po_seed)

                db.add(NilaiPersediaan(
                    date=START_DATETIME.date(), id_cabang=b.id_cabang, nama_cabang=b.nama_cabang,
                    ref_doc=po_entry.nomor_po, id_item=item.id_item, nama_item=item.nama_item,
                    quantity_awal=0, quantity_masuk=qty_awal, total_biaya_masuk=total_biaya,
                    quantity_keluar=0, total_biaya_keluar=0, quantity_akhir=qty_awal, total_biaya_akhir=total_biaya
                ))
        db.commit()

def ambil_persediaan_terakhir(db, id_cabang, id_item):
    return db.query(NilaiPersediaan).filter_by(id_cabang=id_cabang, id_item=id_item).order_by(NilaiPersediaan.id_ledger.desc()).first()

def dapatkan_bobot_kategori_dan_jeda(hour, day_of_week, profile, sales_weight):
    is_weekend = day_of_week >= 5
    traffic_multiplier = 1.3 if profile == "OFFICE" and not is_weekend else (1.6 if profile == "LEISURE" and is_weekend else 0.5)
    traffic_multiplier *= sales_weight

    if 7 <= hour < 11:       category_weights, base_sleep = [75, 20, 5],  random.uniform(15.0, 35.0)
    elif 11 <= hour < 14:    category_weights, base_sleep = [30, 10, 60], random.uniform(8.0, 20.0)
    elif 14 <= hour < 18:    category_weights, base_sleep = [60, 35, 5],  random.uniform(40.0, 90.0)
    elif 18 <= hour < 21:    category_weights, base_sleep = [25, 15, 60], random.uniform(20.0, 45.0)
    else:                    category_weights, base_sleep = [40, 40, 20], random.uniform(45.0, 120.0)

    return category_weights, max(0.2, base_sleep / traffic_multiplier)

def generate_data(branch_id, branch_name, branch_city):
    global VIRTUAL_CLOCK_MODIFIER, EOD_TRACKER, TOTAL_BRANCHES
    db = SessionLocal()
    branch_info = BRANCH_DATA[branch_name]
    profile = branch_info["profile"]
    sales_weight = branch_info.get("sales_weight", 1.0)
    menus = db.query(ProductMenu).all()
    items_dict = {i.id_item: i for i in db.query(ItemBahanBaku).all()}

    last_printed_hour = -1
    eod_processed_date = None

    try:
        while True:
            now = get_simulated_datetime()
            today = now.date()
            time_stamp_str = now.strftime('%Y-%m-%d %H:%M:%S')

            pending_purchases = db.query(Purchase).filter(Purchase.id_cabang == branch_id, Purchase.status == "DIPROSES").all()
            for po_in_transit in pending_purchases:
                lead_days = (hash(po_in_transit.nomor_po) % 2) + 1
                waktu_seharusnya_tiba = po_in_transit.tanggal_dipesan + timedelta(days=lead_days)

                if now >= waktu_seharusnya_tiba:
                    po_in_transit.tanggal_diterima = now
                    po_in_transit.status = "DITERIMA"

                    stock_saat_ini = ambil_persediaan_terakhir(db, branch_id, po_in_transit.id_item)
                    q_awal = stock_saat_ini.quantity_akhir if stock_saat_ini else 0
                    b_awal = stock_saat_ini.total_biaya_akhir if stock_saat_ini else 0

                    q_akhir = q_awal + po_in_transit.kuantitas
                    b_akhir = b_awal + po_in_transit.total_biaya

                    np_entry = NilaiPersediaan(
                        date=today, id_cabang=branch_id, nama_cabang=branch_name,
                        ref_doc=po_in_transit.nomor_po, id_item=po_in_transit.id_item, nama_item=po_in_transit.nama_item,
                        quantity_awal=q_awal, quantity_masuk=po_in_transit.kuantitas, total_biaya_masuk=po_in_transit.total_biaya,
                        quantity_keluar=0, total_biaya_keluar=0, quantity_akhir=q_akhir, total_biaya_akhir=b_akhir
                    )
                    db.add(np_entry)
                    db.flush()  # 🛠️ Ambil ID Ledger sebelum commit
                    
                    # 🪵 LOG PENERIMAAN BARANG LOGISTIK 
                    print(f"📦 [{time_stamp_str}] [{branch_name}] LOGISTIK INBOUND -> Ledger ID: {np_entry.id_ledger} | PO: {po_in_transit.nomor_po} | Item: {po_in_transit.nama_item} x{po_in_transit.kuantitas} Diterima.")
                    db.commit()

                    payload_po_received = {
                        "datetime": po_in_transit.datetime.isoformat(), "id_cabang": branch_id, "nama_cabang": branch_name,
                        "nomor_po": po_in_transit.nomor_po,
                        "nama_supplier": po_in_transit.nama_supplier,
                        "kota": po_in_transit.kota,
                        "alamat": po_in_transit.alamat,
                        "tanggal_dipesan": po_in_transit.tanggal_dipesan.isoformat(),
                        "tanggal_diterima": po_in_transit.tanggal_diterima.isoformat(),
                        "id_item": po_in_transit.id_item, "nama_item": po_in_transit.nama_item, "satuan": po_in_transit.satuan,
                        "kuantitas": int(po_in_transit.kuantitas), "total_biaya": float(po_in_transit.total_biaya), "status": "DITERIMA"
                    }
                    push_to_api("purchases", payload_po_received)

                    payload_inventory_in = {
                        "date": str(today), "id_cabang": branch_id, "nama_cabang": branch_name,
                        "ref_doc": po_in_transit.nomor_po, "id_item": po_in_transit.id_item, "nama_item": po_in_transit.nama_item,
                        "quantity_awal": int(q_awal), "quantity_masuk": int(po_in_transit.kuantitas), "total_biaya_masuk": float(po_in_transit.total_biaya),
                        "quantity_keluar": 0, "total_biaya_keluar": 0.0, "quantity_akhir": int(q_akhir), "total_biaya_akhir": float(b_akhir)
                    }
                    push_to_api("nilai-persediaan", payload_inventory_in)

            if not (7 <= now.hour < 23):
                if now.hour == 23 and today != eod_processed_date:
                    print(f"🌙 [{now.strftime('%Y-%m-%d %H:%M')}] [{branch_name}] Memulai Konsolidasi Deviasi Produksi...")
                    
                    com_records = db.query(CostOfMaterial).filter_by(date=today, id_cabang=branch_id).all()
                    com_variance = defaultdict(lambda: {"bom_qty": 0.0, "actual_qty": 0.0})
                    for rec in com_records:
                        com_variance[rec.id_item]["bom_qty"] += rec.bom_quantity
                        com_variance[rec.id_item]["actual_qty"] += rec.quantity
                    
                    for id_item, totals in com_variance.items():
                        item_obj = items_dict[id_item]
                        nama_item = item_obj.nama_item
                        
                        total_bom = totals["bom_qty"]
                        total_actual = totals["actual_qty"]
                        selisih_qty = total_actual - total_bom
                        
                        if selisih_qty == 0:
                            continue
                            
                        stock_terakhir = ambil_persediaan_terakhir(db, branch_id, id_item)
                        q_awal = stock_terakhir.quantity_akhir if stock_terakhir else 0
                        b_awal = stock_terakhir.total_biaya_akhir if stock_terakhir else 0
                        
                        scm_meta = ITEM_SCM_SETTINGS[nama_item]
                        harga_satuan_stok = (b_awal / q_awal) if q_awal > 0 else scm_meta["price"]
                        selisih_biaya = selisih_qty * harga_satuan_stok
                        
                        q_akhir = q_awal - selisih_qty
                        b_akhir = b_awal - selisih_biaya
                        
                        ref_dev = f"DEV-{today.strftime('%Y%m%d')}"
                        
                        np_dev = NilaiPersediaan(
                            date=today, id_cabang=branch_id, nama_cabang=branch_name,
                            ref_doc=ref_dev, id_item=id_item, nama_item=nama_item,
                            quantity_awal=q_awal, quantity_masuk=0, total_biaya_masuk=0,
                            quantity_keluar=selisih_qty, total_biaya_keluar=selisih_biaya,
                            quantity_akhir=q_akhir, total_biaya_akhir=b_akhir
                        )
                        db.add(np_dev)
                        db.flush()  # 🛠️ Ambil ID Ledger Deviasi sebelum EOD
                        
                        # 🪵 LOG PENJURNALAN DEVIASI PRODUKSI (EOD)
                        print(f"⚖️  [{time_stamp_str}] [{branch_name}] JURNAL DEVIASI SCM -> Ledger ID: {np_dev.id_ledger} | Item: {nama_item} | Selisih Fisik: {selisih_qty:.2f} | Nilai Deviasi: Rp {selisih_biaya:,.0f}")
                        
                        payload_inventory_dev = {
                            "date": str(today), "id_cabang": branch_id, "nama_cabang": branch_name,
                            "ref_doc": ref_dev, "id_item": id_item, "nama_item": nama_item,
                            "quantity_awal": float(q_awal), "quantity_masuk": 0.0, "total_biaya_masuk": 0.0,
                            "quantity_keluar": float(selisih_qty), "total_biaya_keluar": float(selisih_biaya),
                            "quantity_akhir": float(q_akhir), "total_biaya_akhir": float(b_akhir)
                        }
                        push_to_api("nilai-persediaan", payload_inventory_dev)
                        
                    db.commit()
                    print(f"✅ [{now.strftime('%Y-%m-%d %H:%M')}] [{branch_name}] Jurnal Deviasi Produksi Berhasil Dibukukan.")
                    eod_processed_date = today

                    # 🟢 KODE SINKRONISASI INSTANT TIMESKIP KE JAM 07:00 BESOK PAGI 🟢
                    with clock_lock:
                        EOD_TRACKER[today].add(branch_id)
                        if len(EOD_TRACKER[today]) == TOTAL_BRANCHES:
                            if ENABLE_NIGHT_TIMESKIP:
                                target_dt = datetime(today.year, today.month, today.day, 7, 0, 0) + timedelta(days=1)
                                seconds_to_advance = (target_dt - now).total_seconds()
                                VIRTUAL_CLOCK_MODIFIER += seconds_to_advance
                                print(f"⏩ [TIMESKIP GLOBAL] Semua ({TOTAL_BRANCHES}) cabang sukses EOD. Melompati malam langsung ke {target_dt.strftime('%Y-%m-%d %H:%M:%S')}")

                if now.hour != last_printed_hour and branch_id == 1:
                    print(f"🌙 [{now.strftime('%Y-%m-%d %H:%M')}] Kafe Tutup Melayani Customer.")
                    last_printed_hour = now.hour
                
                time.sleep(0.01)
                continue

            cat_weights, virtual_sleep_time = dapatkan_bobot_kategori_dan_jeda(now.hour, now.weekday(), profile, sales_weight)
            chosen_cat = random.choices(["BEVERAGE", "SNACK", "FOOD"], weights=cat_weights)[0]

            payment_options = ["CASH", "QRIS", "GOFOOD", "GRABFOOD"]
            payment_weights = [25, 45, 15, 15]
            chosen_payment = random.choices(payment_options, weights=payment_weights)[0]
            chosen_channel = "ONLINE" if chosen_payment in ["GOFOOD", "GRABFOOD"] else "OFFLINE"

            available_menus = [m for m in menus if m.kategori.upper() == chosen_cat]
            if not available_menus:
                continue

            menu = random.choice(available_menus)
            qty_jual = random.choices([1, 2, 3, 4], weights=[70, 20, 8, 2])[0]

            menu_meta = BASE_MENU_DATA.get(menu.nama_menu.upper())
            harga_berlaku = menu_meta["harga_online"] if chosen_channel == "ONLINE" else menu_meta["harga_offline"]
            subtotal = harga_berlaku * qty_jual

            sale_entry = Sales(
                datetime=now, id_cabang=branch_id, nama_cabang=branch_name,
                kota=branch_info["kota"], alamat=branch_info["alamat"],
                id_menu=menu.id_menu, nama_menu=menu.nama_menu, kategori_menu=menu.kategori, qty=qty_jual,
                subtotal=subtotal, waktu_transaksi=now.strftime("%H:%M:%S"), kategori_payment=chosen_payment
            )
            db.add(sale_entry)
            db.flush()  # 🛠️ Ambil ID Transaksi dari PostgreSQL Sequence sebelum dikirim
            
            # 🪵 LOG ENTRY TRANSAKSI PENJUALAN KASIR
            print(f"🛒 [{time_stamp_str}] [{branch_name}] SALES INPUT -> ID Transaksi: {sale_entry.id_transaksi} | Menu: {sale_entry.nama_menu} x{qty_jual} | Subtotal: Rp {subtotal:,.0f} | Channel: {chosen_payment}")
            
            db.commit()
            db.refresh(sale_entry)

            payload_sales = {
                "id_transaksi": sale_entry.id_transaksi, "datetime": sale_entry.datetime.isoformat(),
                "id_cabang": sale_entry.id_cabang, "nama_cabang": sale_entry.nama_cabang,
                "kota": branch_info["kota"], "alamat": branch_info["alamat"],
                "id_menu": sale_entry.id_menu, "nama_menu": sale_entry.nama_menu,
                "kategori_menu": sale_entry.kategori_menu, "qty": sale_entry.qty,
                "subtotal": float(sale_entry.subtotal), "waktu_transaksi": sale_entry.waktu_transaksi,
                "kategori_payment": sale_entry.kategori_payment
            }
            push_to_api("sales", payload_sales)

            bom_ingredients = db.query(BillOfMaterial).filter_by(id_menu=menu.id_menu).all()
            for ingredient in bom_ingredients:
                item_raw = items_dict[ingredient.id_item]
                scm_meta = ITEM_SCM_SETTINGS[item_raw.nama_item]

                qty_bom_total = ingredient.quantity * qty_jual
                deviasi_persen = random.uniform(-0.02, 0.05)
                qty_aktual_total = qty_bom_total * (1 + deviasi_persen)

                stock_terakhir = ambil_persediaan_terakhir(db, branch_id, item_raw.id_item)
                qty_awal = stock_terakhir.quantity_akhir if stock_terakhir else 0
                biaya_awal = stock_terakhir.total_biaya_akhir if stock_terakhir else 0

                harga_satuan_stok = (biaya_awal / qty_awal) if qty_awal > 0 else scm_meta["price"]
                biaya_bom_keluar = qty_bom_total * harga_satuan_stok
                biaya_aktual_keluar = qty_aktual_total * harga_satuan_stok

                qty_akhir = qty_awal - qty_bom_total
                biaya_akhir = biaya_awal - biaya_bom_keluar

                tx_inv = NilaiPersediaan(
                    date=today, id_cabang=branch_id, nama_cabang=branch_name,
                    ref_doc=f"TX-{sale_entry.id_transaksi}", id_item=item_raw.id_item, nama_item=item_raw.nama_item,
                    quantity_awal=qty_awal, quantity_masuk=0, total_biaya_masuk=0,
                    quantity_keluar=qty_bom_total, total_biaya_keluar=biaya_bom_keluar,
                    quantity_akhir=qty_akhir, total_biaya_akhir=biaya_akhir
                )
                db.add(tx_inv)
                db.flush() # 🛠️ Ambil ID Ledger Pengurangan Stok
                
                # 🪵 LOG POTONG STOK TEORITIS (BOM)
                print(f"📉 [{time_stamp_str}] [{branch_name}] INVENTORY DEDUCT -> Ledger ID: {tx_inv.id_ledger} | Ref Doc: TX-{sale_entry.id_transaksi} | Item: {item_raw.nama_item} | Qty Keluar: {qty_bom_total:.4f}")

                payload_inventory_out = {
                    "date": str(today), "id_cabang": branch_id, "nama_cabang": branch_name,
                    "ref_doc": f"TX-{sale_entry.id_transaksi}", "id_item": item_raw.id_item, "nama_item": item_raw.nama_item,
                    "quantity_awal": float(qty_awal), "quantity_masuk": 0.0, "total_biaya_masuk": 0.0,
                    "quantity_keluar": float(qty_bom_total), "total_biaya_keluar": float(biaya_bom_keluar),
                    "quantity_akhir": float(qty_akhir), "total_biaya_akhir": float(biaya_akhir)
                }
                push_to_api("nilai-persediaan", payload_inventory_out)

                com_hari_ini = db.query(CostOfMaterial).filter_by(date=today, id_cabang=branch_id, id_menu=menu.id_menu, id_item=item_raw.id_item).first()
                item_cat = scm_meta.get("kategori", "LAINNYA")

                if com_hari_ini:
                    com_hari_ini.quantity += qty_aktual_total
                    com_hari_ini.total_biaya += biaya_aktual_keluar
                    com_hari_ini.harga = com_hari_ini.total_biaya / com_hari_ini.quantity
                    com_hari_ini.bom_quantity += qty_bom_total
                    com_hari_ini.bom_total_biaya += biaya_bom_keluar
                    db.flush()
                    
                    com_id = getattr(com_hari_ini, 'id_com', getattr(com_hari_ini, 'id', 'N/A'))
                    com_qty = com_hari_ini.quantity
                    com_total = com_hari_ini.total_biaya
                    com_harga = com_hari_ini.harga
                    com_bom_qty = com_hari_ini.bom_quantity
                    com_bom_total = com_hari_ini.bom_total_biaya
                    
                    # 🪵 LOG UPDATE COST OF MATERIAL
                    print(f"📊 [{time_stamp_str}] [{branch_name}] COM UPDATED -> COM ID: {com_id} | Menu: {menu.nama_menu} -> Item: {item_raw.nama_item} | Total Aktual Qty: {com_qty:.2f} | Total Aktual Biaya: Rp {com_total:,.0f}")
                else:
                    new_com = CostOfMaterial(
                        date=today, id_cabang=branch_id, nama_cabang=branch_name,
                        id_menu=menu.id_menu, nama_menu=menu.nama_menu,
                        id_item=item_raw.id_item, nama_item=item_raw.nama_item,
                        kategori=item_cat,
                        quantity=qty_aktual_total, harga=harga_satuan_stok, total_biaya=biaya_aktual_keluar,
                        bom_quantity=qty_bom_total, bom_total_biaya=biaya_bom_keluar
                    )
                    db.add(new_com)
                    db.flush() # 🛠️ Ambil ID Row COM yang baru dibuat
                    
                    com_id = getattr(new_com, 'id_com', getattr(new_com, 'id', 'N/A'))
                    com_qty = qty_aktual_total
                    com_total = biaya_aktual_keluar
                    com_harga = harga_satuan_stok
                    com_bom_qty = qty_bom_total
                    com_bom_total = biaya_bom_keluar
                    
                    # 🪵 LOG NEW COST OF MATERIAL ENTRY
                    print(f"📊 [{time_stamp_str}] [{branch_name}] COM CREATED -> COM ID: {com_id} | Menu: {menu.nama_menu} -> Item: {item_raw.nama_item} | Qty Aktual: {com_qty:.2f} | Biaya: Rp {com_total:,.0f}")

                payload_com = {
                    "date": str(today), "id_cabang": branch_id, "nama_cabang": branch_name,
                    "id_menu": menu.id_menu, "nama_menu": menu.nama_menu,
                    "id_item": item_raw.id_item, "nama_item": item_raw.nama_item,
                    "kategori": item_cat,
                    "quantity": float(com_qty), "harga": float(com_harga), "total_biaya": float(com_total),
                    "bom_quantity": float(com_bom_qty), "bom_total_biaya": float(com_bom_total)
                }
                push_to_api("cost-of-material", payload_com)

                dynamic_trigger = int(scm_meta["trigger"] * sales_weight)
                dynamic_restock_qty = int(scm_meta["restock_qty"] * sales_weight)

                if qty_akhir <= dynamic_trigger:
                    po_menggantung = db.query(Purchase).filter(
                        Purchase.id_cabang == branch_id,
                        Purchase.id_item == item_raw.id_item,
                        Purchase.status == "DIPROSES"
                    ).first()
                    if po_menggantung:
                        continue

                    qty_beli_po = dynamic_restock_qty
                    harga_pasar_saat_ini = scm_meta["price"] * random.uniform(0.97, 1.03) * (1.04 if now.weekday() >= 5 else 1.0)
                    total_biaya_po = qty_beli_po * harga_pasar_saat_ini

                    vendor_info = get_supplier_details(item_raw.nama_item, branch_city)

                    po_entry = Purchase(
                        datetime=now, tanggal_dipesan=now, tanggal_diterima=None,
                        id_cabang=branch_id, nama_cabang=branch_name,
                        nama_supplier=vendor_info["nama"],
                        kota=vendor_info["kota"],
                        alamat=vendor_info["alamat"],
                        id_item=item_raw.id_item, nama_item=item_raw.nama_item,
                        satuan=item_raw.satuan, kuantitas=qty_beli_po, total_biaya=total_biaya_po, status="DIPROSES"
                    )
                    db.add(po_entry)
                    db.flush()
                    db.refresh(po_entry)

                    po_id = getattr(po_entry, 'id_purchase', getattr(po_entry, 'id', 'N/A'))

                    print(f"⚠️  [{time_stamp_str}] [{branch_name}] MANDATE PO ISSUED -> PO ID: {po_id} | No PO: {po_entry.nomor_po} | Item: {item_raw.nama_item} x{qty_beli_po} VIA {vendor_info['nama']}")

                    payload_po_issued = {
                        "datetime": po_entry.datetime.isoformat(), "id_cabang": branch_id, "nama_cabang": branch_name,
                        "nomor_po": po_entry.nomor_po,
                        "nama_supplier": po_entry.nama_supplier,
                        "kota": po_entry.kota,
                        "alamat": po_entry.alamat,
                        "tanggal_dipesan": po_entry.tanggal_dipesan.isoformat(), "tanggal_diterima": None,
                        "id_item": po_entry.id_item, "nama_item": po_entry.nama_item, "satuan": po_entry.satuan,
                        "kuantitas": int(po_entry.kuantitas), "total_biaya": float(po_entry.total_biaya), "status": "DIPROSES"
                    }
                    push_to_api("purchases", payload_po_issued)

            db.commit()
            time.sleep(virtual_sleep_time / TIME_SPEEDUP_FACTOR)

    except Exception as e:
        print(f"❌ [Simulation Error] Cabang {branch_name} berhenti: {str(e)}")
    finally:
        db.close()


def main():
    global TOTAL_BRANCHES
    if RESET_DATABASE:
        print("🔄 [RESET] Menginisialisasi ulang database skema...")
        Base.metadata.drop_all(bind=engine)
        Base.metadata.create_all(bind=engine)

    db = SessionLocal()
    try:
        seed_data_master(db)
        branches = db.query(Branch).all()
        TOTAL_BRANCHES = len(branches)
    finally:
        db.close()

    print(f"🚀 Memulai generate data untuk {len(branches)} cabang...")
    threads = []
    for b in branches:
        t = threading.Thread(target=generate_data, args=(b.id_cabang, b.nama_cabang, b.kota))
        t.daemon = True
        threads.append(t)
        t.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n🛑 Generate data dihentikan.")

if __name__ == '__main__':
    main()