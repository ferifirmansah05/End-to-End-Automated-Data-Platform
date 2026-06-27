import os
from datetime import datetime
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Date, event, text, FetchedValue
from sqlalchemy.sql.ddl import DDL
from sqlalchemy.orm import declarative_base, sessionmaker

DATABASE_URL = "postgresql://postgres:password@postgres:5432/erp"

Base = declarative_base()
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

class DailySequence(Base):
    __tablename__ = 'daily_sequences'
    seq_name = Column(String(50), primary_key=True)
    seq_date = Column(Date, primary_key=True)
    current_val = Column(Integer, default=1, nullable=False)

class Branch(Base):
    __tablename__ = 'branches'
    id_cabang = Column(Integer, primary_key=True, autoincrement=True)
    nama_cabang = Column(String(100), nullable=False)
    kota = Column(String(100), nullable=False)
    alamat = Column(String(255), nullable=False)

class ProductMenu(Base):
    __tablename__ = 'menus'
    id_menu = Column(Integer, primary_key=True, autoincrement=True)
    nama_menu = Column(String(100), nullable=False)
    kategori = Column(String(50), nullable=False)
    harga_jual = Column(Float, nullable=False)
    kategori_channel = Column(String(20), nullable=False)

class BillOfMaterial(Base):
    __tablename__ = 'recipes_bom'
    id_bom = Column(Integer, primary_key=True, autoincrement=True)
    id_menu = Column(Integer, nullable=False) 
    nama_menu = Column(String(100), nullable=False)
    id_item = Column(Integer, nullable=False) 
    nama_item = Column(String(100), nullable=False)
    quantity = Column(Float, nullable=False) 
    satuan = Column(String(20), nullable=False)

class ItemBahanBaku(Base):
    __tablename__ = 'items'
    id_item = Column(Integer, primary_key=True, autoincrement=True)
    nama_item = Column(String(100), nullable=False)
    satuan = Column(String(20), nullable=False)

class Sales(Base):
    __tablename__ = 'sales'
    id = Column(Integer, primary_key=True, autoincrement=True)
    id_transaksi = Column(String(30), FetchedValue()) 
    datetime = Column(DateTime, server_default=text("CURRENT_TIMESTAMP"))
    id_cabang = Column(Integer, nullable=False)
    nama_cabang = Column(String(100), nullable=False)
    kota = Column(String(100), nullable=False)
    alamat = Column(String(255), nullable=False)
    id_menu = Column(Integer, nullable=False)
    nama_menu = Column(String(100), nullable=False)
    kategori_menu = Column(String(50), nullable=False)
    qty = Column(Integer, nullable=False)
    subtotal = Column(Integer, nullable=False)
    waktu_transaksi = Column(String(20), nullable=False)
    kategori_payment = Column(String(30), nullable=False)

# 💡 UPDATE TRIGGER SALES: Menggunakan Upsert Row-Locking pada daily_sequences
create_sales_func_ddl = DDL("""
CREATE OR REPLACE FUNCTION generate_sales_trx_id()
RETURNS TRIGGER AS $$
DECLARE
    v_daily_seq INT;
BEGIN
    INSERT INTO daily_sequences (seq_name, seq_date, current_val)
    VALUES ('sales', NEW.datetime::date, 1)
    ON CONFLICT (seq_name, seq_date)
    DO UPDATE SET current_val = daily_sequences.current_val + 1
    RETURNING current_val INTO v_daily_seq;

    NEW.id_transaksi := 'TRX-' || TO_CHAR(NEW.datetime, 'YYYYMMDD') || '-' || LPAD(v_daily_seq::text, 6, '0');
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
""")

create_sales_trigger_ddl = DDL("""
CREATE OR REPLACE TRIGGER trg_sales_trx_id
    BEFORE INSERT ON sales
    FOR EACH ROW
    EXECUTE FUNCTION generate_sales_trx_id();
""")

event.listen(Sales.__table__, 'after_create', create_sales_func_ddl)
event.listen(Sales.__table__, 'after_create', create_sales_trigger_ddl)

class Purchase(Base):
    __tablename__ = 'purchases'
    id_purchase = Column(Integer, primary_key=True, autoincrement=True)
    datetime = Column(DateTime, default=datetime.utcnow)
    id_cabang = Column(Integer, nullable=False)
    nama_cabang = Column(String(100), nullable=False)
    nomor_po = Column(String(50), FetchedValue()) 
    nama_supplier = Column(String(150), nullable=False)
    kota = Column(String(100), nullable=False)
    alamat = Column(String(255), nullable=False)
    tanggal_dipesan = Column(DateTime, nullable=False)
    tanggal_diterima = Column(DateTime, nullable=True)
    id_item = Column(Integer, nullable=False)
    nama_item = Column(String(100), nullable=False)
    satuan = Column(String(20), nullable=False)
    kuantitas = Column(Float, nullable=False) 
    total_biaya = Column(Float, nullable=False)    
    status = Column(String(30), nullable=False)

# 💡 UPDATE TRIGGER PURCHASE: Menggunakan Upsert Row-Locking pada daily_sequences
create_purchase_func_ddl = DDL("""
CREATE OR REPLACE FUNCTION generate_purchase_po_num()
RETURNS TRIGGER AS $$
DECLARE
    v_daily_seq INT;
BEGIN
    INSERT INTO daily_sequences (seq_name, seq_date, current_val)
    VALUES ('purchase', NEW.tanggal_dipesan::date, 1)
    ON CONFLICT (seq_name, seq_date)
    DO UPDATE SET current_val = daily_sequences.current_val + 1
    RETURNING current_val INTO v_daily_seq;

    NEW.nomor_po := 'PO-' || TO_CHAR(NEW.tanggal_dipesan, 'YYYYMMDD') || '-' || LPAD(v_daily_seq::text, 6, '0');
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
""")

create_purchase_trigger_ddl = DDL("""
CREATE OR REPLACE TRIGGER trg_purchase_po_num
    BEFORE INSERT ON purchases
    FOR EACH ROW
    EXECUTE FUNCTION generate_purchase_po_num();
""")

event.listen(Purchase.__table__, 'after_create', create_purchase_func_ddl)
event.listen(Purchase.__table__, 'after_create', create_purchase_trigger_ddl)

class CostOfMaterial(Base):
    __tablename__ = 'cost_of_material'
    id_com = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(Date, nullable=False) 
    id_cabang = Column(Integer, nullable=False)
    nama_cabang = Column(String(100), nullable=False)
    id_menu = Column(Integer, nullable=False)
    nama_menu = Column(String(150), nullable=False)
    id_item = Column(Integer, nullable=False)
    nama_item = Column(String(100), nullable=False)
    kategori = Column(String(50), nullable=False)
    quantity = Column(Float, nullable=False) 
    harga = Column(Float, nullable=False) 
    total_biaya = Column(Float, nullable=False)
    bom_quantity = Column(Float, nullable=False)
    bom_total_biaya = Column(Float, nullable=False)

class NilaiPersediaan(Base):
    __tablename__ = 'nilai_persediaan'
    id_ledger = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(Date, nullable=False)
    id_cabang = Column(Integer, nullable=False)
    nama_cabang = Column(String(100), nullable=False)
    ref_doc = Column(String(50), nullable=False) 
    id_item = Column(Integer, nullable=False)
    nama_item = Column(String(100), nullable=False)
    quantity_awal = Column(Float, nullable=False) 
    quantity_masuk = Column(Float, nullable=False) 
    total_biaya_masuk = Column(Float, nullable=False)
    quantity_keluar = Column(Float, nullable=False) 
    total_biaya_keluar = Column(Float, nullable=False)
    quantity_akhir = Column(Float, nullable=False) 
    total_biaya_akhir = Column(Float, nullable=False)

def init_db():
    Base.metadata.create_all(bind=engine)