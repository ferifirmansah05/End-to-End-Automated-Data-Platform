CREATE TABLE IF NOT EXISTS default.dim_cabang (
    id_cabang UInt32,
    nama_cabang String,
    kota String,
    alamat String
) ENGINE = ReplacingMergeTree()
ORDER BY id_cabang;

CREATE TABLE IF NOT EXISTS default.dim_menu (
    id_menu UInt32,
    nama_menu String,
    kategori String
) ENGINE = ReplacingMergeTree()
ORDER BY id_menu;

CREATE TABLE IF NOT EXISTS default.dim_item (
    id_item UInt32,
    nama_item String,
    satuan String,
    kategori String
) ENGINE = ReplacingMergeTree()
ORDER BY id_item;

CREATE TABLE IF NOT EXISTS default.dim_supplier (
    id_supplier UInt32,
    nama_supplier String,
    kota String,
    alamat String
) ENGINE = ReplacingMergeTree()
ORDER BY id_supplier;

CREATE TABLE IF NOT EXISTS default.fact_sales (
    id_transaksi String,
    tanggal_transaksi Date,
    id_cabang UInt32,
    id_menu UInt32,
    qty UInt32,
    subtotal Float64,
    waktu_transaksi String,
    kategori_payment String
) ENGINE = MergeTree()
ORDER BY (tanggal_transaksi, id_cabang, id_menu);

CREATE TABLE IF NOT EXISTS default.fact_purchases (
    tanggal_dipesan DateTime,
    tanggal_diterima Nullable(DateTime),
    id_cabang UInt32,
    nomor_po String,
    id_supplier UInt32,
    id_item UInt32,
    kuantitas Float64,
    total_biaya Float64,
    status String
) ENGINE = ReplacingMergeTree()
ORDER BY (id_cabang, id_item, nomor_po);

CREATE TABLE IF NOT EXISTS default.fact_cost_of_material (
    tanggal Date,
    id_cabang UInt32,
    id_menu UInt32,
    id_item UInt32,
    quantity Float64,
    harga Float64,
    total_biaya Float64,
    bom_quantity Float64,
    bom_total Float64
) ENGINE = ReplacingMergeTree()
ORDER BY (tanggal, id_cabang, id_menu, id_item);

CREATE TABLE IF NOT EXISTS default.fact_nilai_persediaan (
    tanggal Date,
    id_cabang UInt32,
    ref_doc String,
    id_item UInt32,
    quantity_awal Float64,
    quantity_masuk Float64,
    total_biaya_masuk Float64,
    quantity_keluar Float64,
    total_biaya_keluar Float64,
    quantity_akhir Float64,
    total_biaya_akhir Float64
) ENGINE = MergeTree()
ORDER BY (tanggal, id_cabang, ref_doc, id_item);

CREATE TABLE default.dm_sales_hourly (
    tanggal_transaksi Date,
    jam String,
    id_cabang UInt32,
    kategori_payment String,
    id_menu UInt32,
    revenue SimpleAggregateFunction(sum, Float64),
    trx_uniq AggregateFunction(uniq, String)
) ENGINE = AggregatingMergeTree()
ORDER BY (tanggal_transaksi, jam, id_cabang, kategori_payment, id_menu);

CREATE MATERIALIZED VIEW default.mv_sales_hourly TO default.dm_sales_hourly AS
SELECT
    toDate(tanggal_transaksi) AS tanggal_transaksi,
    concat(substring(waktu_transaksi, 1, 2), ':00') AS jam,
    id_cabang,
    kategori_payment,
    id_menu,
    sum(subtotal) AS revenue,
    uniqState(toString(id_transaksi)) AS trx_uniq
FROM default.fact_sales
GROUP BY tanggal_transaksi, jam, id_cabang, kategori_payment, id_menu;

CREATE TABLE default.dm_inventory_daily (
    tanggal Date,
    id_cabang UInt32,
    id_item UInt32,
    nilai_stok SimpleAggregateFunction(sum, Float64),
    qty_masuk SimpleAggregateFunction(sum, Float64),
    qty_keluar SimpleAggregateFunction(sum, Float64),
    qty_akhir AggregateFunction(argMax, Float64, Date)
) ENGINE = AggregatingMergeTree()
ORDER BY (tanggal, id_cabang, id_item);

CREATE MATERIALIZED VIEW default.mv_inventory_daily TO default.dm_inventory_daily AS
SELECT
    toDate(tanggal) AS tanggal,
    id_cabang,
    id_item,
    sum(total_biaya_akhir) AS nilai_stok,
    sum(quantity_masuk) AS qty_masuk,
    sum(quantity_keluar) AS qty_keluar,
    argMaxState(quantity_akhir, toDate(tanggal)) AS qty_akhir
FROM default.fact_nilai_persediaan
GROUP BY tanggal, id_cabang, id_item;

CREATE TABLE default.dm_outlet_daily (
    tanggal Date,
    id_cabang UInt32,
    id_item UInt32,
    id_menu UInt32,
    actual_cost SimpleAggregateFunction(sum, Float64),
    bom_cost SimpleAggregateFunction(sum, Float64)
) ENGINE = SummingMergeTree()
ORDER BY (tanggal, id_cabang, id_item, id_menu);

CREATE MATERIALIZED VIEW default.mv_outlet_daily TO default.dm_outlet_daily AS
SELECT
    toDate(tanggal) AS tanggal,
    id_cabang,
    id_item,
    id_menu,
    sum(total_biaya) AS actual_cost,
    sum(bom_total) AS bom_cost
FROM default.fact_cost_of_material
GROUP BY tanggal, id_cabang, id_item, id_menu;

CREATE TABLE default.dm_purchase_daily (
    tanggal_dipesan Date,
    id_cabang UInt32,
    id_supplier UInt32,
    id_item UInt32,
    status String,
    total_spend SimpleAggregateFunction(sum, Float64),
    kuantitas SimpleAggregateFunction(sum, Float64),
    po_uniq AggregateFunction(uniq, String),
    lead_time_days AggregateFunction(avg, Float64)
) ENGINE = AggregatingMergeTree()
ORDER BY (tanggal_dipesan, id_cabang, id_supplier, id_item, status);

CREATE MATERIALIZED VIEW default.mv_purchase_daily TO default.dm_purchase_daily AS
SELECT
    toDate(tanggal_dipesan) AS tanggal_dipesan,
    id_cabang,
    id_supplier,
    id_item,
    status,
    sum(total_biaya) AS total_spend,
    sum(kuantitas) AS kuantitas,
    uniqState(toString(nomor_po)) AS po_uniq,
    avgState(
            multiIf(
                isNull(tanggal_diterima), 0.0,
                cast(dateDiff('day', toDate(tanggal_dipesan), toDate(tanggal_diterima)), 'Float64')
            )
        ) AS lead_time_days
FROM default.fact_purchases
GROUP BY tanggal_dipesan, id_cabang, id_supplier, id_item, status;