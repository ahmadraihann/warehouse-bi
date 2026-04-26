# Warehouse BI - Sistem Manajemen Inventaris

Pipeline Business Intelligence (BI) yang dirancang untuk mengelola dan menganalisis data inventaris gudang. Proyek ini menggunakan **DuckDB** untuk pemrosesan analitik performa tinggi, **dbt** (data build tool) untuk transformasi modular dan pengujian kualitas data, serta **Apache Airflow** untuk orkestrasi *end-to-end*.

## 🚀 Ringkasan Proyek

Sistem ini mengotomatiskan perjalanan data inventaris dari file CSV mentah hingga menjadi *Data Mart* yang bersih, teruji, dan siap untuk dianalisis.

- **Data Ingestion**: Script Python kustom untuk memuat dataset CSV ke dalam database lokal DuckDB.
- **Transformasi**: Model dbt untuk membersihkan, menggabungkan, dan memodelkan data ke dalam dimensi dan fakta.
- **Quality Assurance**: Pengujian dbt otomatis untuk memastikan integritas data (keunikan, pemeriksaan nilai null, validasi relasi).
- **Orkestrasi**: DAG Airflow untuk menjadwalkan dan memantau seluruh pipeline.

---

## 📋 Prasyarat

Pastikan Anda telah menginstal perangkat lunak berikut:
- **Python 3.10+**
- **pip** (manajer paket Python)
- **dbt-duckdb**
- **Apache Airflow** (opsional, untuk orkestrasi)

---

## 🛠️ Persiapan & Instalasi

### 1. Clone Repositori
```bash
git clone https://github.com/ahmadraihann/warehouse-bi.git
cd warehouse-bi
```

### 2. Buat Virtual Environment
```bash
python -m venv venv
# Di Windows:
.\venv\Scripts\activate
# Di Unix atau MacOS:
source venv/bin/activate
```

### 3. Instal Dependensi
Instal kebutuhan inti dan paket dbt/airflow yang diperlukan:
```bash
pip install -r requirements.txt
pip install dbt-duckdb apache-airflow
```

---

## 📊 Persiapan Data

Proyek ini menggunakan dataset dari Kaggle. Anda perlu mengunduhnya dan meletakkannya di direktori yang benar.

### 1. Unduh Dataset
Unduh dataset dari Kaggle:
👉 [Kaggle Dataset URL](https://www.kaggle.com/datasets/bhanupratapbiswas/inventory-analysis-case-study)

### 2. Penempatan File
Ekstrak file dan letakkan di folder `syntetic_data_generation/data/`. File-file berikut **wajib** ada:
- `2017PurchasePricesDec.csv`
- `BegInvFINAL12312016.csv`
- `EndInvFINAL12312016.csv`
- `InvoicePurchases12312016.csv`
- `PurchasesFINAL12312016.csv`
- `SalesFINAL12312016.csv`

Pastikan struktur foldernya terlihat seperti ini:
```
syntetic_data_generation/
└── data/
    ├── 2017PurchasePricesDec.csv
    ├── BegInvFINAL12312016.csv
    └── ... (file lainnya)
```

---

## ⚙️ Menjalankan Pipeline

### Langkah 1: Ingest Data Mentah
Jalankan script ingestion untuk membuat database DuckDB dan memuat tabel mentah:
```bash
python syntetic_data_generation/pipeline.py --fresh
```
*Flag `--fresh` akan menghapus tabel yang ada dan membuatnya kembali.*

### Langkah 2: Transformasi Data dengan dbt
Masuk ke direktori proyek dbt:
```bash
cd dbt_transformasi_dan_pemodelan_data
```

Jalankan model untuk membangun Data Mart Anda:
```bash
dbt run
```

### Langkah 3: Jalankan Pengujian Data (dbt test)
Validasi kualitas data Anda menggunakan framework pengujian dbt:
```bash
dbt test
```
*Ini akan memeriksa konsistensi skema, kunci unik (unique keys), dan nilai yang tidak boleh kosong (non-null).*

---

## 🌪️ Orkestrasi Airflow

Untuk mengotomatiskan seluruh proses (Ingestion -> Transformasi -> Pengujian), gunakan DAG Airflow yang telah disediakan.

### 1. Konfigurasi Airflow
Atur `AIRFLOW_HOME` ke direktori proyek Anda atau lokasi pilihan Anda:
```bash
export AIRFLOW_HOME=$(pwd)
```

### 2. Sesuaikan Path
Buka `dags/warehouse_dag.py` dan pastikan variabel `BASE_DIR` sesuai dengan path proyek lokal Anda:
```python
BASE_DIR = r"c:\path\ke\warehouse-bi-anda"
```

### 3. Jalankan Airflow
Inisialisasi DB dan jalankan webserver/scheduler:
```bash
airflow db init
airflow webserver --port 8080
# Di terminal lain:
airflow scheduler
```

Akses UI di `http://localhost:8080` dan aktifkan DAG `warehouse_inventory_pipeline`.

---

## 📂 Struktur Proyek

- `dags/`: Definisi DAG Airflow.
- `dbt_transformasi_dan_pemodelan_data/`: Proyek inti dbt (model, pengujian, profil).
- `syntetic_data_generation/`: Script ingestion data dan penyimpanan CSV mentah.
- `inventory.duckdb`: Database analitik (dihasilkan setelah proses ingestion).
- `requirements.txt`: Dependensi Python.
