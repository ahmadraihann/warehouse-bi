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
- **Docker Desktop** (Sangat disarankan untuk pengguna Windows)
- **Git**

Jika ingin menjalankan secara manual (Linux/macOS):
- **Python 3.10+**
- **pip**

---

## 🛠️ Persiapan Cepat (Docker)

1. **Clone Repositori**:
   ```bash
   git clone https://github.com/ahmadraihann/warehouse-bi.git
   cd warehouse-bi
   ```

2. **Inisialisasi & Jalankan**:
   ```bash
   docker-compose up airflow-init
   docker-compose up -d
   ```

3. **Buka Airflow**: Akses `http://localhost:8080` (User/Pass: `airflow`).

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

## 🐳 Menjalankan dengan Docker (Rekomendasi)

Cara termudah dan paling stabil untuk menjalankan proyek ini (terutama di Windows) adalah menggunakan Docker.

### 1. Inisialisasi Database Airflow
```bash
docker-compose up airflow-init
```

### 2. Jalankan Seluruh Layanan
```bash
docker-compose up -d
```
*Perintah ini akan menjalankan Postgres (metadata), Airflow Webserver, dan Airflow Scheduler.*

### 3. Akses Airflow UI
Buka browser dan akses: `http://localhost:8080`
- **Username**: `airflow`
- **Password**: `airflow`

Aktifkan DAG `warehouse_inventory_pipeline` untuk mulai menjalankan pipeline secara otomatis.

---

## 🛠️ Opsi 2: Setup Manual (Tanpa Docker)

*Gunakan opsi ini hanya jika Anda berada di Linux/macOS atau menggunakan WSL2 di Windows.*

### 1. Clone & Virtual Environment
```bash
git clone https://github.com/ahmadraihann/warehouse-bi.git
cd warehouse-bi
python -m venv venv
# Aktifkan venv
```

### 2. Instal Dependensi
```bash
pip install -r requirements.txt
```

### 3. Eksekusi Manual
- **Ingestion**: `python syntetic_data_generation/pipeline.py --fresh`
- **dbt Run**: `cd dbt_transformasi_dan_pemodelan_data && dbt run`
- **dbt Test**: `dbt test`

---

## 📂 Struktur Proyek

- `dags/`: Definisi DAG Airflow.
- `dbt_transformasi_dan_pemodelan_data/`: Proyek inti dbt (model, pengujian, profil).
- `syntetic_data_generation/`: Script ingestion data dan penyimpanan CSV mentah.
- `inventory.duckdb`: Database analitik (dihasilkan setelah proses ingestion).
- `requirements.txt`: Dependensi Python.
