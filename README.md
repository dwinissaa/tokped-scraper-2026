# Tokopedia Scraper 2026 (Sumut Edition) 🚀

Kumpulan skrip otomasi Python untuk ekstraksi data toko dan produk Tokopedia secara masif di wilayah **Sumatera Utara**. Proyek ini dikembangkan untuk kebutuhan pengumpulan data statistik yang terstruktur, cepat, dan mampu menangani ribuan entitas data.

**Last Condition:** 4 March 2026  
**Target Lokasi:** Seluruh Kabupaten/Kota di Sumatera Utara.

---

## 🛠️ Tech Stack
* **Python 3.10+**
* **Utama:** `curl_cffi` (Bypass TLS Fingerprinting), `cloudscraper` (Bypass Cloudflare), `requests`.
* **Data:** `pandas`, `csv`, `json`.
* **UI/UX:** `tqdm` (Progress bar).
* **Concurrency:** `ThreadPoolExecutor` untuk *deep scraping*.

---

## 🌐 [PENTING!!] Solusi Error Timeout (DNS Fix)
Jika muncul error `Resolving timed out`, sesuaikan DNS menjadi **Manual IPv4** agar koneksi lebih stabil:

* **Settings** > **Network & Internet** > **Wi-Fi** > **Hardware properties**
* **DNS server assignment**: Klik **Edit** > Pilih **Manual**
* **IPv4**: **ON**
  1. **Preferred DNS**: `1.1.1.1`
  2. **Alternate DNS**: `8.8.8.8`
* **Save** dan jalankan ulang skrip.

---

## 📂 Struktur & Alur Kerja

Skrip dibagi menjadi 3 tahap utama untuk memastikan kestabilan proses:

### 0. Install Packages
Jalankan perintah ini
```bash
pip install -r requirements.txt
```

### 1. Persiapan Master Data
**File:** `0. Scraping Master Tokped.ipynb`
* **Kegunaan:** Menarik daftar ID wilayah Sumut yang valid dari API GQL Tokopedia dan mengekstrak struktur kategori dari file HTML.
* **Output:** `daftar_kabkot_sumut.csv` & `kategori_tokopedia_FULL.csv`.

### 2. Shop Discovery (Mega Scraper)
**File:** `1. Scraping Shop Tokopedia.py`
* **Kegunaan:** Mencari semua toko yang menjual produk di wilayah Sumut berdasarkan kategori.
* **Fitur:** Menggunakan *Smart Sampling* (mengambil sampel dari berbagai tipe sortir: *Ulasan, Terbaru, Harga*) untuk menjaring toko unik sebanyak mungkin.
* **Output:** CSV mentah per wilayah di dalam folder `Data_Tokopedia_Sumut/`.

### 3. Product & Shop Deep Scraper
**File:** `2. Scraping Product Tokopedia.py`
* **Kegunaan:** Masuk ke setiap toko secara mendalam untuk mengambil seluruh daftar produk dan metadata toko.
* **Fitur Unggulan:**
    * **Atomic Write:** Hanya menyimpan data jika satu toko berhasil di-scrape secara tuntas.
    * **Resume Mode:** Jika skrip mati, akan otomatis lanjut (skip) toko yang sudah sukses di-scrape sebelumnya.
    * **Deep Detail:** Mengambil `Terjual_Eksak` (angka asli) dan `Category_ID` yang tidak muncul di halaman depan.
* **Output:** `SHOP_P[x]/` dan `PRODUCT_P[x]/`.

---

## 🚀 Cara Menjalankan

1.  **Siapkan Master:**
    Buka `0. Scraping Master Tokped.ipynb`. Untuk scraping kategori, simpan halaman `tokopedia.com/p` sebagai file HTML dan arahkan skrip ke file tersebut.
    
2.  **Cari Daftar Toko:**
    ```bash
    python "1. Scraping Shop Tokopedia.py"
    ```
    Tunggu hingga folder `Data_Tokopedia_Sumut` terisi lengkap.

3.  **Scraping Detail Produk:**
    ```bash
    python "2. Scraping Product Tokopedia.py"
    ```
    * Masukkan mode `1` jika ingin melakukan split file (misal: 1 file berisi 500 toko) agar bisa dijalankan di banyak laptop sekaligus.
    * Masukkan mode `2` untuk mulai scraping produk berdasarkan file split tersebut.

---

## ⚠️ Catatan Teknis & Error Handling

* **Bypass 429 (Too Many Requests):** Skrip menggunakan `impersonate="chrome110"` dari `curl_cffi` agar request terlihat seperti browser asli. Jika tetap terkena block, gunakan koneksi hotspot seluler atau VPN.
* **Delimiter:** Semua file CSV menggunakan delimiter semikolon (`;`) agar bisa langsung dibuka di Excel tanpa merusak format teks yang mengandung koma.
* **Terjual Eksak:** Jika produk gagal ditarik detail penjualannya, field akan diisi string kosong `""` (bukan `0`) untuk membedakan antara gagal scrape dengan produk yang memang belum laku.

---

**Hanya untuk tujuan penelitian.**