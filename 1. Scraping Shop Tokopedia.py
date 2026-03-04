import math
import time
import random
import csv
import os
from curl_cffi import requests
from tqdm import tqdm

# --- KONFIGURASI ---
SOURCE_KATEGORI = 'kategori_tokopedia_FULL.csv'
SOURCE_LOKASI = 'daftar_kabkot_sumut.csv'
BASE_OUTPUT_FOLDER = 'Data_Tokopedia_Sumut'
MAX_RETRIES = 3 
RETRY_DELAY = 5 
LOG_FILE = "error_scraper.log"

def log_error(message):
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"[{timestamp}] {message}\n")

def get_tokopedia_smart_sampling(category_id, city_id, category_name, pbar_total):
    url = "https://gql.tokopedia.com/graphql/SearchProductQuery"
    headers = {
        "content-type": "application/json",
        "x-source": "tokopedia-lite",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    
    sort_options = [
        {"name": "Paling Sesuai", "ob": "23"}, 
        {"name": "Terbaru", "ob": "9"}, 
        {"name": "Ulasan", "ob": "5"}, 
        {"name": "Harga Tertinggi", "ob": "4"}, 
        {"name": "Harga Terendah", "ob": "3"}
    ]
    
    unique_shops = {} 
    rows_per_page = 60
    
    # PARAMETER BALANCED SAMPLING
    MIN_PAGE_LIMIT = 10   # Minimal ambil 10 halaman kalau produk > 600
    MAX_PAGE_LIMIT = 50   # Maksimal mentok di 50 halaman per sortir
    SAMPLING_RATE = 0.5   # Ambil 50% dari total halaman yang tersedia

    for index, sort in enumerate(sort_options):
        page = 1
        max_page_for_this_sort = 1 
        
        while page <= max_page_for_this_sort:
            params = f"page={page}&ob={sort['ob']}&sc={category_id}&rows={rows_per_page}&source=directory&device=desktop&st=product&fcity={city_id}"
            payload = [{
                "operationName": "SearchProductQuery", 
                "variables": {"params": params, "adParams": ""}, 
                "query": "query SearchProductQuery($params: String) { searchProduct(params: $params) { count products { shop { id name location url } } } }"
            }]

            success = False
            for attempt in range(MAX_RETRIES):
                try:
                    resp = requests.post(url, headers=headers, json=payload, impersonate="chrome110", timeout=30)
                    if resp.status_code == 200:
                        res_json = resp.json()[0].get('data')
                        if not res_json: 
                            success = True; break
                        
                        data = res_json['searchProduct']
                        total_count = data.get('count', 0)
                        
                        if page == 1:
                            total_pages = math.ceil(total_count / rows_per_page)
                            
                            # --- LOGIKA KEPUTUSAN HALAMAN (REVISI BALANCED) ---
                            if total_pages <= MIN_PAGE_LIMIT:
                                # Kasus produk dikit: sikat semua halaman di sort pertama saja
                                if index == 0:
                                    max_page_for_this_sort = total_pages
                                else:
                                    # Langsung keluar fungsi untuk ganti sub-kat karena sort lain pasti duplikat
                                    return list(unique_shops.values())
                            else:
                                # Kasus produk banyak: gunakan sampling 50% (range 10-50)
                                calc_sampling = math.ceil(total_pages * SAMPLING_RATE)
                                max_page_for_this_sort = max(MIN_PAGE_LIMIT, min(calc_sampling, MAX_PAGE_LIMIT))

                        # Progress update
                        pbar_total.set_description(
                            f"L1: {category_name} | Catid: {category_id} | sort: {sort['name']} | Prd: {total_count} | Hal: {page}/{max_page_for_this_sort}"
                        )

                        products = data.get('products', [])
                        if products:
                            for p in products:
                                s = p.get('shop')
                                if s and s['id'] not in unique_shops:
                                    unique_shops[s['id']] = {
                                        'Shop_ID': s['id'], 
                                        'Nama_Toko': s['name'], 
                                        'Lokasi': s['location'], 
                                        'URL': s['url'], 
                                        'Kategori_ID': category_id, 
                                        'Reference_Sort': sort['name']
                                    }
                        success = True
                        break
                    elif resp.status_code == 429:
                        time.sleep(RETRY_DELAY * 2)
                    else:
                        time.sleep(RETRY_DELAY)
                except Exception:
                    time.sleep(RETRY_DELAY)
            
            if not success: break
            page += 1
            time.sleep(random.uniform(1.1, 1.8)) 
            
    return list(unique_shops.values())

def run_mega_scraper():
    # 1. Baca Lokasi
    locations = []
    if not os.path.exists(SOURCE_LOKASI):
        print(f"Error: File {SOURCE_LOKASI} tidak ditemukan!")
        return
    with open(SOURCE_LOKASI, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader: locations.append(row)

    # 2. Baca Kategori
    categories_by_l1 = {}
    total_sub_categories = 0
    if not os.path.exists(SOURCE_KATEGORI):
        print(f"Error: File {SOURCE_KATEGORI} tidak ditemukan!")
        return
    with open(SOURCE_KATEGORI, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f, delimiter=';')
        for row in reader:
            l1 = row['L1_Name'].strip()
            if l1 not in categories_by_l1: categories_by_l1[l1] = []
            categories_by_l1[l1].append(row['Category_ID'])
            total_sub_categories += 1

    # 3. Mulai Loop Wilayah
    locs = locations
    print(f"Bersiap menscraping shop di {len(locs)} wilayah:")
    for i, loc in enumerate(locs, 1):
        print(f"{i}. {loc['nama']}")
    print("-" * 30) # Garis pembatas agar rapi
    
    for loc in locs:
        city_id = loc['id']
        city_name = loc['nama'].replace('.', '').strip()
        city_folder = os.path.join(BASE_OUTPUT_FOLDER, city_name)
        if not os.path.exists(city_folder): os.makedirs(city_folder)

        print(f"\n>>> PROCESSING WILAYAH: {city_name}")
        pbar_total = tqdm(total=total_sub_categories, desc=f"Mulai {city_name}", unit="subkat")

        for l1_name, cat_ids in categories_by_l1.items():
            l1_safe_name = l1_name.replace('/', '_').replace(',', '')
            file_path = os.path.join(city_folder, f"{l1_safe_name}.csv")
            
            # Skip jika file kategori L1 ini sudah ada (asumsi sudah selesai)
            if os.path.exists(file_path):
                pbar_total.update(len(cat_ids))
                continue

            seen_shop_ids = set()
            for c_id in cat_ids:
                shops = []
                # Panggil sampling pintar
                try:
                    shops = get_tokopedia_smart_sampling(c_id, city_id, l1_name, pbar_total)
                except Exception as e:
                    log_error(f"Gagal sub-kat {c_id} di {city_name}: {str(e)}")

                new_shops_to_append = []
                if shops:
                    for s in shops:
                        if s['Shop_ID'] not in seen_shop_ids:
                            new_shops_to_append.append(s)
                            seen_shop_ids.add(s['Shop_ID'])
                    
                    if new_shops_to_append:
                        try:
                            file_exists = os.path.isfile(file_path)
                            with open(file_path, 'a', newline='', encoding='utf-8-sig') as f:
                                # Gunakan delimiter ';' agar mudah dibuka di Excel Indonesia
                                writer = csv.DictWriter(f, fieldnames=new_shops_to_append[0].keys(), delimiter=';')
                                if not file_exists: writer.writeheader()
                                writer.writerows(new_shops_to_append)
                        except Exception as e:
                            log_error(f"Gagal Tulis CSV {file_path}: {str(e)}")

                pbar_total.update(1)
                pbar_total.set_postfix({"new": len(new_shops_to_append), "total_L1": len(seen_shop_ids)})

        pbar_total.close()

if __name__ == "__main__":
    try:
        run_mega_scraper()
    except KeyboardInterrupt:
        print("\n\nScraping dihentikan paksa oleh user.")
    except Exception as e:
        print(f"\n\nTerjadi error fatal: {e}")