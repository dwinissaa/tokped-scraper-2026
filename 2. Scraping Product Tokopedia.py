import time
import random
import csv
import os
import glob
import re
import cloudscraper
import math
from datetime import datetime
from curl_cffi import requests
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- KONFIGURASI ---
BASE_INPUT_FOLDER = r'D:\Dwi Nissa\bantu2\Kak Indri\Data_Tokopedia_Sumut'
SPLIT_FOLDER = 'Split_Shop_Tokopedia_Sumut'
LOG_FILE = "error_deep_scraper.log"

PRODUCT_FIELDNAMES = ['Shop_ID', 'Kabkot', 'Product_ID', 'Category_ID', 'Nama_Produk', 'Harga', 'Terjual', 'Terjual_Eksak', 'Rating', 'URL_Produk', 'Scraped_At']
SHOP_FIELDNAMES = ['Shop_ID', 'Nama_Toko', 'Domain', 'Lokasi', 'Open_Since', 'Total_Favorite', 'Produk_Terjual', 'Transaksi_Sukses', 'Is_Official']

COMMON_HEADERS = {
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "content-type": "application/json",
    "x-source": "tokopedia-lite",
}

def log_error(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"[{timestamp}] {message}\n")

def split_shops(chunk_size=500):
    print("Menganalisa semua file di folder Kabkot...")
    all_domains = []
    cities = [d for d in os.listdir(BASE_INPUT_FOLDER) if os.path.isdir(os.path.join(BASE_INPUT_FOLDER, d))]
    for city in cities:
        for f in glob.glob(os.path.join(BASE_INPUT_FOLDER, city, "*.csv")):
            try:
                with open(f, 'r', encoding='utf-8-sig') as cf:
                    reader = csv.DictReader(cf, delimiter=';')
                    for row in reader:
                        if 'URL' in row:
                            domain = row['URL'].split('/')[-1]
                            if domain:
                                all_domains.append({'Shop_ID': row['Shop_ID'], 'Domain': domain, 'Kabkot': city})
            except: continue

    unique_data = {v['Domain']: v for v in all_domains}.values()
    list_unique = list(unique_data)
    total_data = len(list_unique)
    num_chunks = math.ceil(total_data / chunk_size)
    
    os.makedirs(SPLIT_FOLDER, exist_ok=True)
    for i in range(num_chunks):
        start = i * chunk_size
        end = start + chunk_size
        chunk = list_unique[start:end]
        file_path = os.path.join(SPLIT_FOLDER, f"SPLIT_SHOP-{i+1}.csv")
        with open(file_path, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=['Shop_ID', 'Domain', 'Kabkot'], delimiter=';')
            writer.writeheader()
            writer.writerows(chunk)
    print(f"Selesai! {total_data} toko dibagi menjadi {num_chunks} file.")

def get_pdp_details(url, pbar, retries=3):
    """Deep Scraping untuk ambil Terjual Eksak dan Category ID"""
    for attempt in range(retries):
        try:
            scraper = cloudscraper.create_scraper()
            pbar.set_postfix_str(f"Deep Check: {url.split('/')[-1][:10]} (Coba {attempt + 1})..")
            if attempt > 0: time.sleep(random.uniform(1, 2))
                
            resp = scraper.get(url, timeout=15)
            if resp.status_code == 200:
                html_text = resp.text
                sold_match = re.search(r'"countSold":"(\d+)"', html_text)
                sold_count = sold_match.group(1) if sold_match else "0"
                
                category_id = ""
                patterns = [
                    r'pdpCategory[:\s]*\{[\s\\"]*id[\s\\"]*[:\s]*[\s\\"]*(\d+)[\s\\"]*\}',
                    r'"id":"pdpCategory(\d+)"',
                    r'"category":\s*\{\s*"id":\s*"(\d+)"',
                    r'"category_id":"?(\d+)"?',
                    r'pdpCategory(\d+)'
                ]
                for pattern in patterns:
                    cat_match = re.search(pattern, html_text)
                    if cat_match:
                        category_id = cat_match.group(1)
                        break
                
                if category_id:
                    return sold_count, category_id
        except Exception:
            continue
    return "", "" # Return blank jika gagal total agar tidak crash

def get_shop_detailed_info(domain_name):
    """Ambil metadata toko"""
    url = "https://gql.tokopedia.com/graphql/ShopInfoCore"
    payload = [{
        "operationName": "ShopInfoCore",
        "variables": {"id": 0, "domain": domain_name},
        "query": """query ShopInfoCore($id: Int!, $domain: String) {
          shopInfoByID(input: {shopIDs: [$id], fields: ["active_product", "assets", "core", "create_info", "favorite", "location", "shopstats", "goldOS"], domain: $domain, source: "shoppage"}) {
            result {
              shopCore { shopID name domain }
              createInfo { openSince }
              favoriteData { totalFavorite }
              location
              shopStats { productSold totalTxSuccess }
              goldOS { isOfficial badge }
            }
          }
        }"""
    }]
    try:
        resp = requests.post(url, json=payload, headers=COMMON_HEADERS, impersonate="chrome110", timeout=10)
        if resp.status_code == 200:
            data = resp.json()[0]['data']['shopInfoByID']
            if data.get('result'):
                res = data['result'][0]
                return {
                    'Shop_ID': res['shopCore']['shopID'],
                    'Nama_Toko': res['shopCore']['name'],
                    'Domain': res['shopCore']['domain'],
                    'Lokasi': res['location'],
                    'Open_Since': res['createInfo']['openSince'],
                    'Total_Favorite': res['favoriteData']['totalFavorite'],
                    'Produk_Terjual': res['shopStats']['productSold'],
                    'Transaksi_Sukses': res['shopStats']['totalTxSuccess'],
                    'Is_Official': res['goldOS']['isOfficial']
                }
    except Exception as e:
        log_error(f"Error Shop Info {domain_name}: {e}")
    return None

def get_shop_products(shop_id, city_name, pbar, max_retries=3):
    """Ambil semua produk toko secara tuntas (All or Nothing)"""
    all_products = []
    page = 1
    current_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    while True:
        success = False
        for attempt in range(max_retries):
            try:
                payload = [{
                    "operationName": "ShopProducts",
                    "variables": {
                        "source": "shop", "sid": str(shop_id), "page": page, "perPage": 80,
                        "etalaseId": "etalase", "sort": 1, "usecase": "ace_get_shop_product_v2"
                    },
                    "query": """query ShopProducts($sid: String!, $source: String, $page: Int, $perPage: Int, $etalaseId: String, $sort: Int, $usecase: String) {
                      GetShopProduct(shopID: $sid, source: $source, filter: {page: $page, perPage: $perPage, fmenu: $etalaseId, sort: $sort, usecase: $usecase}) {
                        data {
                          name product_url product_id
                          price { text_idr }
                          label_groups { position title }
                          stats { reviewCount rating averageRating }
                        }
                      }
                    }"""
                }]
                
                resp = requests.post("https://gql.tokopedia.com/graphql/ShopProducts", 
                                     json=payload, headers=COMMON_HEADERS, impersonate="chrome110", timeout=15)
                
                if resp.status_code == 200:
                    res_json = resp.json()
                    prods = res_json[0]['data']['GetShopProduct']['data']
                    
                    # Proses Deep Scraping dengan Thread
                    temp_page_data = []
                    with ThreadPoolExecutor(max_workers=5) as executor:
                        future_to_prod = {}
                        for p in prods:
                            label_terjual = "0"
                            if p.get('label_groups'):
                                for lb in p['label_groups']:
                                    if lb.get('position') == "ri_product_credibility":
                                        label_terjual = lb.get('title', "0")
                            
                            p['label_terjual'] = label_terjual
                            fut = executor.submit(get_pdp_details, p['product_url'], pbar)
                            future_to_prod[fut] = p

                        for fut in as_completed(future_to_prod):
                            orig_p = future_to_prod[fut]
                            eksak, cid = fut.result()
                            temp_page_data.append({
                                'Shop_ID': shop_id, 'Kabkot': city_name, 
                                'Product_ID': orig_p.get('product_id', ''),
                                'Category_ID': cid, 'Nama_Produk': orig_p.get('name', ''), 
                                'Harga': orig_p.get('price', {}).get('text_idr', '0'), 
                                'Terjual': orig_p.get('label_terjual', '0'), 
                                'Terjual_Eksak': eksak if eksak else "", 
                                'Rating': orig_p.get('stats', {}).get('averageRating', 0), 
                                'URL_Produk': orig_p.get('product_url', ''), 'Scraped_At': current_ts
                            })
                    
                    all_products.extend(temp_page_data)
                    
                    if len(prods) < 80: 
                        return all_products # SELESAI SEMUA HALAMAN
                    
                    page += 1
                    success = True
                    time.sleep(random.uniform(0.1, 0.3))
                    break # Keluar dari loop retry halaman, lanjut ke next page
                
            except Exception as e:
                if attempt == max_retries - 1:
                    log_error(f"Gagal total Halaman {page} Toko {shop_id}: {e}")
                    return None # Gagalkan satu toko ini (All or Nothing)
                time.sleep(random.uniform(0.1, 0.3))
        
        if not success: return None

def start_scraping_process():
    print("--- TOKOPEDIA MULTI-DEVICE SCRAPER (RESUME MODE) ---")
    mode = input("Pilih mode: [1] Split File Baru, [2] Mulai Scraping : ")
    
    if mode == "1":
        size = input("1 file mau isi berapa toko? (Default 500): ")
        split_shops(chunk_size=int(size) if size else 500)
        return

    # Load File
    all_split_files = glob.glob(os.path.join(SPLIT_FOLDER, "SPLIT_SHOP-*.csv"))
    if not all_split_files: 
        print("File split tidak ditemukan.")
        return
    
    file_indices = [int(re.search(r'-(\d+)\.csv', f).group(1)) for f in all_split_files]
    period = input("Scraping untuk periode ke berapa? (1, 2, dst): ")
    file_num = input(f"Mau scraping file ke berapa? ({min(file_indices)}-{max(file_indices)}): ")
    
    input_file = os.path.join(SPLIT_FOLDER, f"SPLIT_SHOP-{file_num}.csv")
    shop_csv_path = f"SHOP_P{period}/SHOP_P{period}-{file_num}.csv"
    prod_csv_path = f"PRODUCT_P{period}/PRODUCT_P{period}-{file_num}.csv"
    
    os.makedirs(os.path.dirname(shop_csv_path), exist_ok=True)
    os.makedirs(os.path.dirname(prod_csv_path), exist_ok=True)

    # --- REFACTOR UTAMA: RESUME LOGIC BERDASARKAN DOMAIN ---
    # Membaca Domain yang sudah sukses agar skip jadi instan tanpa HIT API
    scraped_domains = set()
    if os.path.exists(shop_csv_path):
        with open(shop_csv_path, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f, delimiter=';')
            for row in reader: 
                scraped_domains.add(str(row['Domain']).strip().lower())

    with open(input_file, 'r', encoding='utf-8-sig') as f:
        targets = [row for row in csv.DictReader(f, delimiter=';')]

    pbar = tqdm(total=len(targets), unit="shop")
    
    for item in targets:
        domain = item['Domain'].strip()
        city = item['Kabkot']
        
        # 1. Cek Lokal (Instan): Jika domain sudah ada di file sukses, langsung skip
        if domain.lower() in scraped_domains:
            pbar.set_description(f"Skipping: {domain}")
            pbar.update(1)
            continue

        pbar.set_description(f"Scraping: {domain}")
        
        # 2. Ambil Shop Info (Hanya dilakukan untuk toko yang BELUM di-scrape)
        shop_info = get_shop_detailed_info(domain)
        if not shop_info:
            log_error(f"Gagal ambil info toko: {domain}")
            pbar.update(1)
            continue

        # 3. PROSES SCRAPING (All or Nothing)
        products = get_shop_products(shop_info['Shop_ID'], city, pbar)
        
        # 4. ATOMIC WRITE: Hanya tulis jika data produk tuntas
        if products is not None:
            # Simpan Produk
            if products:
                p_exists = os.path.isfile(prod_csv_path)
                with open(prod_csv_path, 'a', newline='', encoding='utf-8-sig') as f:
                    writer = csv.DictWriter(f, fieldnames=PRODUCT_FIELDNAMES, delimiter=';', extrasaction='ignore')
                    if not p_exists: writer.writeheader()
                    writer.writerows(products)
            
            # Simpan Shop (Tanda sukses toko ini)
            s_exists = os.path.isfile(shop_csv_path)
            with open(shop_csv_path, 'a', newline='', encoding='utf-8-sig') as f:
                writer = csv.DictWriter(f, fieldnames=SHOP_FIELDNAMES, delimiter=';', extrasaction='ignore')
                if not s_exists: writer.writeheader()
                writer.writerow(shop_info)
            
            scraped_domains.add(domain.lower())
        else:
            log_error(f"Toko {domain} di-skip sementara karena gangguan koneksi.")

        pbar.update(1)
        time.sleep(random.uniform(0.1, 0.3))

    pbar.close()

if __name__ == "__main__":
    start_scraping_process()