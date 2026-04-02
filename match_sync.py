import os
import sys
import re
import time
import random
import requests
import mysql.connector
from bs4 import BeautifulSoup

SITES = [s.strip() for s in os.environ.get('GAME_SERVER_API', '').split(',') if s.strip()]
CF_URLS_TO_PURGE = [u.strip() for u in os.environ.get('CF_URLS', '').split(',') if u.strip()]
WP_PURGE_KEY = os.environ.get('P_PASS')

def get_random_headers():
    versions = ["120.0.0.0", "121.0.0.0", "122.0.0.0", "123.0.0.0", "124.0.0.0"]
    v = random.choice(versions)
    return {
        'User-Agent': f'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{v} Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
    }

def get_db_connection():
    for attempt in range(3):
        try:
            conn = mysql.connector.connect(
                host=os.environ['DB_HOST'],
                user=os.environ['DB_USER'],
                password=os.environ['DB_PASSWORD'],
                database=os.environ['DB_NAME'],
                connect_timeout=15
            )
            cursor = conn.cursor(dictionary=True)
            cursor.execute("SET time_zone = '+05:30'") 
            return conn, cursor
        except mysql.connector.Error as e:
            if attempt < 2:
                time.sleep(5)
            else:
                print(f"DB Error: {e}")
                sys.exit(1)

def clean_number(text):
    if not text: return None
    digits = re.findall(r'\d+', text)
    return digits[0] if digits else None

def parse_src_0(html, round_num):
    soup = BeautifulSoup(html, 'lxml')
    tbody = soup.find('tbody')
    if not tbody: return None, None
    rows = tbody.find_all('tr')
    if len(rows) < 2: return None, None
    col_idx = round_num - 1
    try:
        m_cells = rows[0].find_all('td')
        s_cells = rows[1].find_all('td')
        if len(m_cells) > col_idx and len(s_cells) > col_idx:
            return clean_number(m_cells[col_idx].text), clean_number(s_cells[col_idx].text)
    except Exception:
        pass
    return None, None

def parse_src_1(html, round_num):
    soup = BeautifulSoup(html, 'lxml')
    cell = soup.find('td', class_=f'bazi-cell-{round_num}')
    if cell:
        m = cell.find(class_="field1-val")
        s = cell.find(class_="field2-val")
        if m and s: 
            return clean_number(m.text), clean_number(s.text)
    return None, None

def main():
    time.sleep(random.randint(1, 20))
    conn, cursor = get_db_connection()
    try:
        cursor.execute("SELECT * FROM f_score WHERE game_date = CURDATE()")
        existing_row = cursor.fetchone() or {}
    except Exception as e:
        print(f"Query Error: {e}")
        conn.close()
        sys.exit(1)

    data_updated = False

    for idx, url in enumerate(SITES):
        print(f"Scanning Source {idx}...")
        try:
            resp = requests.get(url, headers=get_random_headers(), timeout=12)
            print(f"Status: {resp.status_code}")
            if resp.status_code in [403, 429]:
                print(f"Source {idx} blocked. Skipping.")
                continue
            if resp.status_code != 200: continue

            for i in range(1, 9):
                round_col = f"r{i}"
                if existing_row.get(f"{round_col}_multi"): continue 

                m, s = None, None
                if idx == 0: m, s = parse_src_0(resp.text, i)
                elif idx == 1: m, s = parse_src_1(resp.text, i)

                if m and s:
                    print(f"Found Result: Round {i} ({m}-{s})")
                    sql = f"""
                        INSERT INTO f_score (game_date, {round_col}_multi, {round_col}_single)
                        VALUES (CURDATE(), %s, %s)
                        ON DUPLICATE KEY UPDATE
                        {round_col}_multi = VALUES({round_col}_multi),
                        {round_col}_single = VALUES({round_col}_single)
                    """
                    cursor.execute(sql, (m, s))
                    conn.commit()
                    data_updated = True
                    existing_row[f"{round_col}_multi"] = m
        except Exception: continue
        
        if data_updated:
            print("Database updated.")
            try:
                cf_urls = [u.strip() for u in os.environ.get('CF_URLS', '').split(',') if u.strip()]
                cf_zones = [z.strip() for z in os.environ.get('ID', '').split(',') if z.strip()]
                cf_token = os.environ.get('TOKEN')
                wp_key = os.environ.get('P_PASS')

                for i, purge_url in enumerate(cf_urls):
                    if wp_key:
                        try:
                            requests.get(f"{purge_url}/?litespeed_trigger={wp_key}", timeout=5)
                            print(f"LiteSpeed Purged: {purge_url}")
                        except: pass
                    
                    if cf_token and i < len(cf_zones):
                        target_zone = cf_zones[i]
                        try:
                            requests.post(
                                f"https://api.cloudflare.com/client/v4/zones/{target_zone}/purge_cache",
                                json={"purge_everything": True},
                                headers={"Authorization": f"Bearer {cf_token}"}, 
                                timeout=5
                            )
                            print(f"Cloudflare Purged: {target_zone}")
                        except Exception as e:
                            print(f"CF Error: {e}")
            except Exception as e: 
                print(f"Purge Warning: {e}")
            
            conn.close()
            sys.exit(0)

    conn.close()
    print("No new data found.")

if __name__ == "__main__":
    main()
