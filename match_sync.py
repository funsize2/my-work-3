import os
import sys
import re
import time
import random
import datetime
import requests
from bs4 import BeautifulSoup

SITES = [s.strip() for s in os.environ.get('GAME_SERVER_API', '').split(',') if s.strip()]
WEBHOOK_URLS = [u.strip() for u in os.environ.get('WEBHOOK_URLS', '').split(',') if u.strip()]
WP_PURGE_KEY = os.environ.get('P_PASS')
RUN_TIMES = [t.strip() for t in os.environ.get('RUN_TIMES', '').split(',') if t.strip()]
LOOP_DURATION = int(os.environ.get('LOOP_DURATION', 320))
SLEEP_MIN = float(os.environ.get('SLEEP_MIN', 6.0))
SLEEP_MAX = float(os.environ.get('SLEEP_MAX', 10.0))
BACKOFF_MIN = float(os.environ.get('BACKOFF_MIN', 4.0))
BACKOFF_MAX = float(os.environ.get('BACKOFF_MAX', 7.0))

def create_browser_session():
    profiles = [
        {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br, zstd',
            'Cache-Control': 'max-age=0',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Ch-Ua': '"Chromium";v="130", "Google Chrome";v="130", "Not?A_Brand";v="99"',
            'Sec-Ch-Ua-Mobile': '?0',
            'Sec-Ch-Ua-Platform': '"Windows"',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
        },
        {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br, zstd',
            'Cache-Control': 'max-age=0',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Ch-Ua': '"Chromium";v="130", "Google Chrome";v="130", "Not?A_Brand";v="99"',
            'Sec-Ch-Ua-Mobile': '?0',
            'Sec-Ch-Ua-Platform': '"macOS"',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
        },
        {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36 Edg/130.0.0.0',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br, zstd',
            'Cache-Control': 'max-age=0',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Ch-Ua': '"Microsoft Edge";v="130", "Chromium";v="130", "Not?A_Brand";v="99"',
            'Sec-Ch-Ua-Mobile': '?0',
            'Sec-Ch-Ua-Platform': '"Windows"',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
        },
        {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.0 Safari/605.1.15',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
        },
        {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:131.0) Gecko/20100101 Firefox/131.0',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br, zstd',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
        },
        {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:131.0) Gecko/20100101 Firefox/131.0',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br, zstd',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
        },
    ]

    selected_headers = random.choice(profiles)
    session = requests.Session()
    session.headers.update(selected_headers)
    return session

BROWSER_SESSION = create_browser_session()

def get_api_endpoint():
    custom_url = os.environ.get('WP_API_URL', '').strip()
    if custom_url:
        if custom_url.endswith('/sync-score'):
            return custom_url
        return custom_url.rstrip('/') + '/wp-json/custom/v1/sync-score'
    
    if WEBHOOK_URLS:
        base = WEBHOOK_URLS[0].rstrip('/')
        if '/wp-json/' in base:
            return base.split('/wp-json/')[0] + '/wp-json/custom/v1/sync-score'
        return f"{base}/wp-json/custom/v1/sync-score"
        
    return None

def fetch_today_record():
    endpoint = get_api_endpoint()
    if not endpoint or not WP_PURGE_KEY:
        print("Error: WEBHOOK_URLS / WP_API_URL or P_PASS missing in environment.")
        return {}
        
    for attempt in range(1, 4):
        try:
            resp = BROWSER_SESSION.get(
                endpoint,
                params={'key': WP_PURGE_KEY, 'action': 'get_today'},
                timeout=15
            )
            if resp.status_code == 200:
                data = resp.json()
                return data.get('data') or {}
            else:
                print(f"API Fetch Attempt {attempt} returned status {resp.status_code}: {resp.text}")
        except Exception as e:
            print(f"API Fetch Attempt {attempt} error: {e}")
        time.sleep(2)
    return {}

def update_round_record(round_num, multi, single, is_master=0):
    endpoint = get_api_endpoint()
    if not endpoint or not WP_PURGE_KEY:
        return False, {}
        
    payload = {
        'key': WP_PURGE_KEY,
        'action': 'update_round',
        'round': round_num,
        'multi': multi,
        'single': single,
        'is_master': is_master
    }
    
    for attempt in range(1, 4):
        try:
            resp = BROWSER_SESSION.post(
                endpoint,
                data=payload,
                timeout=15
            )
            if resp.status_code == 200:
                res_data = resp.json()
                return res_data.get('updated', False), res_data.get('data', {})
            else:
                print(f"API Update Attempt {attempt} (Round {round_num}) status {resp.status_code}: {resp.text}")
        except Exception as e:
            print(f"API Update Attempt {attempt} error (Round {round_num}): {e}")
        time.sleep(1.5)
        
    return False, {}

def get_active_round():
    if not RUN_TIMES:
        return None, None
        
    now = datetime.datetime.now()
    now_minutes = now.hour * 60 + now.minute
    
    for idx, time_str in enumerate(RUN_TIMES, start=1):
        try:
            parts = time_str.split(':')
            if len(parts) != 2: continue
            r_hour, r_min = int(parts[0]), int(parts[1])
            start_mins = r_hour * 60 + r_min
            end_mins = start_mins + 30
            
            if start_mins <= now_minutes < end_mins:
                return idx, time_str
        except Exception:
            continue
            
    return None, None

def clean_number(text):
    if not text: return None
    digits = re.findall(r'\d+', text)
    return digits[0] if digits else None

def parse_src_1(html, round_num):
    soup = BeautifulSoup(html, 'lxml')
    cell_prefix = os.environ.get('SRC_1_PREFIX', 'bazi-cell-')
    cell = soup.find('td', class_=f'{cell_prefix}{round_num}')
    if cell:
        m_cls = os.environ.get('SRC_1_M_CLASS', 'field1-val')
        s_cls = os.environ.get('SRC_1_S_CLASS', 'field2-val')
        m = cell.find(class_=m_cls)
        s = cell.find(class_=s_cls)
        if m and s: 
            return clean_number(m.text), clean_number(s.text)
    return None, None

def parse_src_2(html, round_num):
    soup = BeautifulSoup(html, 'lxml')
    container_cls = os.environ.get('SRC_2_CONTAINER', 'flex flex-1 flex-col items-center')
    containers = soup.find_all('div', class_=container_cls)
    
    if len(containers) >= round_num:
        target_round = containers[round_num - 1]
        spans = target_round.find_all('span')
        
        if len(spans) >= 3:
            m_text = spans[1].get_text(strip=True)
            s_text = spans[2].get_text(strip=True)
            
            m = clean_number(m_text)
            s = clean_number(s_text)
            
            if m and s:
                return m, s
                
    return None, None

def parse_src_3(html, round_num):
    soup = BeautifulSoup(html, 'lxml')
    target_div_cls = os.environ.get('SRC_3_CONTAINER', 'today')
    today_div = soup.find('div', class_=target_div_cls)
    if not today_div:
        today_div = soup.find('table')
        if not today_div:
            return None, None
            
    rows = today_div.find_all('tr')
    if len(rows) >= 4:
        multi_row = rows[2].find_all('td')
        single_row = rows[3].find_all('td')
        
        idx = round_num - 1
        if len(multi_row) > idx and len(single_row) > idx:
            m = clean_number(multi_row[idx].get_text(strip=True))
            s = clean_number(single_row[idx].get_text(strip=True))
            if m and s:
                return m, s
                
    return None, None

def parse_src_4(html, round_num):
    soup = BeautifulSoup(html, 'lxml')
    target_cls = os.environ.get('SRC_4_CLASS', 'ffresult')
    
    table_fig = None
    if target_cls:
        table_fig = soup.find('figure', class_=lambda c: c and target_cls in c)
        
    if not table_fig:
        for fig in soup.find_all('figure', class_=lambda c: c and 'wp-block-table' in c):
            classes = fig.get('class', [])
            if not any('hist' in c.lower() for c in classes):
                table_fig = fig
                break
                
    if not table_fig:
        return None, None

    rows = table_fig.find_all('tr')
    if len(rows) >= 5:
        multi_row = rows[3].find_all('td')
        single_row = rows[4].find_all('td')
    elif len(rows) == 4:
        multi_row = rows[2].find_all('td')
        single_row = rows[3].find_all('td')
    else:
        return None, None

    idx = round_num - 1
    if len(multi_row) > idx and len(single_row) > idx:
        m = clean_number(multi_row[idx].get_text(strip=True))
        s = clean_number(single_row[idx].get_text(strip=True))
        if m and s:
            return m, s

    return None, None

def parse_src_5(html, round_num):
    soup = BeautifulSoup(html, 'lxml')
    grid_cls = os.environ.get('SRC_5_GRID', 'grid-cols-4')
    
    grid = soup.find('div', class_=lambda c: c and grid_cls in c)
    if not grid:
        grid = soup.find('div', class_=lambda c: c and 'grid' in c and 'gap-' in c)
        if not grid:
            return None, None

    cards = grid.find_all('div', recursive=False)
    if not cards:
        cards = grid.find_all('div', class_=lambda c: c and 'rounded-2xl' in c)
        
    idx = round_num - 1
    if len(cards) > idx:
        card = cards[idx]
        text_elements = card.find_all('div')
        digits_found = []
        
        for el in text_elements:
            txt = el.get_text(strip=True)
            if txt.endswith(('st', 'nd', 'rd', 'th')):
                continue
            cleaned = clean_number(txt)
            if cleaned and cleaned not in digits_found:
                digits_found.append(cleaned)
                
        if len(digits_found) >= 2:
            return digits_found[0], digits_found[1]

    return None, None

def trigger_webhooks():
    try:
        urls = WEBHOOK_URLS
        wp_key = WP_PURGE_KEY
        
        for target_url in urls:
            if wp_key:
                try:
                    if '/wp-json/' in target_url:
                        endpoint = target_url
                    else:
                        endpoint = f"{target_url.rstrip('/')}/wp-json/custom/v1/bump-cache-version"
                    
                    params = {"key": wp_key}
                    resp = BROWSER_SESSION.get(endpoint, params=params, timeout=5)
                    print(f"Cache Purge Webhook ({resp.status_code}) -> {target_url}")
                except Exception as e:
                    print(f"Webhook Error ({target_url}): {e}")
    except Exception as e: 
        print(f"Purge Warning: {e}")

def main():
    existing_row = fetch_today_record()
    if not existing_row:
        print("Initial fetch completed (empty or new day record).")

    active_round, active_time = get_active_round()
    s1_verified_rounds = set()

    if active_round:
        curr_m = str(existing_row.get(f"r{active_round}_multi") or '').strip()
        print(f"Active Window: Round {active_round} (Scheduled {active_time})")
        if all(existing_row.get(f"r{r}_multi") for r in range(1, 9)):
            print("All 8 daily rounds are already completed. Exiting immediately.")
            sys.exit(0)
    else:
        print("Standard Scan Mode (No specific scheduled window active).")

    start_time = time.time()
    active_target_completed = False

    while (time.time() - start_time) < LOOP_DURATION:
        for idx, url in enumerate(SITES, start=1):
            if (time.time() - start_time) >= LOOP_DURATION:
                break

            print(f"[{int(time.time() - start_time)}s] Scanning Source {idx}...")
            data_updated = False
            try:
                resp = BROWSER_SESSION.get(url, timeout=12)
                if resp.status_code in [403, 429]:
                    print(f"Source {idx} blocked ({resp.status_code}). Skipping.")
                    time.sleep(random.uniform(BACKOFF_MIN, BACKOFF_MAX))
                    continue
                if resp.status_code != 200:
                    time.sleep(random.uniform(BACKOFF_MIN, BACKOFF_MAX))
                    continue

                for i in range(1, 9):
                    round_col = f"r{i}"
                    curr_m = str(existing_row.get(f"{round_col}_multi") or '').strip()
                    curr_s = str(existing_row.get(f"{round_col}_single") or '').strip()

                    if idx > 1 and curr_m:
                        continue

                    if idx == 1 and i in s1_verified_rounds and curr_m:
                        continue

                    m, s = None, None
                    if idx == 1: m, s = parse_src_1(resp.text, i)
                    elif idx == 2: m, s = parse_src_2(resp.text, i)
                    elif idx == 3: m, s = parse_src_3(resp.text, i)
                    elif idx == 4: m, s = parse_src_4(resp.text, i)
                    elif idx == 5: m, s = parse_src_5(resp.text, i)

                    if m and s:
                        m_str, s_str = str(m).strip(), str(s).strip()
                        
                        if idx == 1:
                            s1_verified_rounds.add(i)
                            if not curr_m:
                                print(f"[Source 1 Master] Inserting Round {i}: ({m_str}-{s_str})")
                                updated, fresh_data = update_round_record(i, m_str, s_str, is_master=1)
                                if updated:
                                    existing_row = fresh_data
                                    data_updated = True
                                    print(f"✅ Round {i} saved & cache purged.")
                                if active_round == i:
                                    active_target_completed = True
                            elif curr_m != m_str or curr_s != s_str:
                                print(f"[Source 1 Master Verification] Correcting Round {i} from ({curr_m}-{curr_s}) to ({m_str}-{s_str})")
                                updated, fresh_data = update_round_record(i, m_str, s_str, is_master=1)
                                if updated:
                                    existing_row = fresh_data
                                    data_updated = True
                                    print(f"✅ Round {i} corrected & cache purged.")
                                if active_round == i:
                                    active_target_completed = True
                            else:
                                if active_round == i:
                                    active_target_completed = True
                        else:
                            print(f"[Source {idx} Speed] Inserting Round {i}: ({m_str}-{s_str})")
                            updated, fresh_data = update_round_record(i, m_str, s_str, is_master=0)
                            if updated:
                                existing_row = fresh_data
                                data_updated = True
                                print(f"⚡ Round {i} live-inserted & cache purged.")
            except Exception as err:
                print(f"Source {idx} Warning: {err}")

            if active_round and active_target_completed and (active_round in s1_verified_rounds):
                print(f"Target Round {active_round} is verified by Master Source 1. Exiting gracefully.")
                sys.exit(0)

            sleep_time = random.uniform(SLEEP_MIN, SLEEP_MAX)
            time.sleep(sleep_time)

    print(f"Loop completed ({int(time.time() - start_time)}s). Exiting.")

if __name__ == "__main__":
    main()
