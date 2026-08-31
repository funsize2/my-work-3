import os
import sys
import re
import time
import random
import datetime
import requests
from bs4 import BeautifulSoup

try:
    SITES = [s.strip() for s in os.environ['GAME_SERVER_API'].split(',') if s.strip()]
    WEBHOOK_URLS = [u.strip() for u in os.environ['WEBHOOK_URLS'].split(',') if u.strip()]
    WP_PURGE_KEY = os.environ['P_PASS'].strip()
    RUN_TIMES = [t.strip() for t in os.environ['RUN_TIMES'].split(',') if t.strip()]
    LOOP_DURATION = int(os.environ['LOOP_DURATION'])

    DELTA_L1 = float(os.environ['DELTA_L1'])
    DELTA_H1 = float(os.environ['DELTA_H1'])
    DELTA_L2 = float(os.environ['DELTA_L2'])
    DELTA_H2 = float(os.environ['DELTA_H2'])

    win_parts = [int(p.strip()) for p in os.environ['WIN_SHIFT'].split(',') if p.strip()]
    WIN_START = win_parts[0]
    WIN_END = win_parts[1]

    BACKOFF_MIN = float(os.environ['BACKOFF_MIN'])
    BACKOFF_MAX = float(os.environ['BACKOFF_MAX'])
except Exception:
    sys.exit(0)

def create_browser_session():
    """
    Initializes a persistent HTTP session with a realistic browser fingerprint
    for scraping external game sites.
    """
    profiles = [
        {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate',
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
            'Accept-Encoding': 'gzip, deflate',
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
            'Accept-Encoding': 'gzip, deflate',
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
            'Accept-Encoding': 'gzip, deflate',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
        },
        {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:131.0) Gecko/20100101 Firefox/131.0',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
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
            'Accept-Encoding': 'gzip, deflate',
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
    """Resolves the WordPress REST API endpoint from env variables."""
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
    """Fetches today's row from the WordPress custom table to initialize RAM state."""
    endpoint = get_api_endpoint()
    if not endpoint or not WP_PURGE_KEY:
        print("Error: WEBHOOK_URLS / WP_API_URL or P_PASS missing in environment.")
        return {}
        
    api_headers = {
        'X-Sync-Auth': str(WP_PURGE_KEY).strip(),
        'Accept': 'application/json',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'
    }
        
    for attempt in range(1, 4):
        try:
            resp = requests.get(
                endpoint,
                params={'key': WP_PURGE_KEY, 'action': 'get_today'},
                headers=api_headers,
                timeout=12
            )
            if resp.status_code == 200:
                try:
                    data = resp.json()
                    return data.get('data') or {}
                except Exception:
                    print(f"API Fetch Attempt {attempt} returned non-JSON response: {resp.text[:250]}")
            else:
                print(f"API Fetch Attempt {attempt} returned status {resp.status_code}: {resp.text[:250]}")
        except Exception as e:
            print(f"API Fetch Attempt {attempt} error: {e}")
        time.sleep(attempt * 2)
        
    return {}

def update_round_record(round_num, multi, single, is_master=0):
    """Sends score update to WordPress REST API endpoint."""
    endpoint = get_api_endpoint()
    if not endpoint or not WP_PURGE_KEY:
        print(f"Error: API endpoint or key not configured for Round {round_num}")
        return False, {}
        
    payload = {
        'key': WP_PURGE_KEY,
        'action': 'update_round',
        'round': round_num,
        'multi': multi,
        'single': single,
        'is_master': is_master
    }
    
    api_headers = {
        'X-Sync-Auth': str(WP_PURGE_KEY).strip(),
        'Accept': 'application/json',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'
    }
    
    for attempt in range(1, 4):
        try:
            resp = requests.post(
                endpoint,
                data=payload,
                params={'key': WP_PURGE_KEY},
                headers=api_headers,
                timeout=12
            )
            if resp.status_code == 200:
                try:
                    res_data = resp.json()
                    if res_data.get('updated'):
                        print(f"Main-site purge result: {res_data.get('purge_details')}")
                    return res_data.get('updated', False), res_data.get('data', {})
                except Exception:
                    print(f"API Update Attempt {attempt} (Round {round_num}) non-JSON response: {resp.text[:250]}")
            else:
                print(f"API Update Attempt {attempt} (Round {round_num}) status {resp.status_code}: {resp.text[:250]}")
        except Exception as e:
            print(f"API Update Attempt {attempt} error (Round {round_num}): {e}")
        time.sleep(attempt * 2)
        
    return False, {}

def get_active_round():
    """
    Checks if current time falls within any round's 30-minute window specified in RUN_TIMES.
    Returns (round_num, start_time_str) or (None, None)
    """
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

def get_adaptive_delays(active_time_str):
    """
    Computes active delay bounds dynamically based on elapsed minutes from round start.
    Switches to peak tier (DELTA_L2, DELTA_H2) if within WIN_START..WIN_END minutes.
    Otherwise applies standard tier (DELTA_L1, DELTA_H1).
    """
    if not active_time_str:
        return DELTA_L1, DELTA_H1
    try:
        parts = active_time_str.split(':')
        if len(parts) == 2:
            r_hour, r_min = int(parts[0]), int(parts[1])
            now = datetime.datetime.now()
            start_mins = r_hour * 60 + r_min
            now_mins = now.hour * 60 + now.minute
            elapsed_mins = now_mins - start_mins
            if WIN_START <= elapsed_mins <= WIN_END:
                return DELTA_L2, DELTA_H2
    except Exception:
        pass
    return DELTA_L1, DELTA_H1

def clean_number(text):
    """Sanitizes text and extracts digits only."""
    if not text: return None
    digits = re.findall(r'\d+', text)
    return digits[0] if digits else None

def parse_src_1(html, round_num):
    soup = BeautifulSoup(html, 'lxml')
    cell_prefix = os.environ.get('SRC_1_PREFIX', '').strip()
    m_cls = os.environ.get('SRC_1_M_CLASS', '').strip()
    s_cls = os.environ.get('SRC_1_S_CLASS', '').strip()
    if not cell_prefix or not m_cls or not s_cls:
        return None, None
    cell = soup.find('td', class_=f'{cell_prefix}{round_num}')
    if cell:
        m = cell.find(class_=m_cls)
        s = cell.find(class_=s_cls)
        if m and s: 
            return clean_number(m.text), clean_number(s.text)
    return None, None

def parse_src_2(html, round_num):
    soup = BeautifulSoup(html, 'lxml')
    container_cls = os.environ.get('SRC_2_CONTAINER', '').strip()
    if not container_cls:
        return None, None
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
    target_div_cls = os.environ.get('SRC_3_CONTAINER', '').strip()
    if not target_div_cls:
        return None, None
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
    target_cls = os.environ.get('SRC_4_CLASS', '').strip()
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
    """Extracts Source 5's numbered result card without reading round labels or tips cards."""
    soup = BeautifulSoup(html, 'lxml')
    grid_class = os.environ.get('SRC_5_GRID', '').strip()
    if not grid_class:
        return None, None

    grid = soup.find('div', class_=lambda c: c and grid_class in c)
    if not grid:
        return None, None

    for card in grid.find_all(['div', 'a'], recursive=False):
        round_label = card.find('div', class_=lambda c: c and 'tracking-widest' in c)
        if not round_label:
            continue

        label_match = re.fullmatch(r'(\d+)(?:st|nd|rd|th)', round_label.get_text(strip=True).lower())
        if not label_match or int(label_match.group(1)) != round_num:
            continue

        multi_el = card.find('div', class_=lambda c: c and 'animate-number-pop' in c)
        single_el = card.find('div', class_=lambda c: c and 'text-gradient-gold' in c)
        if not multi_el or not single_el:
            return None, None

        multi = clean_number(multi_el.get_text(strip=True))
        single = clean_number(single_el.get_text(strip=True))
        if multi is not None and single is not None:
            return multi, single

    return None, None

def main():
    existing_row = fetch_today_record()
    if not existing_row:
        print("Initial fetch completed (empty or new day record).")

    active_round, active_time = get_active_round()
    s1_verified_rounds = set()

    if active_round:
        print(f"Active Window: Round {active_round} (Scheduled {active_time})")
    else:
        print("Standard Scan Mode (No specific scheduled window active).")

    start_time = time.time()
    last_api_attempt_time = {}

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
                        attempt_key = (i, m_str, s_str)
                        last_attempt = last_api_attempt_time.get(attempt_key, 0)
                        
                        if (time.time() - last_attempt) < 15 and not curr_m:
                            continue
                        
                        if idx == 1:
                            if not curr_m:
                                print(f"[Source 1 Master] Inserting Round {i}: ({m_str}-{s_str})")
                                last_api_attempt_time[attempt_key] = time.time()
                                updated, fresh_data = update_round_record(i, m_str, s_str, is_master=1)
                                if updated:
                                    existing_row[f"r{i}_multi"], existing_row[f"r{i}_single"] = m_str, s_str
                                    s1_verified_rounds.add(i)
                                    data_updated = True
                            elif curr_m != m_str or curr_s != s_str:
                                print(f"[Source 1 Master Verification] Correcting Round {i} from ({curr_m}-{curr_s}) to ({m_str}-{s_str})")
                                last_api_attempt_time[attempt_key] = time.time()
                                updated, fresh_data = update_round_record(i, m_str, s_str, is_master=1)
                                if updated:
                                    existing_row[f"r{i}_multi"], existing_row[f"r{i}_single"] = m_str, s_str
                                    s1_verified_rounds.add(i)
                                    data_updated = True
                            else:
                                print(f"[Source 1 Master Verified] Round {i} matches existing DB ({curr_m}-{curr_s}).")
                                s1_verified_rounds.add(i)
                        else:
                            print(f"[Source {idx} Speed] Inserting Round {i}: ({m_str}-{s_str})")
                            last_api_attempt_time[attempt_key] = time.time()
                            updated, fresh_data = update_round_record(i, m_str, s_str, is_master=0)
                            if updated:
                                existing_row[f"r{i}_multi"], existing_row[f"r{i}_single"] = m_str, s_str
                                data_updated = True
            except Exception as err:
                print(f"Source {idx} Warning: {err}")

            if active_round:
                rounds_up_to_active = range(1, active_round + 1)
                all_exist = all(existing_row.get(f"r{r}_multi") for r in rounds_up_to_active)
                all_verified = all(r in s1_verified_rounds for r in rounds_up_to_active)
                if all_exist and all_verified:
                    print(f"✅ Rounds 1 to {active_round} verified with Source 1. Stopping runner and exiting.")
                    sys.exit(0)
            else:
                all_8_exist = all(existing_row.get(f"r{r}_multi") for r in range(1, 9))
                all_8_verified = all(r in s1_verified_rounds for r in range(1, 9))
                if all_8_exist and all_8_verified:
                    print("✅ All 8 daily rounds verified with Source 1. Stopping runner and exiting.")
                    sys.exit(0)

            cur_min, cur_max = get_adaptive_delays(active_time)
            sleep_time = random.uniform(cur_min, cur_max) if not data_updated else random.uniform(1.0, 2.0)
            time.sleep(sleep_time)

    print(f"Loop completed ({int(time.time() - start_time)}s). Exiting.")

if __name__ == "__main__":
    main()
