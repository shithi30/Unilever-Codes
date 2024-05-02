#!/usr/bin/env python
# coding: utf-8

# In[1]:


## Pandamart

# import
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
import pandas as pd
import duckdb
from googleapiclient.discovery import build
from google.oauth2 import service_account
from random import shuffle, randint
import time

# record
start_time = time.time()

# list
ubl_skus_df = pd.read_csv('Eagle Eye - Updated SKU List (All Platforms).csv')

# urls
urls = [
    'https://www.foodpanda.com.bd/darkstore/w2lx/pandamart-gulshan-w2lx',
    'https://www.foodpanda.com.bd/darkstore/h5rj/pandamart-bashundhara',
    'https://www.foodpanda.com.bd/darkstore/ta7z/pandamart-dhanmondi',
    'https://www.foodpanda.com.bd/darkstore/n7ph/pandamart-uttara',
    'https://www.foodpanda.com.bd/darkstore/v1ts/pandamart-mogbazar',
    'https://www.foodpanda.com.bd/darkstore/q4hz/pandamart-sylhet-02',
    'https://www.foodpanda.com.bd/darkstore/a2er/pandamart-khulna',
    'https://www.foodpanda.com.bd/darkstore/w2nv/pandamart-chittagong-1'
]
shuffle(urls)
urls = [urls[0]]

# credentials
SERVICE_ACCOUNT_FILE = 'read-write-to-gsheet-apis-1-04f16c652b1e.json'
SAMPLE_SPREADSHEET_ID = '1gkLRp59RyRw4UFds0-nNQhhWOaS4VFxtJ_Hgwg2x2A0'
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

# API
def sheet_api():
    creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    service = build('sheets', 'v4', credentials=creds)
    sheet = service.spreadsheets()
    return sheet

# preference
options = webdriver.ChromeOptions()
options.add_argument('ignore-certificate-errors')

# open window
len_urls = len(urls)
for j in range(0, len_urls): 
    driver = webdriver.Chrome(options=options)
    driver.maximize_window()
    
    # url
    print("Scraping from: " + urls[j])
    driver.get(urls[j])

    # cross
    elem = driver.find_element(By.CLASS_NAME, 'groceries-icon')
    ActionChains(driver).click().perform()
    
    # accumulator
    df_acc = pd.DataFrame()

    # banners
    elems = driver.find_elements(By.CLASS_NAME, 'campaign-banners-swiper-link')
    banners = len(elems)
    for i in range(0, banners): 
        elems = driver.find_elements(By.CLASS_NAME, 'campaign-banners-swiper-link')
        ActionChains(driver).move_to_element(elems[i]).click().perform()

        # scroll
        last_height = driver.execute_script('return document.body.scrollHeight')
        while True:
            driver.execute_script('window.scrollTo(0, document.body.scrollHeight);')
            time.sleep(4)
            new_height = driver.execute_script('return document.body.scrollHeight')
            if new_height == last_height: break
            last_height = new_height

        # soup
        soup_init = BeautifulSoup(driver.page_source, 'html.parser')
        soup = soup_init.find_all('div', attrs={'class', 'box-flex product-card-attributes'})

        # scrape
        sku = []
        current_price = []
        original_price = []
        offer = []
        for s in soup:
            # sku
            try: val = s.find('p', attrs={'class', 'product-card-name'}).get_text()
            except: val = None
            sku.append(val)
            # current price
            try: val = s.find("span", attrs={"data-testid", "product-card-price"}).get_text().split()[1]
            except: val = None
            current_price.append(val)
            # original price
            try: val = s.find("span", attrs={"data-testid", "product-card-price-before-discount"}).get_text().split()[1]
            except: val = None
            original_price.append(val)
            # offer
            try: val = s.find("span", attrs={"class", "bds-c-tag__label"}).get_text()
            except: val = None
            offer.append(val)

        # accumulate
        df = pd.DataFrame()
        df['sku'] = sku
        df['current_price'] = current_price
        df['original_price'] = original_price
        df['offer'] = offer
        df['banner'] = i + 1
        df['pos_in_pg'] = list(range(1, df.shape[0]+1))
        df_acc = df_acc._append(df, ignore_index = True)

        # back
        driver.back()
        
    # close window
    driver.close()

    # Unilever
    brands = ['Boost Health', 'Boost Drink', 'Boost Jar', 'Clear Shampoo', 'Simple Fac', 'Simple Mask', 'Pepsodent', 'Brylcreem', 'Bru Coffee', 'St. Ives', 'St.Ives', 'Horlicks', 'Sunsilk', 'Sun Silk', 'Lux', 'Ponds', "Pond's", 'Closeup', 'Close Up', 'Cif', 'Dove', 'Maltova', 'Domex', 'Clinic Plus', 'Tresemme', 'Tresemmé', 'GlucoMax', 'Knorr', 'Glow Lovely', 'Fair Lovely', 'Glow Handsome', 'Wheel Wash', 'Axe Body', 'Pureit', 'Lifebuoy', 'Surf Excel', 'Vaseline', 'Vim', 'Rin']
    skus = df_acc['sku'].tolist()
    sku_count = len(skus)
    if_ubl = [None] * sku_count
    for i in range(0, sku_count):
        for b in brands:
            bb = b.split()
            if len(bb) == 1: bb.append('')
            if bb[0].lower() + ' ' in skus[i].lower() and bb[1].lower() in skus[i].lower(): if_ubl[i] = b
    df_acc['brand_unilever'] = if_ubl
    df_acc = df_acc.dropna(subset = ['brand_unilever'])[df_acc.columns]

    # data
    qry = '''
    with 
        tbl3 as
        (select * 
        from 
            (select Category category, "Updated Perfect Name" sku
            from ubl_skus_df
            where Platform='Pandamart'
            ) tbl1 
            left join 
            df_acc tbl2 using(sku)
        ) 
    select 
        *, 
        case 
            when category is not null and pos_in_pg is not null then 'enlisted + online'
            when category is not null and pos_in_pg is null then 'enlisted + offline'
            when category is null and pos_in_pg is not null then 'unlisted + online'
            when category is null and pos_in_pg is null then 'unlisted + offline'
        end ola_status
    from 
        (select * from tbl3
        union all
        select null category, sku, current_price, original_price, offer, banner, pos_in_pg, brand_unilever
        from df_acc
        where sku not in(select distinct sku from tbl3)
        ) tbl4
    '''
    df_acc = duckdb.query(qry).df()
    df_acc['site'] = urls[j].split("/")[-1]
    df_acc['report_time'] = time.strftime('%d-%b-%y, %I:%M %p')

    # OLA
    qry = '''
    select
        site,
        count(distinct sku) skus_enlisted, 
        count(distinct case when pos_in_pg is not null then sku else null end) skus_online,
        round(count(distinct case when pos_in_pg is not null then sku else null end)*1.00/count(distinct sku), 4) ola,
        max(report_time) report_time
    from df_acc
    where category is not null
    group by 1
    '''
    ola_df = duckdb.query(qry).df()
    
    # call API
    sheet = sheet_api()
    # extract
    values = sheet.values().get(spreadsheetId=SAMPLE_SPREADSHEET_ID, range='Pandamart OLA!A1:K').execute().get('values', [])
    df_acc_rd = pd.DataFrame(values[1:] , columns = values[0])
    values = sheet.values().get(spreadsheetId=SAMPLE_SPREADSHEET_ID, range='Pandamart OLA!M1:Q').execute().get('values', [])
    ola_df_rd = pd.DataFrame(values[1:] , columns = values[0])
    # transform
    qry = '''select * from df_acc_rd where site!=''' + "'" + urls[j].split("/")[-1] + "'" + ''' union all select * from df_acc'''
    df_acc_wrt = duckdb.query(qry).df().fillna('')
    qry = '''select * from ola_df_rd where site!=''' + "'" + urls[j].split("/")[-1] + "'" + ''' union all select * from ola_df'''
    ola_df_wrt = duckdb.query(qry).df()
    # load
    sheet.values().clear(spreadsheetId=SAMPLE_SPREADSHEET_ID, range='Pandamart OLA').execute()
    sheet.values().update(spreadsheetId=SAMPLE_SPREADSHEET_ID, range="'Pandamart OLA'!A1", valueInputOption='USER_ENTERED', body={'values': [df_acc_wrt.columns.values.tolist()] + df_acc_wrt.values.tolist()}).execute()
    sheet.values().update(spreadsheetId=SAMPLE_SPREADSHEET_ID, range="'Pandamart OLA'!M1", valueInputOption='USER_ENTERED', body={'values': [ola_df_wrt.columns.values.tolist()] + ola_df_wrt.values.tolist()}).execute()
    
    # # delay
    # if j < (len_urls - 1): time.sleep(randint(80, 100))

# stats
elapsed_time = time.time() - start_time
print("Elapsed time to report (mins): " + str(round(elapsed_time / 60.00, 2)))


# In[ ]:




