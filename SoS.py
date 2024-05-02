#!/usr/bin/env python
# coding: utf-8

# In[ ]:


## Chaldal SoS

# import
import pandas as pd
import duckdb
from selenium import webdriver
# from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
from googleapiclient.discovery import build
from google.oauth2 import service_account
import time

# accumulators
start_time = time.time()
df_acc = pd.DataFrame()

# particulars
keywords = ['conditioner', 'handwash', 'bodywash', 'facewash', 'lotion', 'cream', 'toothpaste', 'dishwash', 'toilet clean', 'soup', 'shampoo', 'health drink', 'washing powder', 'wash liquid', 'detergent', 'moisturizer', 'soap', 'petroleum jelly', 'hair oil', 'germ kill']
brands = ['Boost Health', 'Boost Drink', 'Boost Jar', 'Clear Shampoo', 'Simple Fac', 'Simple Mask', 'Pepsodent', 'Brylcreem', 'Bru Coffee', 'St. Ives', 'St.Ives', 'Horlicks', 'Sunsilk', 'Sun Silk', 'Lux', 'Ponds', "Pond's", 'Closeup', 'Close Up', 'Cif', 'Dove', 'Maltova', 'Domex', 'Clinic Plus', 'Tresemme', 'Tresemmé', 'GlucoMax', 'Knorr', 'Glow Lovely', 'Fair Lovely', 'Glow Handsome', 'Wheel Wash', 'Axe Body', 'Pureit', 'Lifebuoy', 'Surf Excel', 'Vaseline', 'Vim', 'Rin']
    
# subsequence
def is_subseq(x, y):
    it = iter(y)
    return all(any(c == ch for c in it) for ch in x)
    
# preference
options = webdriver.ChromeOptions()
options.add_argument('ignore-certificate-errors')

# open window
driver = webdriver.Chrome(options=options)
driver.maximize_window()

# url
for k in keywords:
    print("Scraping for keyword: " + k)
    url = "https://chaldal.com/search/" + k
    driver.get(url)

    # scroll
    last_height = driver.execute_script("return document.body.scrollHeight")
    while True:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(5)
        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == last_height: break
        last_height = new_height

    # soup
    soup_init = BeautifulSoup(driver.page_source, 'html.parser')
    soup = soup_init.find_all("div", attrs={"class": "product"})

    # scrape
    skus = []
    quants = []
    prices = []
    prices_if_discounted = []
    options = []
    if_ubl = [] 
    for s in soup:
        # sku
        try: val = s.find("div", attrs={"class": "name"}).get_text()
        except: val = None
        skus.append(val)
        # quantity
        try: val = s.find("div", attrs={"class": "subText"}).get_text()
        except: val = None
        quants.append(val)
        # price
        try: val = float(s.find("div", attrs={"class": "price"}).get_text().split()[1].replace(',', ''))
        except: val = None
        prices.append(val)
        # discount
        try: val = float(s.find("div", attrs={"class": "discountedPrice"}).get_text().split()[1].replace(',', ''))
        except: val = None
        prices_if_discounted.append(val)
        # option
        try: val = s.find("p", attrs={"class": "buyText"}).get_text() 
        except: val = None
        options.append(val)

    # accumulate
    df = pd.DataFrame()
    df['sku'] = [str(s) + ' ' + str(q) for s, q in zip(skus, quants)]
    df['quantity'] = quants
    df['price'] = prices
    df['price_if_discounted'] = prices_if_discounted
    df['option'] = options
    df['pos_in_pg'] = list(range(1, df.shape[0]+1))
    df['keyword'] = k
    df['relevance'] = ['relevant' if is_subseq(k.replace(' ', ''), s.lower()) else 'irrelevant' for s in skus]
    # Unilever
    sku_count = len(skus)
    for i in range(0, sku_count):
        if_ubl.append(None)
        for b in brands:
            bb = b.split()
            if len(bb) == 1: bb.append('')
            if bb[0].lower() + ' ' in skus[i].lower() and bb[1].lower() in skus[i].lower(): if_ubl[i] = b
    df['brand_unilever'] = if_ubl

    # record
    df['report_time'] = time.strftime('%d-%b-%y, %I:%M %p')
    df_acc = df_acc._append(df).fillna('')

# close window
driver.close()

# SoS
qry = '''
select
    keyword, 
    count(1) results, 
    count(case when relevance='relevant' then 1 else null end) relevant_results, 
    count(case when relevance='relevant' and brand_unilever!='' then 1 else null end) relevant_results_ubl,
    round(count(case when relevance='relevant' and brand_unilever!='' then 1 else null end)*1.00/count(case when relevance='relevant' then 1 else null end), 4) ubl_sos, 
    count(case when relevance='relevant' and brand_unilever!='' and pos_in_pg<11 then 1 else null end) ubl_sos_top10,
    max(report_time) report_time
from df_acc
group by 1
'''
sos_df = duckdb.query(qry).df().fillna('')

# credentials
SERVICE_ACCOUNT_FILE = 'read-write-to-gsheet-apis-1-04f16c652b1e.json'
SAMPLE_SPREADSHEET_ID = '1gkLRp59RyRw4UFds0-nNQhhWOaS4VFxtJ_Hgwg2x2A0'
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

# API
creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
service = build('sheets', 'v4', credentials=creds)
sheet = service.spreadsheets()

# update
sheet.values().clear(spreadsheetId=SAMPLE_SPREADSHEET_ID, range='Chaldal SoS').execute()
sheet.values().update(spreadsheetId=SAMPLE_SPREADSHEET_ID, range="Chaldal SoS!A1", valueInputOption='USER_ENTERED', body={'values': [df_acc.columns.values.tolist()] + df_acc.values.tolist()}).execute()
sheet.values().update(spreadsheetId=SAMPLE_SPREADSHEET_ID, range="Chaldal SoS!L1", valueInputOption='USER_ENTERED', body={'values': [sos_df.columns.values.tolist()] + sos_df.values.tolist()}).execute()

# stats
# display(df_acc.head(5))
print("Listings in result: " + str(df_acc.shape[0]))
elapsed_time = time.time() - start_time
print("Elapsed time to report (mins): " + str(round(elapsed_time / 60.00, 2)))


# In[1]:


# Pandamart SoS

# import
import pandas as pd
import duckdb
from selenium import webdriver
# from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
from googleapiclient.discovery import build
from google.oauth2 import service_account
import time

# accumulators
start_time = time.time()
df_acc = pd.DataFrame()

# particulars
keywords = ['conditioner', 'handwash', 'bodywash', 'facewash', 'lotion', 'face cream', 'toothpaste', 'dishwash', 'toilet clean', 'soup', 'shampoo', 'health drink', 'detergent', 'moisturizer', 'soap', 'petroleum jelly', 'hair oil', 'germ kill']
brands = ['Boost Health', 'Boost Drink', 'Boost Jar', 'Clear Shampoo', 'Simple Fac', 'Simple Mask', 'Pepsodent', 'Brylcreem', 'Bru Coffee', 'St. Ives', 'St.Ives', 'Horlicks', 'Sunsilk', 'Sun Silk', 'Lux', 'Ponds', "Pond's", 'Closeup', 'Close Up', 'Cif', 'Dove', 'Maltova', 'Domex', 'Clinic Plus', 'Tresemme', 'Tresemmé', 'GlucoMax', 'Knorr', 'Glow Lovely', 'Fair Lovely', 'Glow Handsome', 'Wheel Wash', 'Axe Body', 'Pureit', 'Lifebuoy', 'Surf Excel', 'Vaseline', 'Vim', 'Rin']

# preference
options = webdriver.ChromeOptions()
options.add_argument('ignore-certificate-errors')

# open window
driver = webdriver.Chrome(options=options)
driver.maximize_window()
driver.get("https://www.foodpanda.com.bd/darkstore/w2lx/pandamart-gulshan-w2lx")

# keyword
for k in keywords:
    print("Scraping for keyword: " + k)
    elem = driver.find_element(By.XPATH, '//*[@id="groceries-menu-react-root"]/div/div/div[2]/div/section/div[3]/div/div/div/div/div[1]/input')
    elem.send_keys(k + "\n")

    # scroll
    last_height = driver.execute_script("return document.body.scrollHeight")
    while True:
        time.sleep(5)
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        new_height = driver.execute_script("return document.body.scrollHeight")
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
    if_ubl = [] 
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
    df['pos_in_pg'] = list(range(1, df.shape[0]+1))
    df['keyword'] = k
  
    # Unilever
    sku_count = len(sku)
    for i in range(0, sku_count):
        if_ubl.append(None)
        for b in brands:
            bb = b.split()
            if len(bb) == 1: bb.append('')
            if bb[0].lower() + ' ' in sku[i].lower() and bb[1].lower() in sku[i].lower(): if_ubl[i] = b
    df['brand_unilever'] = if_ubl

    # record
    df['report_time'] = time.strftime('%d-%b-%y, %I:%M %p')
    df_acc = df_acc._append(df).fillna('')
    
    # back
    driver.back()

# close window
driver.close()

# SoS
qry = '''
select
    keyword, 
    count(1) results, 
    count(case when brand_unilever!='' then 1 else null end) results_ubl,
    round(count(case when brand_unilever!='' then 1 else null end)*1.00/count(1), 4) ubl_sos,
    count(case when brand_unilever!='' and pos_in_pg<11 then 1 else null end) ubl_sos_top10,
    max(report_time) report_time
from df_acc
group by 1
'''
sos_df = duckdb.query(qry).df().fillna('')

# credentials
SERVICE_ACCOUNT_FILE = 'read-write-to-gsheet-apis-1-04f16c652b1e.json'
SAMPLE_SPREADSHEET_ID = '1gkLRp59RyRw4UFds0-nNQhhWOaS4VFxtJ_Hgwg2x2A0'
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

# API
creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
service = build('sheets', 'v4', credentials=creds)
sheet = service.spreadsheets()

# update
sheet.values().clear(spreadsheetId=SAMPLE_SPREADSHEET_ID, range='Pandamart SoS').execute()
sheet.values().update(spreadsheetId=SAMPLE_SPREADSHEET_ID, range="Pandamart SoS!A1", valueInputOption='USER_ENTERED', body={'values': [df_acc.columns.values.tolist()] + df_acc.values.tolist()}).execute()
sheet.values().update(spreadsheetId=SAMPLE_SPREADSHEET_ID, range="Pandamart SoS!J1", valueInputOption='USER_ENTERED', body={'values': [sos_df.columns.values.tolist()] + sos_df.values.tolist()}).execute()

# stats
# display(df_acc.head(5))
print("Listings in result: " + str(df_acc.shape[0]))
elapsed_time = time.time() - start_time
print("Elapsed time to report (mins): " + str(round(elapsed_time / 60.00, 2)))


# In[2]:


## Shajgoj SoS

# import
import pandas as pd
import duckdb
from selenium import webdriver
# from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
from googleapiclient.discovery import build
from google.oauth2 import service_account
import time

# accumulators
start_time = time.time()
df_acc = pd.DataFrame()

# subsequence
def is_subseq(x, y):
    it = iter(y)
    return all(any(c == ch for c in it) for ch in x)

# particulars
keywords = ['conditioner', 'handwash', 'bodywash', 'facewash', 'lotion', 'cream', 'toothpaste', 'shampoo', 'moisturizer', 'soap', 'petroleumjelly', 'hairoil']
brands = ['Clear Shampoo', 'Simple Fac', 'Simple Mask', 'Pepsodent', 'Brylcreem', 'St. Ives', 'St.Ives', 'Sunsilk', 'Sun Silk', 'Lux', 'Ponds', "Pond's", 'Closeup', 'Close Up', 'Cif', 'Dove', 'Clinic Plus', 'Tresemme', 'Tresemmé', 'Glow Lovely', 'Fair Lovely', 'Glow Handsome', 'Axe Body', 'Lifebuoy', 'Vaseline']

# preference
options = webdriver.ChromeOptions()
options.add_argument('ignore-certificate-errors')

# open window
driver = webdriver.Chrome(options=options)
driver.maximize_window()

# keyword
for k in keywords:
    print("Scraping for keyword: " + k)
    i = 0
    skus = []
    
    # url
    while(1):
        driver.get("https://shop.shajgoj.com/shop/#q=" + k + "&hPP=21&idx=wp_posts_product&p=" + str(i) + "&post_type=product&is_v=1")
        i = i + 1
        time.sleep(3)
        
        # initialize
        relevance = 'irrelevant'
        soup_init = BeautifulSoup(driver.page_source, 'html.parser').find_all("div", attrs={"class": "ais-infinite-hits--item ais-hits--item"})
        skus_init = len(skus)

        # soup
        for s in soup_init: 
            sku = s.find("a", attrs={"class": "product_title"}).get_text()
            if sku not in skus: skus.append(sku)
        for s in skus[skus_init:]:
            if is_subseq(k.replace(' ', ''), s.lower()): 
                print(s)
                relevance = 'relevant'
        if relevance == 'irrelevant' or skus_init > 250: break
        soup = soup_init

    # scrape
    skus = []
    quants = []
    prices = []
    prices_app = []
    prices_before = []
    offers = []
    options = []
    ratings = []
    if_ubl = []
    for s in soup:

        # sku
        try: val = s.find("a", attrs={"class": "product_title"}).get_text()
        except: val = None
        skus.append(val)

        # quantity
        try: val = s.find("div", attrs={"class": "alg-variation"}).get_text().strip()
        except: val = None
        quants.append(val)

        # price
        try: val = s.find("span", attrs={"class": "alg-hit__currentprice product_price"}).get_text().strip().split()[1]
        except: val = None
        prices.append(val)

        # app price
        try: val = s.find("div", attrs={"class": "alg-app-price"}).get_text().split()[-1]
        except: val = None
        prices_app.append(val)

        # price before
        try: val = s.find("span", attrs={"class": "alg-hit__previousprice product_price"}).get_text().strip().split()[1]
        except: val = None
        prices_before.append(val)

        # offer
        try: val = s.find("div", attrs={"class": "alg-product-ribbon-container"}).get_text().strip()
        except: val = None
        offers.append(val)

        # option
        try: val = s.find("div", attrs={"class": "alg-hit__actions"}).get_text().strip()
        except: val = None
        options.append(val)

        # rating
        try: val = float(s.find("span", attrs={"class": "alg-rating"})["style"][6:-2])/20
        except: val = None
        ratings.append(val)

    # accumulate
    df = pd.DataFrame()
    df['basepack'] = skus
    df['sku'] = [s + ' ' + q for s, q in zip(skus, quants)]
    df['quantity'] = quants
    df['price'] = prices
    df['price_app'] = prices_app
    df['price_before'] = prices_before
    df['offer'] = offers
    df['option'] = options
    df['rating'] = ratings
    df['keyword'] = k
    df['relevance'] = ['relevant' if is_subseq(k.replace(' ', ''), s.lower()) else 'irrelevant' for s in skus]

    # Unilever
    sku_count = len(skus)
    for i in range(0, sku_count):
        if_ubl.append(None)
        for b in brands:
            bb = b.split()
            if len(bb) == 1: bb.append('')
            if bb[0].lower() + ' ' in skus[i].lower() and bb[1].lower() in skus[i].lower(): if_ubl[i] = b
    df['brand_unilever'] = if_ubl

    # record
    df = duckdb.query('''select distinct * from df''').df()
    df['pos_in_pg'] = list(range(1, df.shape[0]+1))
    df['report_time'] = time.strftime('%d-%b-%y, %I:%M %p')
    df_acc = df_acc._append(df).fillna('')
    
# close window
driver.close()

# SoS
qry = '''
select
    keyword, 
    count(1) results, 
    count(case when relevance='relevant' then 1 else null end) relevant_results, 
    count(case when relevance='relevant' and brand_unilever!='' then 1 else null end) relevant_results_ubl,
    round(count(case when relevance='relevant' and brand_unilever!='' then 1 else null end)*1.00/count(case when relevance='relevant' then 1 else null end), 4) ubl_sos, 
    count(case when relevance='relevant' and brand_unilever!='' and pos_in_pg<11 then 1 else null end) ubl_sos_top10,
    max(report_time) report_time
from df_acc
group by 1
'''
sos_df = duckdb.query(qry).df().fillna('')

# credentials
SERVICE_ACCOUNT_FILE = 'read-write-to-gsheet-apis-1-04f16c652b1e.json'
SAMPLE_SPREADSHEET_ID = '1gkLRp59RyRw4UFds0-nNQhhWOaS4VFxtJ_Hgwg2x2A0'
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

# APIs
creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
service = build('sheets', 'v4', credentials=creds)
sheet = service.spreadsheets()

# update
sheet.values().clear(spreadsheetId=SAMPLE_SPREADSHEET_ID, range='Shajgoj SoS').execute()
sheet.values().update(spreadsheetId=SAMPLE_SPREADSHEET_ID, range="Shajgoj SoS!A1", valueInputOption='USER_ENTERED', body={'values': [df_acc.columns.values.tolist()] + df_acc.values.tolist()}).execute()
sheet.values().update(spreadsheetId=SAMPLE_SPREADSHEET_ID, range="Shajgoj SoS!P1", valueInputOption='USER_ENTERED', body={'values': [sos_df.columns.values.tolist()] + sos_df.values.tolist()}).execute()

# stats
# display(df_acc.head(5))
print("Total SKUs found: " + str(df_acc.shape[0]))
elapsed_time = time.time() - start_time
print("Elapsed time to report (mins): " + str(round(elapsed_time / 60.00, 2)))


# In[6]:


# Daraz SoS

# import
import pandas as pd
import duckdb
from selenium import webdriver
# from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver import ActionChains
from bs4 import BeautifulSoup
from googleapiclient.discovery import build
from google.oauth2 import service_account
import time

# accumulators
start_time = time.time()
df_acc = pd.DataFrame()

# particulars
keywords = ['conditioner', 'handwash', 'bodywash', 'facewash', 'lotion', 'cream', 'toothpaste', 'dishwash', 'toilet clean', 'soup', 'shampoo', 'health drink', 'detergent', 'moisturizer', 'soap', 'petroleum jelly', 'hair oil', 'germ kill']
brands = ['Boost Health', 'Boost Drink', 'Boost Jar', 'Clear Shampoo', 'Simple Fac', 'Simple Mask', 'Pepsodent', 'Brylcreem', 'Bru Coffee', 'St. Ives', 'St.Ives', 'Horlicks', 'Sunsilk', 'Sun Silk', 'Lux', 'Ponds', "Pond's", 'Closeup', 'Close Up', 'Cif', 'Dove', 'Maltova', 'Domex', 'Clinic Plus', 'Tresemme', 'Tresemmé', 'GlucoMax', 'Knorr', 'Glow Lovely', 'Fair Lovely', 'Glow Handsome', 'Wheel Wash', 'Axe Body', 'Pureit', 'Lifebuoy', 'Surf Excel', 'Vaseline', 'Vim', 'Rin']

# subsequence
def is_subseq(x, y):
    it = iter(y)
    return all(any(c == ch for c in it) for ch in x)

# preference
options = webdriver.ChromeOptions()
options.add_argument('ignore-certificate-errors')

# open window
driver = webdriver.Chrome(options=options)
driver.maximize_window()
driver.get('https://www.daraz.com.bd/')

# keyword
for k in keywords:
    print("Scraping for keyword: " + k)
    elem = driver.find_element(By.ID, "q")
    elem.send_keys(Keys.CONTROL + "a")
    elem.send_keys(Keys.DELETE)
    elem.send_keys(k + "\n")

    # initialize
    pg = 1
    pos = 0
    new_skus = set()
    while(1):

        # scroll
        last_height = driver.execute_script("return document.body.scrollHeight")
        while True:
            time.sleep(5)
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            new_height = driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height: break
            last_height = new_height

        # soup
        soup_init = BeautifulSoup(driver.page_source, "html.parser")
        soup = soup_init.find_all("div", attrs={"class": "gridItem--Yd0sa"})

        # scrape
        sku = []
        current_price = []
        original_price = []
        offer = []
        rating = []
        reviews = []
        in_mall = []
        in_mart = []
        position = []
        sku_count = len(soup)
        for i in range(0, sku_count):            
            
            # SKU
            try: val = soup[i].find("div", attrs={"id": "id-title"}).get_text()
            except: val = None
            sku.append(val)
            # current price
            try: val = soup[i].find("span", attrs={"class": "currency--GVKjl"}).get_text()
            except: val = None
            current_price.append(val)
            # original price
            try: val = soup[i].find("del", attrs={"class": "currency--GVKjl"}).get_text()[2:]
            except: val = None
            original_price.append(val)
            # offer
            try: val = soup[i].find("div", attrs={"class": "voucher-wrapper--vCNzH"}).get_text()
            except: val = None
            offer.append(val)
            # rating    
            try: val = soup[i].find("span", attrs={"class": "ratig-num--KNake rating--pwPrV"}).get_text()
            except: val = None
            rating.append(val)
            # reviews
            try: val = soup[i].find("span", attrs={"class": "rating__review--ygkUy"}).get_text()[1:-1]
            except: val = None
            reviews.append(val)
            # mall
            in_mall.append(1)
            try: soup[i].find("i", attrs={"style": "background-image: url(&quot;https://img.alicdn.com/imgextra/i2/O1CN01m9OC6a1UK86X51Dcq_!!6000000002498-2-tps-108-54.png&quot;); width: 32px; height: 16px; vertical-align: text-bottom;"})["class"]
            except: in_mall[i] = 0
            # mart
            in_mart.append(1)
            try: soup[i].find("i", attrs={"style": "background-image: url(&quot;https://img.alicdn.com/imgextra/i1/O1CN01gS7Ros1VI7zYtUDwQ_!!6000000002629-2-tps-64-32.png&quot;); width: 32px; height: 16px; vertical-align: text-bottom;"})["class"]
            except: in_mart[i] = 0
            # position
            pos = pos + 1
            position.append(pos)
            
        # novelty
        skus_before = len(new_skus)
        for s in sku: 
            if is_subseq(k.replace(' ', ''), s.lower()): 
                new_skus.add(s)
        if len(new_skus) == skus_before: break

        # accumulate 
        df = pd.DataFrame()
        df['sku'] = sku
        df['current_price'] = current_price
        df['original_price'] = original_price
        df['offer'] = offer
        df['rating'] = rating
        df['reviews'] = reviews
        df['in_mall'] = in_mall
        df['in_mart'] = in_mart
        df['keyword'] = k
        df['relevance'] = ['relevant' if is_subseq(k.replace(' ', ''), s.lower()) else 'irrelevant' for s in sku]
        df['pg_no'] = pg
        df['position'] = position

        # Unilever
        if_ubl = []
        skus = len(sku)
        for i in range(0, skus):
            if_ubl.append(None)
            for b in brands:
                bb = b.split()
                if len(bb) == 1: bb.append('')
                if bb[0].lower() + ' ' in sku[i].lower() and bb[1].lower() in sku[i].lower(): if_ubl[i] = b
        df['brand_unilever'] = if_ubl

        # record
        df['report_time'] = time.strftime('%d-%b-%y, %I:%M %p')
        df_acc = df_acc._append(df).fillna('')

        # next page
        elem = driver.find_element(By.CLASS_NAME, "ant-pagination-next")
        ActionChains(driver).move_to_element(elem).click().perform()

        # limit
        pg = pg + 1
        if pg == 16: break
    
# close window
driver.close()

# SoS
qry = '''
select
    keyword, 
    count(1) results, 
    count(case when relevance='relevant' then 1 else null end) relevant_results, 
    count(case when relevance='relevant' and brand_unilever!='' then 1 else null end) relevant_results_ubl,
    round(count(case when relevance='relevant' and brand_unilever!='' then 1 else null end)*1.00/count(case when relevance='relevant' then 1 else null end), 4) ubl_sos, 
    count(case when relevance='relevant' and brand_unilever!='' and position<11 then 1 else null end) ubl_sos_top10,
    max(report_time) report_time
from df_acc
group by 1
'''
sos_df = duckdb.query(qry).df().fillna('')

# credentials
SERVICE_ACCOUNT_FILE = 'read-write-to-gsheet-apis-1-04f16c652b1e.json'
SAMPLE_SPREADSHEET_ID = '1gkLRp59RyRw4UFds0-nNQhhWOaS4VFxtJ_Hgwg2x2A0'
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

# APIs
creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
service = build('sheets', 'v4', credentials=creds)
sheet = service.spreadsheets()

# update
sheet.values().clear(spreadsheetId=SAMPLE_SPREADSHEET_ID, range='Daraz SoS').execute()
sheet.values().update(spreadsheetId=SAMPLE_SPREADSHEET_ID, range="Daraz SoS!A1", valueInputOption='USER_ENTERED', body={'values': [df_acc.columns.values.tolist()] + df_acc.values.tolist()}).execute()
sheet.values().update(spreadsheetId=SAMPLE_SPREADSHEET_ID, range="Daraz SoS!P1", valueInputOption='USER_ENTERED', body={'values': [sos_df.columns.values.tolist()] + sos_df.values.tolist()}).execute()

# stats
# display(df_acc.head(5))
print("Total SKUs found: " + str(df_acc.shape[0]))
elapsed_time = time.time() - start_time
print("Elapsed time to report (mins): " + str(round(elapsed_time / 60.00, 2)))
        


# In[ ]:




