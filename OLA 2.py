#!/usr/bin/env python
# coding: utf-8

# In[1]:


## Chaldal

# import
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver import ActionChains
from bs4 import BeautifulSoup
import pandas as pd
import duckdb
from googleapiclient.discovery import build
from google.oauth2 import service_account
import time

# accumulators
start_time = time.time()
df_acc = pd.DataFrame()

# list
ubl_skus_df = pd.read_csv('Eagle Eye - Updated SKU List (All Platforms).csv')

# preference
options = webdriver.ChromeOptions()
options.add_argument('ignore-certificate-errors')

# open window
driver = webdriver.Chrome(options=options)
driver.maximize_window()

# url
i = 0
while(1): 
    url = "https://www.chaldal.com/Unilever"
    driver.get(url)

    # location
    elem = driver.find_element(By.CLASS_NAME, "metropolitanAreaName")
    elem.click()
    elem = driver.find_element(By.ID, "Group_47542")
    elem.click()
    time.sleep(4)
    elems = driver.find_elements(By.CLASS_NAME, "cityImageContainer")
    achains = ActionChains(driver)
    try: achains.move_to_element(elems[i]).click().perform()
    except: break
    time.sleep(6)
    loc = driver.find_element(By.CLASS_NAME, "metropolitanAreaName").text.replace("\n", " ")
    print("Scraping from location: " + loc)
    i = i + 1

    # scroll
    SCROLL_PAUSE_TIME = 5
    last_height = driver.execute_script("return document.body.scrollHeight")
    while True:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(SCROLL_PAUSE_TIME)
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
    for s in soup:
        # sku
        try: val = s.find("div", attrs={"class": "name"}).get_text()
        except: val = None
        skus.append(val)
        # quantity
        try: val = s.find("div", attrs={"class": "subText"}).get_text() #.replace(" ", "")
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
    df['basepack'] = skus
    df['sku'] = [str(s) + ' ' + str(q) for s, q in zip(skus, quants)]
    df['quantity'] = quants
    df['price'] = prices
    df['price_if_discounted'] = prices_if_discounted
    df['option'] = options
    df['pos_in_pg'] = list(range(1, df.shape[0]+1))

    # OOS
    qry = '''
    with 
        tbl3 as
        (select * 
        from 
            (select Category category, Brand brand, "Updated Perfect Name" sku
            from ubl_skus_df
            where Platform='Chaldal'
            ) tbl1 
            left join 
            df tbl2 using(sku)
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
        select null category, null brand, sku, basepack, quantity, price, price_if_discounted, option, pos_in_pg    
        from df 
        where sku not in(select distinct sku from tbl3)
        ) tbl4
    '''
    df = duckdb.query(qry).df()
    df['location'] = loc
    df['report_time'] = time.strftime('%d-%b-%y, %I:%M %p')
    
    # append
    df_acc = df_acc._append(df)
    
    # wait
    time.sleep(35)

# close window
driver.close()

# csv
df_acc.to_csv("chaldal_OLA_data.csv", index=False)

# credentials
SERVICE_ACCOUNT_FILE = 'read-write-to-gsheet-apis-1-04f16c652b1e.json'
SAMPLE_SPREADSHEET_ID = '1gkLRp59RyRw4UFds0-nNQhhWOaS4VFxtJ_Hgwg2x2A0'
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

# APIs
creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
service = build('sheets', 'v4', credentials=creds)
sheet = service.spreadsheets()

# update
resultClear = sheet.values().clear(spreadsheetId=SAMPLE_SPREADSHEET_ID, range='Chaldal OLA').execute()
qry = '''
select
    location, 
    count(category) skus_enlisted, 
    count(case when pos_in_pg is not null then category else null end) skus_online,
    round(count(case when pos_in_pg is not null then category else null end)*1.00/count(category), 4) ola
from df_acc
where category is not null
group by 1
'''
ola_df = duckdb.query(qry).df()
ola_df['report_time'] = time.strftime('%d-%b-%y, %I:%M %p')
request = sheet.values().update(spreadsheetId=SAMPLE_SPREADSHEET_ID, range="'Chaldal OLA'!N1", valueInputOption='USER_ENTERED', body={'values': [ola_df.columns.values.tolist()] + ola_df.values.tolist()}).execute()
df_acc = df_acc.fillna('')
request = sheet.values().update(spreadsheetId=SAMPLE_SPREADSHEET_ID, range="'Chaldal OLA'!A1", valueInputOption='USER_ENTERED', body={'values': [df_acc.columns.values.tolist()] + df_acc.values.tolist()}).execute()

# stats
# display(df_acc.head(5))
print("Listings in result: " + str(df_acc.shape[0]))
elapsed_time = time.time() - start_time
print("Elapsed time to report (mins): " + str(round(elapsed_time / 60.00, 2)))


# In[2]:


## Shajgoj

# import
from selenium import webdriver
from bs4 import BeautifulSoup
import pandas as pd
import duckdb
from googleapiclient.discovery import build
from google.oauth2 import service_account
import time

# accumulators
start_time = time.time()

# list
ubl_skus_df = pd.read_csv('Eagle Eye - Updated SKU List (All Platforms).csv')

# preference
options = webdriver.ChromeOptions()
options.add_argument('ignore-certificate-errors')

# open window
driver = webdriver.Chrome(options=options)
driver.maximize_window()

# url
url = "https://shop.shajgoj.com/unilever-bangladesh/"
driver.get(url)
time.sleep(10)

# soup
soup_init = BeautifulSoup(driver.page_source, 'html.parser')
soup = soup_init.find_all("div", attrs={"class": "product_page shajgoj_upsell"})

# close window
driver.close()

# scrape
skus = []
quants = []
prices = []
prices_if_discounted = []
offers = []
options = []
for s in soup:
    # sku
    try: val = s.find("div", attrs={"class": "upsell_name"}).get_text()
    except: val = None
    skus.append(val)
    # quantity
    try: val = s.find("div", attrs={"class": "upsell_weight"}).get_text()[0:-3].replace('(', '').replace(')', '')
    except: val = None
    quants.append(val)
    # price
    try: val = s.find("div", attrs={"class": "upsell_price"}).get_text().split()[1]
    except: val = None
    prices.append(val)
    # discount
    try: val = s.find("div", attrs={"class": "upsell_price"}).get_text().split()[3]
    except: val = None
    prices_if_discounted.append(val)
    # offer
    try: val = s.find("span", attrs={"class": "freq_sale_ribbon"}).get_text()
    except: val = None
    offers.append(val)
    # option
    try: val = s.find("button", attrs={"type": "submit"}).get_text()
    except: val = s.find("button", attrs={"class": "request_restock product_recom"}).get_text()
    options.append(val)

# accumulate
df = pd.DataFrame()
df['basepack'] = skus
df['sku'] = [s + ' ' + q for s, q in zip(skus, quants)]
df['quantity'] = quants
df['price'] = prices
df['price_if_discounted'] = prices_if_discounted
df['pos_in_pg'] = list(range(1, df.shape[0]+1))
df['offer'] = offers
df['option'] = options
df['ola_status'] = ['enlisted + online' if opt == 'ADD TO CART' else 'enlisted + offline' for opt in options]
df['report_time'] = time.strftime('%d-%b-%y, %I:%M %p')

# csv
df.to_csv("shajgoj_OLA_data.csv", index=False)

# credentials
SERVICE_ACCOUNT_FILE = 'read-write-to-gsheet-apis-1-04f16c652b1e.json'
SAMPLE_SPREADSHEET_ID = '1gkLRp59RyRw4UFds0-nNQhhWOaS4VFxtJ_Hgwg2x2A0'
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

# APIs
creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
service = build('sheets', 'v4', credentials=creds)
sheet = service.spreadsheets()

# update
resultClear = sheet.values().clear(spreadsheetId=SAMPLE_SPREADSHEET_ID, range='Shajgoj OLA').execute()
qry = '''
select
    sum(1) skus_enlisted, 
    sum(case when option='ADD TO CART' then 1 else 0 end) skus_online,
    round(sum(case when option='ADD TO CART' then 1 else 0 end)*1.00/sum(1), 4) ola
from df
'''
ola_df = duckdb.query(qry).df()
ola_df['report_time'] = time.strftime('%d-%b-%y, %I:%M %p')
request = sheet.values().update(spreadsheetId=SAMPLE_SPREADSHEET_ID, range="'Shajgoj OLA'!L1", valueInputOption='USER_ENTERED', body={'values': [ola_df.columns.values.tolist()] + ola_df.values.tolist()}).execute()
df = df.fillna('')
request = sheet.values().update(spreadsheetId=SAMPLE_SPREADSHEET_ID, range="'Shajgoj OLA'!A1", valueInputOption='USER_ENTERED', body={'values': [df.columns.values.tolist()] + df.values.tolist()}).execute()

# stats
# display(df.head(5))
print("Total SKUs found: " + str(df.shape[0]))
elapsed_time = time.time() - start_time
print("Elapsed time to report (mins): " + str(round(elapsed_time / 60.00, 2)))


# In[37]:


## Daraz

# import
import pandas as pd
import duckdb
from selenium import webdriver
from bs4 import BeautifulSoup
from googleapiclient.discovery import build
from google.oauth2 import service_account
import time

# accumulators
start_time = time.time()
df_acc = pd.DataFrame()

# list
ubl_skus_df = pd.read_csv('Eagle Eye - Updated SKU List (All Platforms).csv')

# preference
options = webdriver.ChromeOptions()
options.add_argument('ignore-certificate-errors')

# open window
driver = webdriver.Chrome(options=options)
driver.maximize_window()

# link
pg = 0
while(1): 
    pg = pg + 1
    link = "https://www.daraz.com.bd/unilever-bangladesh/?from=wangpu&lang=en&langFlag=en&page=" + str(pg) + "&pageTypeId=2&q=All-Products"
    driver.get(link)

    # scroll
    SCROLL_PAUSE_TIME = 5
    last_height = driver.execute_script("return document.body.scrollHeight")
    while True:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(SCROLL_PAUSE_TIME)
        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == last_height: break
        last_height = new_height

    # soup
    soup_init = BeautifulSoup(driver.page_source, "html.parser")
    soup = soup_init.find_all("div", attrs={"class": "gridItem--Yd0sa"})

    # page
    sku_count = len(soup)
    if sku_count == 0: break 
    print("Scraping from page: " + str(pg))
    
    # scrape
    sku = []
    current_price = []
    original_price = []
    offer = []
    rating = []
    reviews = []
    in_mall = []
    in_mart = []
    pos_in_pg = []
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
        try: soup[i].find("i", attrs={"class": "ic-dynamic-badge ic-dynamic-badge-lazMall ic-dynamic-group-1"})["style"]
        except: in_mall[i] = 0
        # mart
        in_mart.append(1)
        try: soup[i].find("i", attrs={"class": "ic-dynamic-badge ic-dynamic-badge-redmart ic-dynamic-group-1"})["style"]
        except: in_mart[i] = 0
        # position
        pos_in_pg.append(i+1)
        
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
    df['pg_no'] = pg
    df['pos_in_pg'] = pos_in_pg
    df_acc = df_acc._append(df)
    
# close window
driver.close()

# OOS
qry = '''
with 
    tbl3 as
    (select * 
    from 
        (select Category category, Brand brand, "Updated Perfect Name" sku
        from ubl_skus_df
        where Platform='Daraz'
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
    select null category, null brand, sku, current_price, original_price, offer, rating, reviews, in_mall, in_mart, pg_no, pos_in_pg
    from df_acc
    where sku not in(select distinct sku from tbl3)
    ) tbl4
'''
df = duckdb.query(qry).df()
df['location'] = 'Bangladesh'
df['report_time'] = time.strftime('%d-%b-%y, %I:%M %p')

# csv
df.to_csv("daraz_OLA_data.csv", index=False)

# credentials
SERVICE_ACCOUNT_FILE = 'read-write-to-gsheet-apis-1-04f16c652b1e.json'
SAMPLE_SPREADSHEET_ID = '1gkLRp59RyRw4UFds0-nNQhhWOaS4VFxtJ_Hgwg2x2A0'
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

# APIs
creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
service = build('sheets', 'v4', credentials=creds)
sheet = service.spreadsheets()

# update
resultClear = sheet.values().clear(spreadsheetId=SAMPLE_SPREADSHEET_ID, range='Daraz OLA').execute()
qry = '''
select
    count(distinct sku) skus_enlisted, 
    count(distinct case when pos_in_pg is not null then sku else null end) skus_online,
    round(count(distinct case when pos_in_pg is not null then sku else null end)*1.00/count(distinct sku), 4) ola
from df
where category is not null
'''
ola_df = duckdb.query(qry).df()
ola_df['report_time'] = time.strftime('%d-%b-%y, %I:%M %p')
request = sheet.values().update(spreadsheetId=SAMPLE_SPREADSHEET_ID, range="'Daraz OLA'!Q1", valueInputOption='USER_ENTERED', body={'values': [ola_df.columns.values.tolist()] + ola_df.values.tolist()}).execute()
df = df.fillna('')
request = sheet.values().update(spreadsheetId=SAMPLE_SPREADSHEET_ID, range="'Daraz OLA'!A1", valueInputOption='USER_ENTERED', body={'values': [df.columns.values.tolist()] + df.values.tolist()}).execute()

# stats
# display(df.head(5))
print("Listings in result: " + str(df.shape[0]))
elapsed_time = time.time() - start_time
print("Elapsed time to report (mins): " + str(round(elapsed_time / 60.00, 2)))


# In[36]:


## OHSOGO

# import
import pandas as pd
import duckdb
from selenium import webdriver
from bs4 import BeautifulSoup
from googleapiclient.discovery import build
from google.oauth2 import service_account
import time

# accumulators
start_time = time.time()
df_acc = pd.DataFrame()

# list
ubl_skus_df = pd.read_csv('Eagle Eye - Updated SKU List (All Platforms).csv')

# preference
options = webdriver.ChromeOptions()
options.add_argument('ignore-certificate-errors')

# open window
driver = webdriver.Chrome(options=options)
driver.maximize_window()

# link
pg = 0
while(1):
    pg = pg + 1  
    link = "https://ohsogo.com/collections/unilever?page=" + str(pg)
    driver.get(link)

    # scroll
    SCROLL_PAUSE_TIME = 5
    last_height = driver.execute_script("return document.body.scrollHeight")
    while True:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(SCROLL_PAUSE_TIME)
        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == last_height: break
        last_height = new_height

    # soup
    soup_init = BeautifulSoup(driver.page_source, "html.parser")
    soup = soup_init.find_all("div", attrs={"class": "card-wrapper card_space"})
    
    # scrape
    sku = []
    current_price = []
    original_price = []
    offer = []
    option = []
    rating = []
    brand = []
    pos_in_pg = []
    sku_count = len(soup)
    for i in range(0, sku_count):
        
        # SKU
        try: val = soup[i].find("a", attrs={"class": "card-information__text h4"}).get_text().strip()
        except: val = None
        sku.append(val)

        # current price
        try: val = soup[i].find("span", attrs={"class": "price__style"}).get_text().strip()
        except: val = None
        current_price.append(val)
        
        # original price
        try: val = soup[i].find("bdi", attrs={"class": "maximun_price"}).get_text()
        except: val = None
        original_price.append(val)
    
        # offer
        try: val = soup[i].find("span", attrs={"class": "badge badge--onsale"}).get_text().strip()
        except: val = None
        offer.append(val)
        
        # option
        try: val = soup[i].find("div", attrs={"class": "card-information__button"}).get_text().strip()
        except: val = None
        option.append(val)

        # rating
        try: val = soup[i].find("span", attrs={"class": "jdgm-prev-badge__stars"})["data-score"]
        except: val = None
        rating.append(val)
        
        # brand
        try: val = soup[i].find("div", attrs={"class": "card-article-info caption-with-letter-spacing"}).get_text()
        except: val = None
        brand.append(val)

        # position
        pos_in_pg.append(i+1)
        
    # page
    if len(sku) == 0: break
    print("Scraping from page: " + str(pg))
        
    # accumulate 
    df = pd.DataFrame()
    df['sku'] = sku
    df['current_price'] = current_price
    df['original_price'] = original_price
    df['offer'] = offer
    df['option'] = option
    df['rating'] = rating
    df['brand_scraped'] = brand
    df['pg_no'] = pg
    df['pos_in_pg'] = pos_in_pg
    df_acc = df_acc._append(df)

# close window
driver.close()

# OOS
qry = '''
with 
    tbl3 as
    (select * 
    from 
        (select Category category, Brand brand, "Updated Perfect Name" sku
        from ubl_skus_df
        where Platform='Ohsogo'
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
    select null category, null brand, sku, current_price, original_price, offer, option, rating, brand_scraped, pg_no, pos_in_pg
    from df_acc
    where sku not in(select distinct sku from tbl3)
    ) tbl4
'''
df_acc = duckdb.query(qry).df()
df_acc['report_time'] = time.strftime('%d-%b-%y, %I:%M %p')

# csv
df_acc.to_csv("ohsogo_OLA_data.csv", index=False)

# credentials
SERVICE_ACCOUNT_FILE = 'read-write-to-gsheet-apis-1-04f16c652b1e.json'
SAMPLE_SPREADSHEET_ID = '1gkLRp59RyRw4UFds0-nNQhhWOaS4VFxtJ_Hgwg2x2A0'
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

# APIs
creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
service = build('sheets', 'v4', credentials=creds)
sheet = service.spreadsheets()

# update
resultClear = sheet.values().clear(spreadsheetId=SAMPLE_SPREADSHEET_ID, range='Ohsogo OLA').execute()
qry = '''
select
    count(distinct sku) skus_enlisted, 
    count(distinct case when pos_in_pg is not null then sku else null end) skus_online,
    round(count(distinct case when pos_in_pg is not null then sku else null end)*1.00/count(distinct sku), 4) ola
from df_acc
where category is not null
'''
ola_df = duckdb.query(qry).df()
ola_df['report_time'] = time.strftime('%d-%b-%y, %I:%M %p')
request = sheet.values().update(spreadsheetId=SAMPLE_SPREADSHEET_ID, range="'Ohsogo OLA'!O1", valueInputOption='USER_ENTERED', body={'values': [ola_df.columns.values.tolist()] + ola_df.values.tolist()}).execute()
df_acc = df_acc.fillna('')
request = sheet.values().update(spreadsheetId=SAMPLE_SPREADSHEET_ID, range="'Ohsogo OLA'!A1", valueInputOption='USER_ENTERED', body={'values': [df_acc.columns.values.tolist()] + df_acc.values.tolist()}).execute()

# stats
# display(df_acc.head(5))
print("Listings in result: " + str(df_acc.shape[0]))
elapsed_time = time.time() - start_time
print("Elapsed time to report (mins): " + str(round(elapsed_time / 60.00, 2)))


# In[10]:


# ## Pandamart

# # import
# from bs4 import BeautifulSoup
# from selenium import webdriver
# from selenium.webdriver.common.by import By
# from selenium.webdriver.common.action_chains import ActionChains
# import pandas as pd
# import duckdb
# from googleapiclient.discovery import build
# from google.oauth2 import service_account
# from random import shuffle, randint
# import time

# # record
# start_time = time.time()

# # list
# ubl_skus_df = pd.read_csv('Eagle Eye - Updated SKU List (All Platforms).csv')

# # urls
# urls = [
#     'https://www.foodpanda.com.bd/darkstore/w2lx/pandamart-gulshan-w2lx',
#     'https://www.foodpanda.com.bd/darkstore/h5rj/pandamart-bashundhara',
#     'https://www.foodpanda.com.bd/darkstore/ta7z/pandamart-dhanmondi',
#     'https://www.foodpanda.com.bd/darkstore/n7ph/pandamart-uttara',
#     'https://www.foodpanda.com.bd/darkstore/v1ts/pandamart-mogbazar',
#     'https://www.foodpanda.com.bd/darkstore/q4hz/pandamart-sylhet-02',
#     'https://www.foodpanda.com.bd/darkstore/a2er/pandamart-khulna',
#     'https://www.foodpanda.com.bd/darkstore/w2nv/pandamart-chittagong-1'
# ]

# # credentials
# SERVICE_ACCOUNT_FILE = 'read-write-to-gsheet-apis-1-04f16c652b1e.json'
# SAMPLE_SPREADSHEET_ID = '1gkLRp59RyRw4UFds0-nNQhhWOaS4VFxtJ_Hgwg2x2A0'
# SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

# # API
# def sheet_api():
#     creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
#     service = build('sheets', 'v4', credentials=creds)
#     sheet = service.spreadsheets()
#     return sheet

# # preference
# options = webdriver.ChromeOptions()
# options.add_argument('ignore-certificate-errors')

# # open window
# len_urls = len(urls)
# for j in range(0, len_urls): 
#     driver = webdriver.Chrome(options=options)
#     driver.maximize_window()
    
#     # url
#     print("Scraping from: " + urls[j])
#     driver.get(urls[j])

#     # cross
#     elem = driver.find_element(By.CLASS_NAME, 'groceries-icon')
#     ActionChains(driver).click().perform()
    
#     # accumulator
#     df_acc = pd.DataFrame()

#     # banners
#     elems = driver.find_elements(By.CLASS_NAME, 'campaign-banners-swiper-link')
#     banners = len(elems)
#     for i in range(0, banners): 
#         elems = driver.find_elements(By.CLASS_NAME, 'campaign-banners-swiper-link')
#         ActionChains(driver).move_to_element(elems[i]).click().perform()

#         # scroll
#         last_height = driver.execute_script('return document.body.scrollHeight')
#         while True:
#             driver.execute_script('window.scrollTo(0, document.body.scrollHeight);')
#             time.sleep(4)
#             new_height = driver.execute_script('return document.body.scrollHeight')
#             if new_height == last_height: break
#             last_height = new_height

#         # soup
#         soup_init = BeautifulSoup(driver.page_source, 'html.parser')
#         soup = soup_init.find_all('div', attrs={'class', 'box-flex product-card-attributes'})

#         # scrape
#         sku = []
#         current_price = []
#         original_price = []
#         offer = []
#         for s in soup:
#             # sku
#             try: val = s.find('p', attrs={'class', 'product-card-name'}).get_text()
#             except: val = None
#             sku.append(val)
#             # current price
#             try: val = s.find("span", attrs={"data-testid", "product-card-price"}).get_text().split()[1]
#             except: val = None
#             current_price.append(val)
#             # original price
#             try: val = s.find("span", attrs={"data-testid", "product-card-price-before-discount"}).get_text().split()[1]
#             except: val = None
#             original_price.append(val)
#             # offer
#             try: val = s.find("span", attrs={"class", "bds-c-tag__label"}).get_text()
#             except: val = None
#             offer.append(val)

#         # accumulate
#         df = pd.DataFrame()
#         df['sku'] = sku
#         df['current_price'] = current_price
#         df['original_price'] = original_price
#         df['offer'] = offer
#         df['banner'] = i + 1
#         df['pos_in_pg'] = list(range(1, df.shape[0]+1))
#         df_acc = df_acc._append(df, ignore_index = True)

#         # back
#         driver.back()
        
#     # close window
#     driver.close()

#     # Unilever
#     brands = ['Boost Health', 'Boost Drink', 'Boost Jar', 'Clear Shampoo', 'Simple Fac', 'Simple Mask', 'Pepsodent', 'Brylcreem', 'Bru Coffee', 'St. Ives', 'St.Ives', 'Horlicks', 'Sunsilk', 'Sun Silk', 'Lux', 'Ponds', "Pond's", 'Closeup', 'Close Up', 'Cif', 'Dove', 'Maltova', 'Domex', 'Clinic Plus', 'Tresemme', 'Tresemmé', 'GlucoMax', 'Knorr', 'Glow Lovely', 'Fair Lovely', 'Glow Handsome', 'Wheel Wash', 'Axe Body', 'Pureit', 'Lifebuoy', 'Surf Excel', 'Vaseline', 'Vim', 'Rin']
#     skus = df_acc['sku'].tolist()
#     sku_count = len(skus)
#     if_ubl = [None] * sku_count
#     for i in range(0, sku_count):
#         for b in brands:
#             bb = b.split()
#             if len(bb) == 1: bb.append('')
#             if bb[0].lower() + ' ' in skus[i].lower() and bb[1].lower() in skus[i].lower(): if_ubl[i] = b
#     df_acc['brand_unilever'] = if_ubl
#     df_acc = df_acc.dropna(subset = ['brand_unilever'])[df_acc.columns]

#     # data
#     qry = '''
#     with 
#         tbl3 as
#         (select * 
#         from 
#             (select Category category, "Updated Perfect Name" sku
#             from ubl_skus_df
#             where Platform='Pandamart'
#             ) tbl1 
#             left join 
#             df_acc tbl2 using(sku)
#         ) 
#     select 
#         *, 
#         case 
#             when category is not null and pos_in_pg is not null then 'enlisted + online'
#             when category is not null and pos_in_pg is null then 'enlisted + offline'
#             when category is null and pos_in_pg is not null then 'unlisted + online'
#             when category is null and pos_in_pg is null then 'unlisted + offline'
#         end ola_status
#     from 
#         (select * from tbl3
#         union all
#         select null category, sku, current_price, original_price, offer, banner, pos_in_pg, brand_unilever
#         from df_acc
#         where sku not in(select distinct sku from tbl3)
#         ) tbl4
#     '''
#     df_acc = duckdb.query(qry).df()
#     df_acc['site'] = urls[j].split("/")[-1]
#     df_acc['report_time'] = time.strftime('%d-%b-%y, %I:%M %p')

#     # OLA
#     qry = '''
#     select
#         site,
#         count(distinct sku) skus_enlisted, 
#         count(distinct case when pos_in_pg is not null then sku else null end) skus_online,
#         round(count(distinct case when pos_in_pg is not null then sku else null end)*1.00/count(distinct sku), 4) ola,
#         max(report_time) report_time
#     from df_acc
#     where category is not null
#     group by 1
#     '''
#     ola_df = duckdb.query(qry).df()
    
#     # call API
#     sheet = sheet_api()
#     # extract
#     values = sheet.values().get(spreadsheetId=SAMPLE_SPREADSHEET_ID, range='Pandamart OLA!A1:K').execute().get('values', [])
#     df_acc_rd = pd.DataFrame(values[1:] , columns = values[0])
#     values = sheet.values().get(spreadsheetId=SAMPLE_SPREADSHEET_ID, range='Pandamart OLA!M1:Q').execute().get('values', [])
#     ola_df_rd = pd.DataFrame(values[1:] , columns = values[0])
#     # transform
#     qry = '''select * from df_acc_rd where site!=''' + "'" + urls[j].split("/")[-1] + "'" + ''' union all select * from df_acc'''
#     df_acc_wrt = duckdb.query(qry).df().fillna('')
#     qry = '''select * from ola_df_rd where site!=''' + "'" + urls[j].split("/")[-1] + "'" + ''' union all select * from ola_df'''
#     ola_df_wrt = duckdb.query(qry).df()
#     # load
#     sheet.values().clear(spreadsheetId=SAMPLE_SPREADSHEET_ID, range='Pandamart OLA').execute()
#     sheet.values().update(spreadsheetId=SAMPLE_SPREADSHEET_ID, range="'Pandamart OLA'!A1", valueInputOption='USER_ENTERED', body={'values': [df_acc_wrt.columns.values.tolist()] + df_acc_wrt.values.tolist()}).execute()
#     sheet.values().update(spreadsheetId=SAMPLE_SPREADSHEET_ID, range="'Pandamart OLA'!M1", valueInputOption='USER_ENTERED', body={'values': [ola_df_wrt.columns.values.tolist()] + ola_df_wrt.values.tolist()}).execute()
    
#     # delay
#     if j < (len_urls - 1): time.sleep(randint(80, 100))

# # stats
# elapsed_time = time.time() - start_time
# print("Elapsed time to report (mins): " + str(round(elapsed_time / 60.00, 2)))


# In[67]:


## email 

# import
import pandas as pd
import duckdb
from googleapiclient.discovery import build
from google.oauth2 import service_account
import win32com.client
from pretty_html_table import build_table
import random
import time

# credentials
SERVICE_ACCOUNT_FILE = 'read-write-to-gsheet-apis-1-04f16c652b1e.json'
SAMPLE_SPREADSHEET_ID = '1gkLRp59RyRw4UFds0-nNQhhWOaS4VFxtJ_Hgwg2x2A0'
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

# API
creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
service = build('sheets', 'v4', credentials=creds)
sheet = service.spreadsheets()

# Chaldal
values = sheet.values().get(spreadsheetId=SAMPLE_SPREADSHEET_ID, range='Chaldal OLA!N1:R').execute().get('values', [])
ola_df_cldl = pd.DataFrame(values[1:] , columns = values[0])
# Shajgoj
values = sheet.values().get(spreadsheetId=SAMPLE_SPREADSHEET_ID, range='Shajgoj OLA!L1:O').execute().get('values', [])
ola_df_shaj = pd.DataFrame(values[1:] , columns = values[0])
# OHSOGO
values = sheet.values().get(spreadsheetId=SAMPLE_SPREADSHEET_ID, range='OHSOGO OLA!O1:R').execute().get('values', [])
ola_df_osgo = pd.DataFrame(values[1:] , columns = values[0])
# Daraz
values = sheet.values().get(spreadsheetId=SAMPLE_SPREADSHEET_ID, range='Daraz OLA!Q1:T').execute().get('values', [])
ola_df_daaz = pd.DataFrame(values[1:] , columns = values[0])
# Pandamart
values = sheet.values().get(spreadsheetId=SAMPLE_SPREADSHEET_ID, range='Pandamart OLA!M1:Q').execute().get('values', [])
ola_df_pmrt = pd.DataFrame(values[1:] , columns = values[0])

# summary
qry = '''
select 'Chaldal' platform, location loc, skus_enlisted, skus_online, ola, report_time from ola_df_cldl
union all
select 'Daraz' platform, '-' loc, skus_enlisted, skus_online, ola, report_time from ola_df_daaz
union all
select 'Shajgoj' platform, '-' loc, skus_enlisted, skus_online, ola, report_time from ola_df_shaj
union all
select 'OHSOGO' platform, '-' loc, skus_enlisted, skus_online, ola, report_time from ola_df_osgo 
union all
select 'Pandamart' platform, site loc, skus_enlisted, skus_online, concat(rpad(left((ola::float*100)::text, 5), 5, '0'), '%') ola, report_time from ola_df_pmrt where report_time like ''' + "'" + time.strftime('%d-%b-%y') + "%'"
ola_email_df = duckdb.query(qry).df()
ola_email_df.columns = ['Platform', 'Location', 'SKUs Enlisted', 'SKUs Online', 'OLA', 'Report Time']

# email
ol = win32com.client.Dispatch("outlook.application")
olmailitem = 0x0
newmail = ol.CreateItem(olmailitem)

# subject, recipients
newmail.Subject = 'OLA Status ' + time.strftime('%d-%b-%y')
newmail.To = "avra.barua@unilever.com; safa-e.nafee@unilever.com; rafid-al.mahmood@unilever.com"
newmail.BCC = "shithi30@outlook.com"

# body
newmail.HTMLbody = f'''
Dear concern,<br><br>
As part of <i>Eagle Eye</i>'s 7OA monitoring, this email summarizes today's OLA as below. View full results <a href="https://docs.google.com/spreadsheets/d/1gkLRp59RyRw4UFds0-nNQhhWOaS4VFxtJ_Hgwg2x2A0/edit#gid=646361614">here</a>.
''' + build_table(ola_email_df, random.choice(['green_dark', 'red_dark', 'blue_dark', 'grey_dark', 'orange_dark']), font_size='12px', text_align='left') + '''
Note that, the statistics presented are reflections at the time of scraping. This is an auto email via <i>win32com</i>.<br><br>
Thanks,<br>
Shithi Maitra<br>
Asst. Manager, CSE<br>
Unilever BD Ltd.<br>
'''
# send
newmail.Send()


# In[ ]:




