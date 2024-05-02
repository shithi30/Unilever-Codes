## Ushopbd

# import
import pandas as pd
import duckdb
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from bs4 import BeautifulSoup
from googleapiclient.discovery import build
from google.oauth2 import service_account
import pywhatkit
import win32com.client
import time

# accumulators
start_time = time.time()
df_acc = pd.DataFrame()

# preference
options = webdriver.ChromeOptions()
options.add_argument("ignore-certificate-errors")
options.add_argument("headless")

# open window
driver = webdriver.Chrome(service=Service(), options=options)
driver.maximize_window()

# link
pg = 0
while(1): 
    pg = pg + 1
    link = "https://ushopbd.com/collections/international?page=" + str(pg) + "&view=list"
    driver.get(link)

    # soup
    soup_init = BeautifulSoup(driver.page_source, "html.parser")
    soup = soup_init.find_all("div", attrs={"class": "list-view-item__title-column"})

    # page
    sku_count = len(soup)
    if sku_count == 0: break 
    print("Scraping from page: " + str(pg))
    
    # scrape
    sku = []
    current_price = []
    original_price = []
    offer = []
    description = []
    option = []
    pos_in_pg = []
    for i in range(0, sku_count):
        # SKU
        try: val = soup[i].find("div", attrs={"class": "h4 grid-view-item__title"}).get_text()
        except: val = None
        sku.append(val)
        # current price
        try: val = soup[i].find("span", attrs={"class": "money"}).get_text()[3:]
        except: val = None
        current_price.append(val)
        # original price
        try: val = soup[i].find("s", attrs={"class": "product-price__price"}).get_text()[3:]
        except: val = None
        original_price.append(val)
        # offer
        try: val = soup[i].find("span", attrs={"class": "offd"}).get_text()
        except: val = None
        offer.append(val)
        # option
        try: val = soup[i].find("button", attrs={"type": "button"}).get_text()
        except: val = None
        option.append(val)
        # description
        try: val = soup[i].find_all("p")[1].get_text().replace("\n", " ")
        except: val = None
        description.append(val)
        # position
        pos_in_pg.append(i+1)
        
    # accumulate 
    df = pd.DataFrame()
    df['global_sku'] = sku
    df['current_price'] = current_price
    df['original_price'] = original_price
    df['offer'] = offer
    df['description'] = description
    df['option'] = option
    df['pg_no'] = pg
    df['pos_in_pg'] = pos_in_pg
    df_acc = df_acc._append(df)

# close window
driver.close()

# credentials
SERVICE_ACCOUNT_FILE = "read-write-to-gsheet-apis-1-04f16c652b1e.json"
SAMPLE_SPREADSHEET_ID = "1gkLRp59RyRw4UFds0-nNQhhWOaS4VFxtJ_Hgwg2x2A0"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# APIs
creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
service = build("sheets", "v4", credentials=creds)
sheet = service.spreadsheets()

# extract
values = sheet.values().get(spreadsheetId=SAMPLE_SPREADSHEET_ID, range='Ushop OLA!A1:A').execute().get('values', [])
df_acc_prev = pd.DataFrame(values[1:] , columns = values[0])
values = sheet.values().get(spreadsheetId=SAMPLE_SPREADSHEET_ID, range='Ushop OLA!K1:O').execute().get('values', [])
ola_df_prev = pd.DataFrame(values[1:] , columns = values[0])

# transform
df_acc['report_time'] = str(time.strftime('%Y-%m-%d %H:%M'))
qry = '''
select * 
from 
    (select 
        'ushopbd.com' platform,
        count(*) skus_online,
        (select concat('- ', string_agg(global_sku, '\n- ')) from df_acc_prev where global_sku not in(select global_sku from df_acc)) skus_gone_oos, 
        (select concat('- ', string_agg(global_sku, '\n- ')) from df_acc where global_sku not in(select global_sku from df_acc_prev)) skus_added_to_stock, 
        max(report_time) report_time
    from df_acc
    union all
    select * from ola_df_prev where length(concat(skus_gone_oos, skus_added_to_stock))>4 
    ) tbl1
order by report_time desc 
limit 15
'''
ola_df_pres = duckdb.query(qry).df()

# load
sheet.values().clear(spreadsheetId=SAMPLE_SPREADSHEET_ID, range='Ushop OLA').execute()
sheet.values().update(spreadsheetId=SAMPLE_SPREADSHEET_ID, range="'Ushop OLA'!A1", valueInputOption='USER_ENTERED', body={'values': [df_acc.columns.values.tolist()] + df_acc.fillna('').values.tolist()}).execute()
sheet.values().update(spreadsheetId=SAMPLE_SPREADSHEET_ID, range="'Ushop OLA'!K1", valueInputOption='USER_ENTERED', body={'values': [ola_df_pres.columns.values.tolist()] + ola_df_pres.fillna('').values.tolist()}).execute()

# # WhatsApp
# note = ola_df_pres['skus_gone_oos'].tolist()[0]
# note = ":warning\t OOS Notification: Ushopbd.com International has run out of:\n" + note if len(note) > 2 else ''
# if len(note) > 0: pywhatkit.sendwhatmsg_to_group_instantly(group_id="DXqnN42tpV27ZoVWszBH9D", message=note, tab_close=True)

# summary
oos = ola_df_pres['skus_gone_oos'].tolist()[0]
oos = "&#9940 Out of Stock: <i>" + oos[2:].replace("\n- ", ", ") + "</i><br>" if len(oos)>2 else ""
ats = ola_df_pres['skus_added_to_stock'].tolist()[0]
ats = "&#9989 Added to Stock: <i>" + ats[2:].replace("\n- ", ", ") + "</i><br>" if len(ats)>2 else ""

# email
ol = win32com.client.Dispatch("outlook.application")
olmailitem = 0x0
newmail = ol.CreateItem(olmailitem)

# Teams
newmail.Subject = "Ushop OOS + ATS"
# newmail.To = "Ushopbd International - Auto Notification <4b04be85.Unilever.onmicrosoft.com@emea.teams.ms>; soykot.chowdhury@unilever.com"
newmail.To = "soykot.chowdhury@unilever.com"
newmail.BCC = "shithi30@outlook.com"
newmail.HTMLbody = oos + ats + "<br>"
if len(oos + ats) > 0: newmail.Send()

# stats
# display(ola_df_pres.head(5))
print("Elapsed time to report (sec): " + str(round(time.time() - start_time)))