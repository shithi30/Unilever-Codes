#!/usr/bin/env python
# coding: utf-8

# In[ ]:


## import
import serpapi
import pandas as pd
import duckdb
from googleapiclient.discovery import build
from google.oauth2 import service_account
import win32com.client
import time


# In[ ]:


## client
start_time = time.time()
client = serpapi.Client(api_key="8e56eebbced63dac4f882b55f6ec39ed550c537bde65bff6d4abd288ad02fcac")


# In[ ]:


## datapoint
def data_from_result(json, field, subfield):
    try: data = json[field] if subfield == "" else json[field][subfield]
    except: data = None
    return data


# In[ ]:


# APIs
results_pmpt = ["FMCG industry when:3d", "eCommerce when:3d"]
results_fmcg = client.search({"engine": "google_news", "q": results_pmpt[0], "gl": "bd"})
results_ecom = client.search({"engine": "google_news", "q": results_pmpt[1], "gl": "bd"})
results_apis = [results_fmcg, results_ecom]


# In[ ]:


## decode

# parse
news_df_acc = pd.DataFrame()
for i in range(0, 2): 
    results = results_apis[i]
    news_df = pd.DataFrame(columns = ["position", "title", "snippet", "source", "author", "link", "publish_date", "search_term"])
    for news_result in results["news_results"]:
        pos = data_from_result(news_result, "position", "")
        ttl = data_from_result(news_result, "title", "")
        spt = data_from_result(news_result, "snippet", "")
        src = data_from_result(news_result, "source", "name")
        atr = data_from_result(news_result, "source", "authors")
        url = data_from_result(news_result, "link", "")
        ymd = data_from_result(news_result, "date", "")
        news_df.loc[len(news_df)] = (pos, ttl, spt, src, ", ".join(atr) if atr is not None else None, url, ymd, results_pmpt[i])

    # record
    news_df_acc = pd.concat([news_df_acc, news_df], ignore_index = True)
    print(str(len(results["news_results"])) + " results parsed.")


# In[ ]:


## services

# credentials
SERVICE_ACCOUNT_FILE = "read-write-to-gsheet-apis-1-04f16c652b1e.json"
SAMPLE_SPREADSHEET_ID = "1gkLRp59RyRw4UFds0-nNQhhWOaS4VFxtJ_Hgwg2x2A0"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# APIs
creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
service = build("sheets", "v4", credentials=creds)
sheet = service.spreadsheets()


# In[ ]:


## ETL

# extract
values = sheet.values().get(spreadsheetId=SAMPLE_SPREADSHEET_ID, range="Reports!A1:J").execute().get("values", [])
prev_df = pd.DataFrame(values[1:] , columns = values[0])

# transform
qry = '''
-- old
select position, title, snippet, source, author, link, publish_date, search_term, 0 if_new, report_date from prev_df union all
-- new
select position, title, snippet, source, author, link, publish_date, search_term, 1 if_new, strftime(now(), '%d-%b-%y, %I:%M %p') report_date
from news_df_acc
where title not in(select title from prev_df)
'''
pres_df = duckdb.query(qry).df()
new_df = duckdb.query('''select * from pres_df where if_new=1 order by position::int asc''').df()
email_df = new_df.head()

# load
sheet.values().clear(spreadsheetId=SAMPLE_SPREADSHEET_ID, range="Reports!A1:J").execute()
sheet.values().update(spreadsheetId=SAMPLE_SPREADSHEET_ID, range="Reports!A1", valueInputOption="USER_ENTERED", body={"values": [pres_df.columns.values.tolist()] + pres_df.fillna("").values.tolist()}).execute()


# In[ ]:


## email

# object
ol = win32com.client.Dispatch("outlook.application")
newmail = ol.CreateItem(0x0)

# expressions, ref: https://www.w3schools.com/charsets/ref_emoji.asp
emos = [128680, 128240, 9889, 8987] # alarm, newspaper, thunder, hourglass

# display
new_cnt = new_df.shape[0]
email_cnt = email_df.shape[0]
email_ttl = email_df["title"].tolist()
email_url = email_df["link"].tolist()
email_ymd = email_df["publish_date"].tolist()

# report
new = '''Dear concern,<br><br>&#''' + str(emos[0]) + ''' Following are, some of the <b>''' + str(new_cnt) + '''</b> newly found <i>FMCG/eCommerce</i> article(s), as of ''' + time.strftime("%d-%b-%y, %I:%M %p") + '''.'''
for i in range(0, email_cnt): new = new + '''<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;• <a href="''' + email_url[i]+ '''">''' + (email_ttl[i] if len(email_ttl[i]) < 63 else email_ttl[i][0:63] + " ...") + '''</a> [''' + email_ymd[i].split(", +")[0] + ''']''' 
new = new + '''<br>&#''' + str(emos[1]) + ''' Full list, <a href="https://docs.google.com/spreadsheets/d/1gkLRp59RyRw4UFds0-nNQhhWOaS4VFxtJ_Hgwg2x2A0/edit#gid=245632566">here</a> (filter by today).'''
new = new + '''<br>&#''' + str(emos[2]) + ''' Powered by: <a href="https://serpapi.com/google-news-api">Google News API [<img src="https://upload.wikimedia.org/wikipedia/commons/thumb/d/da/Google_News_icon.svg/2503px-Google_News_icon.svg.png" style="display: inline-block; width: 20px; height: 15px; padding-top: 2px">]</a>'''
new = new + '''<br>&#''' + str(emos[3]) + ''' Elapsed time to report (sec): ''' + str(round(time.time() - start_time)) + '''<br><br>'''
newmail.HTMLbody = new + '''Thanks,<br>Shithi Maitra<br>Asst. Manager, CSE<br>Unilever BD Ltd.<br>'''

# Teams
newmail.Subject = "FMCG Reports (" + str(new_cnt) + "*)" if new_cnt > 0 else "FMCG Reports"
if new_cnt > 0: newmail.To = "avra.barua@unilever.com; safa-e.nafee@unilever.com; rafid-al.mahmood@unilever.com"
newmail.BCC = "shithi30@outlook.com"
newmail.Send()


# In[ ]:




