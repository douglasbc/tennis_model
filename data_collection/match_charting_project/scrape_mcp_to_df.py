import re
import requests

import bs4
import pandas as pd


def scrape_mcp_table_to_df(url):

    USER_AGENT = "Mozilla/5.0 (Linux; Android 12; SM-X906C Build/QP1A.190711.020; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/80.0.3987.119 Mobile Safari/537.36"

    req = requests.get(url, headers={"User-Agent": USER_AGENT})
    soup = bs4.BeautifulSoup(req.text, 'html.parser')
    table = soup.find_all('table')[1]
    body = table.find_all('tr')

    head = body[0]
    body_rows = body[1:]

    # Creating an empty list to keep column names 
    headings = []
    # Loop through all th elements
    for item in head.find_all("th"):
        # Convert the th elements to text and strip "\n"
        item = (item.text).rstrip("\n")
        # Append the clean column name to headings
        headings.append(item)

    # Next is to loop though the rest of the rows
    # Creating an empty list for rest of all the rows
    all_rows = [] 

    # Taking a single row at a time
    for row_num in range(1, len(body_rows)):       
        row = []                                
        # Loop through all row entries
        for row_item in body_rows[row_num].find_all("td"):       
            
            #  Using 'row_item.text' removes the tags from the entries and remove \xa0 and \n and comma from row_item.text by a regular expression        
            # Removing contents-- xa0 encodes the flag , \n is the newline and comma separates thousands in numbers.            
            aa = re.sub(r"(?<=\w)([A-Z])", r" \1", re.sub("(\xa0)|(\n)|,","",row_item.text))
            
            # One row entry (aa) is being appended
            row.append(aa)
        # Appending single row to all_rows
        all_rows.append(row)

    return pd.DataFrame(data=all_rows,columns=headings)


def scrape_mcp():

    url_dict = {
        'atp_last_52_serve' : 'https://tennisabstract.com/reports/mcp_leaders_serve_men_last52.html',
        'atp_last_52_return' : 'https://tennisabstract.com/reports/mcp_leaders_return_men_last52.html',
        'atp_last_52_rally' : 'https://tennisabstract.com/reports/mcp_leaders_rally_men_last52.html',
        'atp_last_52_tactics' : 'https://tennisabstract.com/reports/mcp_leaders_tactics_men_last52.html',
        'atp_career_serve' : 'https://tennisabstract.com/reports/mcp_leaders_serve_men_career.html',
        'atp_career_return' : 'https://tennisabstract.com/reports/mcp_leaders_return_men_career.html',
        'atp_career_rally' : 'https://tennisabstract.com/reports/mcp_leaders_rally_men_career.html',
        'atp_career_tactics' : 'https://tennisabstract.com/reports/mcp_leaders_tactics_men_career.html',
        'wta_last_52_serve' : 'https://tennisabstract.com/reports/mcp_leaders_serve_women_last52.html',
        'wta_last_52_return' : 'https://tennisabstract.com/reports/mcp_leaders_return_women_last52.html',
        'wta_last_52_rally' : 'https://tennisabstract.com/reports/mcp_leaders_rally_women_last52.html',
        'wta_last_52_tactics' : 'https://tennisabstract.com/reports/mcp_leaders_tactics_women_last52.html',
        'wta_career_serve' : 'https://tennisabstract.com/reports/mcp_leaders_serve_women_career.html',
        'wta_career_return' : 'https://tennisabstract.com/reports/mcp_leaders_return_women_career.html',
        'wta_career_rally' : 'https://tennisabstract.com/reports/mcp_leaders_rally_women_career.html',
        'wta_career_tactics' : 'https://tennisabstract.com/reports/mcp_leaders_tactics_women_career.html',
    }

    df_dict = dict()

    for key, value in url_dict.items():
        df_dict[key] = scrape_mcp_table_to_df(value)

    return df_dict