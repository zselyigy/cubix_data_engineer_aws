from bs4 import BeautifulSoup    # library for parsing HTML and XML document
import pandas as pd
import requests                  # an HTTP client library 
from icecream import ic

# load the web page by an http get request
response = requests.get(url= 'https://en.wikipedia.org/wiki/Community_areas_in_Chicago')

# convert it to a BeautifulSoup soup object
soup = BeautifulSoup(response.content, 'html.parser')

# we can find items in this soup easily
title = soup.find(id= 'firstHeading')
#print(title.string)

#print(soup.prettify())

# find all tables od the page
# for table in soup.find_all('table'):
#     ic(table.get('class'))

# get the table we are interested in by name and class
table = soup.find('table', class_= 'wikitable sortable plainrowheaders mw-datatable')

# extract the information about the area numbers and name
# put them in a list of dictionaries
data = []
for row in table.tbody.find_all('tr')[2 : -1]:
    # the numbers
    cell = row.find_all('td')
    area_code = cell[0].get_text(strip= True)
    # the names
    header_cell = row.find('th')
    community_name = header_cell.a.get_text(strip= True)
    # append to the list as a dictionary
    data.append({'area_code': area_code, 'community_name': community_name})
#ic(data)
    
# create a pandas dataframe
community_areas = pd.DataFrame(data)
# convert he area_code to integers
community_areas['area_code'] = community_areas['area_code'].astype('int')
community_areas.info()
# write the dataframe to csv
community_areas.to_csv(r'.\csv\community_areas_master.csv', index= False)
