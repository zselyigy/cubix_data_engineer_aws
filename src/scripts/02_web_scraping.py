from bs4 import BeautifulSoup    # library for parsing HTML and XML document
import pandas as pd
import requests                  # an HTTP client library 

response = requests.get(url= 'https://en.wikipedia.org/wiki/Community_areas_in_Chicago')

soup = BeautifulSoup(response.content, 'html.parser')
title = soup.find(id= 'firstHeading')
print(title.string)

#print(soup.prettify())