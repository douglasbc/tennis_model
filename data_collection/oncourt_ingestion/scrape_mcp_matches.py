import requests
from bs4 import BeautifulSoup
import re


USER_AGENT = "Mozilla/5.0 (Linux; Android 12; SM-X906C Build/QP1A.190711.020; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/80.0.3987.119 Mobile Safari/537.36"

# URL of the webpage to scrape
url = "https://www.tennisabstract.com/charting/meta.html"

# Send a GET request to the webpage
response = requests.get(url, headers={"User-Agent": USER_AGENT})
response.raise_for_status()  # Check if the request was successful

# Parse the HTML content of the webpage
soup = BeautifulSoup(response.content, 'html.parser')

# Define the regular expression pattern to match the URLs
# pattern = re.compile(r"https://www\.tennisabstract\.com/charting/201\d{5}-[MW]")
pattern = re.compile(r"https://www\.tennisabstract\.com/charting/201\d{5}-[MW]")


# Find all the URLs that match the pattern
urls = set()  # Use a set to avoid duplicates
for link in soup.find_all('a', href=True):
    href = link['href']
    if pattern.match(href):
        urls.add(href)

# Print the found URLs
for url in sorted(urls):
    print(url)
