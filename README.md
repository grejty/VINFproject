**====== Téma: ======**

Zachytávanie údajov o profesionálnych hráčoch z hry Counter-Strike 

**====== Stránka: ======**

liquipedia.com 

**====== Pseudokód: ======**

1. Set initial settings like base URL and headers

2. Sleep for 30 seconds (to respect rate limiting)

3. Fetch robots.txt from the website to check for permission


Main Program:

4. If allowed by robots.txt:
    
    a. Fetch URLs (function fetch_urls)
   
        - Sleep 30 seconds between each request
   
        - Download HTML content of a URL
   
        - Extract URLs using regex pattern matching
   
        - Save region URLs and player URLs to text files
   
    b. Save HTMLs (function save_htmls)
   
        - Read player URLs from a file
   
        - For each player URL, sleep for 30 seconds, then
   
            - Fetch HTML content
   
            - Save HTML content and the URL to text files

Utility Functions:

- get_base_url: Extracts the base URL from a full URL

- save_to_txt: Writes content to specific text files based on the task type ("player_url", "region_url", "html")

**====== Konzultácia č. 2 ======**

19.10.2023 - Crawluje dáta, používa regex, parsuje údaje. Do budúcej konzultácie urobí indexáciu.
