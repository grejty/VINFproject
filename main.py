from time import sleep
from bs4 import BeautifulSoup
from collections import defaultdict
import requests
import csv
import re
import html
import os
import pandas as pd

URL = "https://liquipedia.net/counterstrike/Portal:Players"
# Fake HTML header with User-Agent to mimic a real browser request
HEADER = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/98.0.4758.102 Safari/537.36"
}


# Get base URL of the site from given URL
########################################################################################################################
def get_base_url(input_url):
    parts = input_url.split('/')
    output = parts[0] + "//" + parts[2]
    return output


# Check if given URL is allowed via robots.txt of the site
########################################################################################################################
def check_robots(url):
    base_url = get_base_url(url)

    sleep(30)
    robots = requests.get(base_url + "/robots.txt", headers=HEADER).text
    robots = robots.split("User-agent: *\n")[1].split("User-agent:")[0].split("Disallow: ")[1:]

    for i, item in enumerate(robots):
        robots[i] = item.strip()

    if URL[len(base_url):] not in robots:
        pass
    else:
        print("I am not allowed to visit this URL.")
        exit(0)


# Saving different content into text file
########################################################################################################################
def save_to_txt(url, content, task):
    # Check if files exist, create them if not
    for file in ['players.txt', 'regions.txt', 'bans.txt', 'scraped_urls.txt', 'scraped_htmls.txt']:
        if not os.path.exists(file):
            with open(file, 'w', encoding='utf-8'):
                pass  # This creates an empty file

    if task == "player_url":
        # Save URL to a TXT file
        with open('players.txt', 'r', encoding='utf-8') as f_read:
            if url not in f_read.read():
                with open('players.txt', 'a', encoding='utf-8') as f_append:
                    f_append.write(url + '\n')

    elif task == "region_url":
        # Save URL to a TXT file
        with open('regions.txt', 'r', encoding='utf-8') as f_read:
            if url not in f_read.read():
                with open('regions.txt', 'a', encoding='utf-8') as f_append:
                    f_append.write(url + '\n')

    elif task == "ban_url":
        # Save URL to a TXT file
        with open('bans.txt', 'r', encoding='utf-8') as f_read:
            if url not in f_read.read():
                with open('bans.txt', 'a', encoding='utf-8') as f_append:
                    f_append.write(url + '\n')

    elif task == "html":
        # Save HTML content to a TXT file
        with open('scraped_urls.txt', 'r+', encoding='utf-8') as f_read:
            if url not in f_read.read():
                f_read.write(url + '\n')
                with open('scraped_htmls.txt', 'a', encoding='utf-8') as f_append:
                    f_append.write(f"URL: {url}\n")
                    f_append.write("HTML Content:\n")
                    f_append.write(content)


# Fetch HTML code
########################################################################################################################
def fetch_urls(url, header):
    sleep(30)
    response = requests.get(url, headers=header)
    soup = BeautifulSoup(response.content, 'html.parser').prettify()

    # Regex to find all href attributes inside of HTML
    pattern = r'href="([^"]+)"'
    hrefs = re.findall(pattern, soup)

    # Regex to find only these hrefs, that start with /counterstrike/Portal:Players/ to fetch all regions tabs URLs
    pattern = r'/counterstrike/Portal:Players/.*'
    for href in hrefs:
        if re.match(pattern, href):
            save_to_txt(get_base_url(url) + href, "", "region_url")

    # V pripade maleho počtu dat, moznost este ziskat dalsich hracov z /counterstrike/Banned_players,
    # NOTE: player bez profilu nemá v Title "(page does not exist)"
    # Regex to find only the href, that starts with /counterstrike/Banned_players to fetch the "Banned" tab URL
    # pattern = r'/counterstrike/Banned_players.*'
    # for href in hrefs:
    #     if re.match(pattern, href):
    #         save_to_txt(get_base_url(url) + href, "", "ban_url")
    #         sleep(30)
    #         response = requests.get(get_base_url(url) + href, headers=header)
    #         soup = BeautifulSoup(response.content, 'html.parser').prettify()
    #
    #         pattern = r'<div\s+class="divRow mainpage-transfer-neutral"[^>]*>[\s\S]*?<\/div>'
    #
    #         players = re.findall(pattern, soup)
    #
    #         for player in players:
    #             print(player)
    # exit()

    print("Extracting of starting URLs completed")

    regions_urls = open('regions.txt', 'r', encoding='utf-8')
    for region in regions_urls:
        sleep(30)
        response = requests.get(region, headers=header)
        soup = BeautifulSoup(response.content, 'html.parser').prettify()
        print(f"[INFO] Visiting {region}...")

        pattern = r'<div class="template-box"(.*?)</div>'
        countries = re.findall(pattern, soup, re.DOTALL)

        for country in countries:
            country_pattern = r'<th[^>]*>\s*<span[^>]+>\s*<img[^>]+>\s*</span>\s*([^<]+)\s*</th>'
            country_name = re.search(country_pattern, country).group(1).strip()

            pattern = r'<a\s+href="([^"]+)"[^>]*>[^<]+</a>\s*-\s*'
            players = re.findall(pattern, country)

            player_names = [re.search(r'/counterstrike/([^/]+)', player).group(1) for player in players]

            print("[INFO] Browsing players from " + country_name + ":")
            print(', '.join(player_names))

            for player in players:
                save_to_txt(get_base_url(url) + player, "", "player_url")


# Save HTML code
########################################################################################################################
def save_htmls(header):
    players_urls = open('players.txt', 'r', encoding='utf-8').readlines()

    for index, player_url in enumerate(players_urls):
        url = player_url.rstrip()

        sleep(30)
        response = requests.get(url.rstrip(), headers=header)
        soup = BeautifulSoup(response.content, 'html.parser').prettify()

        save_to_txt(url, soup, "html")
        print(f"[{index + 1}/{len(players_urls)}]")
        print(f'Successfully saved HTML content from {url}')


# Parse saved HTMLs
########################################################################################################################
def parse_htmls():
    html_chunks = []

    # First we need to read all htmls
    with open('scraped_htmls.txt', 'r') as file:
        lines = file.readlines()

        html_chunk = ""
        skip_next = False  # Flag to skip the next line

        # Loop through the lines
        for line in lines:
            line = line.strip()

            if skip_next:
                # Skip the next line
                skip_next = False
                continue

            if line.startswith('URL:'):
                # If a new URL is encountered, it indicates the start of a new HTML content
                if html_chunk:
                    html_chunks.append(html_chunk)
                html_chunk = ""
                skip_next = True  # Skip the next line

            else:
                html_chunk += line + "\n"

        # Add the last HTML chunk (if any)
        if html_chunk:
            html_chunks.append(html_chunk)

    headers = {
        'Nick': None,
        'Overview': None,
        'Name': None,
        'Romanized Name': None,
        'Nationality': None,
        'Born': None,
        'Status': None,
        'Years Active (Player)': None,
        'Years Active (Coach)': None,
        'Years Active (Analyst)': None,
        'Role': None,
        'Team': None,
        'Nicknames': None,
        'Alternate IDs': None,
        'Approx. Total Winnings': None,
        'Games': None,
    }

    # Define the CSV file path
    csv_file_path = 'parsed_data.csv'

    # Create a CSV file and write headers as the first row
    with open(csv_file_path, mode='w', newline='', encoding='utf-8') as csv_file:
        writer = csv.writer(csv_file, delimiter='\t')
        writer.writerow(headers.keys())

        # Iterate through the HTML chunks
        for chunk in html_chunks:
            # Clear the values in headers at the start of each loop
            for key in headers:
                headers[key] = None

            # Regex na najdene nicku hráča a následné uloženie
            nick = re.search(r"<span dir=\"auto\">[\S\s](.*)[\S\s]</span>", chunk).group(1)

            if nick:
                headers['Nick'] = nick

            # Regex na najdene "overview" textu hráča a následné uloženie
            overview_info = re.findall(r"<meta content=\"([^\"].*[\s\S]*)['\"] name=\"description\"", chunk)
            if overview_info:
                overview_info = ''.join(overview_info).split("meta content=")[-1].strip("'\"").replace("\n", " ")
                overview_info = html.unescape(overview_info)
                headers['Overview'] = overview_info

            pattern = r'<div class="infobox-cell-2 infobox-description">\n(.*?)</div>\n' \
                      r'<div style="width:50%">\n(.*?)\n</div>'
            infobox_info = re.findall(pattern, chunk, re.DOTALL)
            for key, value in infobox_info:
                key = key.strip()
                value = value.strip()

                if "\xa0" in value:
                    value = value.replace("\xa0", " ")
                if value.startswith('<a href=') or '<span class="flag">' in value:
                    value = re.findall(r'">([\s\S]*?)</a>', value)[-1].strip()
                if '<b>' in value or '<br/>' in value:
                    value = re.sub('<.*?>', ' ', value)
                    value = ' '.join(value.split()).strip()
                    value = re.sub(r'(\d{4}\s*–\s*\d{4})\s*', r'\1, ', value)
                    value = re.sub('\n<br/>\n', ', ', value)
                if "mw-redirect" in value:
                    value = re.search(r'>(.*?)</a>', value, re.DOTALL).group(1).strip()
                if "reference" in value:
                    value = re.sub(r'<sup\b[^>]*>.*?</sup>', '', value, flags=re.DOTALL)
                    value = re.findall(r'([^\n,]+)', value)
                    value = ', '.join(value)

                # Update the headers dictionary
                if key[:-1] in headers:
                    headers[key[:-1]] = value

            # Write the values to the CSV file
            writer.writerow(headers.values())
            print(headers)

    print(f'CSV file saved at {csv_file_path}')


# Create index for input columns
########################################################################################################################
def create_index(columns):
    df = pd.read_csv('parsed_data.csv', sep='\t')

    index = defaultdict(list)

    for column in columns:
        for j, value in enumerate(df[column]):
            index[(column, value)].append(j)

    return index


# Search function
########################################################################################################################
def search(index, search_query):
    split_tmp = search_query.split("=")
    column = split_tmp[0]
    players = split_tmp[1].split(" AND ")

    # Define dictionary for storing values returned from query, which we can then compare
    compare_dict = {
        players[0]: [],
        players[1]: [],
    }

    for player in players:
        player_index = index.get(('Nick', player))[0]
        for key, value in index.items():
            if isinstance(key[1], float) and player_index in value:
                print(player, "nebol ako hráč aktívny.")
                return

            if key[0] == column and player_index in value:
                compare_dict[player] = key[1]

    print()
    print(f"Query: {search_query}")
    print()
    print("Result:")
    print(compare_dict)

    years = list(compare_dict.values())
    if could_have_played_together(years[0], years[1]):
        print("\033[1mThe two players could have played together.\033[0m")
    else:
        print("\033[1mThe two players could not have played together.\033[0m")


# Parses the years string and returns a list of tuples representing periods
def parse_years(years_str):
    periods = years_str.strip().strip(',').split(', ')
    years_list = []

    for period in periods:
        start_end = period.split(' – ')
        if len(start_end) == 1:
            start = end = int(start_end[0])
        else:
            start, end = int(start_end[0]), int(start_end[1]) if start_end[1] != "Present" else float('inf')
        years_list.append((start, end))

    return years_list


# Checks if there is an overlap between two periods
def check_overlap(period1, period2):
    start1, end1 = period1
    start2, end2 = period2

    if end1 is None or end2 is None:
        return True

    return not (end1 < start2 or start1 > end2)


# Checks if two players could have played together during their careers
def could_have_played_together(player1_periods, player2_periods):
    player1_periods = parse_years(player1_periods)
    player2_periods = parse_years(player2_periods)

    for p1 in player1_periods:
        for p2 in player2_periods:
            if check_overlap(p1, p2):
                return True
    return False


def main():
    global URL, HEADER
    # check_robots (URL)
    # fetch_urls(URL, HEADER)
    # save_htmls(HEADER)
    # parse_htmls()

    index_columns = ['Nick', 'Years Active (Player)']
    index = create_index(index_columns)

    # could have - sycrone AND dukiiii,  karl AND flex0r
    # could not have - RobbaN AND dukiiii, Jee AND karl

    # Test cases:
    search(index, 'Years Active (Player)=sycrone AND dukiiii')
    search(index, 'Years Active (Player)=karl AND flex0r')
    search(index, 'Years Active (Player)=RobbaN AND dukiiii')
    search(index, 'Years Active (Player)=Jee AND karl')
    # TODO pridat data z wiki o timoch, potom doplnit search query či mohli hrat v rovnakom time, alebo proti sebe


main()
