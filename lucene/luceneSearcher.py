import lucene
from java.nio.file import Paths
from org.apache.lucene.store import NIOFSDirectory
from org.apache.lucene.analysis.standard import StandardAnalyzer
from org.apache.lucene.index import DirectoryReader
from org.apache.lucene.search import IndexSearcher
from org.apache.lucene.queryparser.classic import MultiFieldQueryParser

lucene.initVM(vmargs=['-Djava.awt.headless=true'])


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


def convert_string_to_int(value):
    if value and ' million' in value:
        # Remove " million" and convert to float, then multiply by 1 million
        return int(float(value.replace(' million', '')) * 1e6)
    elif not value or any(char.isalpha() for char in value):
        return None
    elif value and ',' in value:
        # Remove commas and convert to int
        return int(value.replace(',', ''))
    else:
        # Convert to int directly
        return int(value)


def search(search_string):
    query_split = search_string.split(" ")

    counter = 0
    years = []
    index_directory = 'lucene/index'

    # create an IndexSearcher object
    reader = DirectoryReader.open(NIOFSDirectory.open(Paths.get(index_directory)))
    searcher = IndexSearcher(reader)

    # columns, we are searching in
    search_columns = ["Nick"]

    query = MultiFieldQueryParser.parse(MultiFieldQueryParser(search_columns, StandardAnalyzer()), search_string)
    results = searcher.search(query, 100)

    print()
    print(f"Query: {search_string}")
    print()

    # for each record, output the information about the Person we have found based on conditions given
    for score in results.scoreDocs:
        doc = searcher.doc(score.doc)

        if doc.get("Nick") in query_split:
            career = doc.get("Years Active(Player)")
            if career:
                years.append(doc.get("Years Active(Player)"))

            query_split.remove(doc.get("Nick"))

            print(f"Nick: {doc.get('Nick')}, Years Active(Player): {doc.get('Years Active(Player)')}")

            population = convert_string_to_int(doc.get('Population'))
            if not population:
                pro_players_per_resident = "[Population data not found]"
            else:
                pro_players_per_resident = population / int(doc.get('Nationality_Count'))
                pro_players_per_resident = '{:,.0f}'.format(pro_players_per_resident)
            print(f"Number of residents per professional "
                  f"player in {doc.get('Nationality')}: {pro_players_per_resident}")

            counter = counter + 1

    print()
    if counter != 2:
        print(f'No records for player(s): {", ".join(map(str, query_split))}')
        return False

    print("Result:")
    if len(years) != 2:
        print("\033[1mYears Active(Player) data not found for one or both players.\033[0m")
        return False
    elif could_have_played_together(years[0], years[1]):
        print("\033[1mThe two players could have played together.\033[0m")
        return True
    else:
        print("\033[1mThe two players could not have played together.\033[0m")
        return False


if __name__ == "__main__":
    # Algeria, Afghanistan, Albania, Azerbaijan, Belgium, Brazil, Bulgaria, Belarus, Bosnia and Herzegovina,
    # Colombia, China, Chile, Croatia, Ecuador, Finland, Germany, Hungary, Hong Kong, Iceland, Italy, India,
    # Indonesia, Iran, Kazakhstan, Kosovo, Latvia, Lithuania, Lebanon, Malta, Mongolia, Morocco, Montenegro,
    # Netherlands, Norway, Poland, Portugal, Pakistan, Philippines, Russia, Romania, Taiwan, Spain, Switzerland,
    # Slovakia, South Korea, Slovenia, Sudan, Serbia, Tunisia, United Kingdom, Ukraine, Uruguay, Uzbekistan, Venezuela
    while True:
        input_query = input("\nEnter search query: \033[1m(t - test cases, q - exit)\033[0m\n")

        if input_query == "t":
            output = search('Freelance peacemaker')
            print("\033[1mExpected:\033[0m The two players could have played together.")
            if output:
                print("\033[1mCorrect.\033[0m")
            else:
                print("\033[1m[X] Incorrect.\033[0m")

            output = search('karl flex0r')
            print("\033[1mExpected:\033[0m The two players could have played together.")
            if output:
                print("\033[1mCorrect.\033[0m")
            else:
                print("\033[1m[X] Incorrect.\033[0m")

            output = search('RobbaN dukiiii')
            print("\033[1mExpected:\033[0m The two players could not have played together.")
            if not output:
                print("\033[1mCorrect.\033[0m")
            else:
                print("\033[1m[X] Incorrect.\033[0m")

            output = search('Jee tomsku')
            print("\033[1mExpected:\033[0m The two players could not have played together.")
            if not output:
                print("\033[1mCorrect.\033[0m")
            else:
                print("\033[1m[X] Incorrect.\033[0m")
        elif input_query == "q":
            exit(1)
        else:
            search(input_query)
