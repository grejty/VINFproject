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


def search(search_string):
    query_split = search_string.split(" ")
    query1 = query_split[0]
    query2 = query_split[1]

    counter = 0
    years = []
    indexDirectory = 'lucene/index'

    # create an IndexSearcher object
    reader = DirectoryReader.open(NIOFSDirectory.open(Paths.get(indexDirectory)))
    searcher = IndexSearcher(reader)

    # columns, we are searching in
    search_columns = ["Nick"]

    query = MultiFieldQueryParser.parse(MultiFieldQueryParser(search_columns, StandardAnalyzer()), search_string)
    results = searcher.search(query, 100)

    # for each record, output the information about the Person we have found based on conditions given
    for score in results.scoreDocs:
        doc = searcher.doc(score.doc)
        print()

        if doc.get("Nick") == query1 or doc.get("Nick") == query2:
            years.append(doc.get("Years Active(Player)"))

            print(f'Nick: {doc.get("Nick")}')
            print(f'Years Active(Player): {doc.get("Years Active(Player)")}')

            counter = counter + 1

    print()
    if not counter:
        print('No records of given players.')
    else:
        print(f"Query: {search_string}")

    print("Result:")
    if could_have_played_together(years[0], years[1]):
        print("\033[1mThe two players could have played together.\033[0m")
    else:
        print("\033[1mThe two players could not have played together.\033[0m")


if __name__ == "__main__":
    input_query = input("Enter search string: \033[1m(t for test cases)\033[0m\n")

    if input_query == "t":
        search('sycrone dukiiii')
        search('karl flex0r')
        search('RobbaN dukiiii')
        search('Jee karl')
    else:
        search(input_query)
