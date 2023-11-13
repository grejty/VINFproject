import lucene
from java.nio.file import Paths
from org.apache.lucene.store import NIOFSDirectory
from org.apache.lucene.analysis.standard import StandardAnalyzer
from org.apache.lucene.index import IndexWriter, IndexWriterConfig, IndexOptions, DirectoryReader
from org.apache.lucene.document import Document, Field, StringField, TextField, StoredField, FieldType
import csv


def createIndex():
    lucene.initVM(vmargs=['-Djava.awt.headless=true'])
    indexDirectory = 'lucene/index'
    indexWriterConfig = IndexWriterConfig(StandardAnalyzer())

    # create an IndexWriter object
    writer = IndexWriter(NIOFSDirectory(Paths.get(indexDirectory)), indexWriterConfig)

    print("Creating index...")

    # open the CSV file for reading
    with open('data/parsed_data.csv', 'r') as csvfile:
        # read the data from the CSV file
        data = csv.reader(csvfile, delimiter='\t')
        # iterate over each row in the CSV file
        for row in data:
            # create a Document object
            doc = Document()
            # add a Field object for each column in the row
            doc.add(Field('Nick', row[0], TextField.TYPE_STORED))
            doc.add(Field('Overview', row[1], TextField.TYPE_STORED))
            doc.add(Field('Name', row[2], TextField.TYPE_STORED))
            doc.add(Field('Romanized Name', row[3], TextField.TYPE_STORED))
            doc.add(Field('Nationality', row[4], TextField.TYPE_STORED))
            doc.add(Field('Born', row[5], TextField.TYPE_STORED))
            doc.add(Field('Status', row[6], TextField.TYPE_STORED))
            doc.add(Field('Years Active(Player)', row[7], TextField.TYPE_STORED))
            doc.add(Field('Years Active(Coach)', row[8], TextField.TYPE_STORED))
            doc.add(Field('Years Active(Analyst)', row[9], TextField.TYPE_STORED))
            doc.add(Field('Role', row[10], TextField.TYPE_STORED))
            doc.add(Field('Team', row[11], TextField.TYPE_STORED))
            doc.add(Field('Nicknames', row[12], TextField.TYPE_STORED))
            doc.add(Field('Alternate IDs', row[13], TextField.TYPE_STORED))
            doc.add(Field('Approx.Total Winnings', row[14], TextField.TYPE_STORED))
            doc.add(Field('Games', row[15], TextField.TYPE_STORED))

            # add the Document to the index
            writer.addDocument(doc)
    writer.commit()

    print("Finished")


if __name__ == "__main__":
    createIndex()
