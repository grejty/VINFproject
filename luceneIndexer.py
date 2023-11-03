from lucene import SimpleFSDirectory, IndexWriter, Document, Field, StandardAnalyzer
from java.io import File
from java.nio.file import Paths

# Initialize PyLucene Index
index_dir = "/path/to/your/index/directory"  # Replace with actual path
directory = SimpleFSDirectory(Paths.get(index_dir))
analyzer = StandardAnalyzer()
config = IndexWriterConfig(analyzer)
writer = IndexWriter(directory, config)

# Sample data (replace with your actual data)
data = [
    {'Nick': 'Player1', 'Years Active (Player)': '2010-2020'},
    {'Nick': 'Player2', 'Years Active (Player)': '2005-2015'},
    # Add more data as needed
]

# Index the specified fields
for entry in data:
    doc = Document()
    doc.add(Field("Nick", entry['Nick'], Field.Store.YES, Field.Index.NOT_ANALYZED))
    doc.add(Field("Years Active (Player)", entry['Years Active (Player)'], Field.Store.YES, Field.Index.NOT_ANALYZED))
    writer.addDocument(doc)

# Commit changes and close writer
writer.commit()
writer.close()
