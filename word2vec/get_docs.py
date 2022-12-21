from pymongo import MongoClient
import re

client = MongoClient('localhost', 27017)
db = client['cldb']
collection = db['news']


db_documents = list(collection.find({}))

with open('news.txt', 'w', encoding="utf-8") as f:
    for i in range(len(list(db_documents))):
        print(db_documents[i]['title'])
        print(db_documents[i]['text'])
        f.write('Статья номер {} \n'.format(i+1))
        f.write(db_documents[i]['title'])
        f.write('\n')
        f.write(re.sub(r'\', \'', ' ', str(db_documents[i]['text'])))
        f.write('\n\n')
    f.close()
