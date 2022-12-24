import os
from pymongo import MongoClient

client = MongoClient('localhost', 27017)
db = client.cldb
collection = db.news
documents = collection.find()
count_documents = 0
for news in documents:
    if count_documents < -1:
        count_documents += 1
    else:
        print(count_documents)
        if count_documents == 2:
            break

        f = open('/home/student/cl/tomita/input.txt', 'w')
        f.write(news['text'])
        f.close()

        #Start tomita
        os.system("cd tomita/; ./tomita-parser config.proto")

        f = open('/home/student/cl/tomita/output.txt', 'r').readlines()

        line = 0
        analiz_text = ""
        while line < len(f):
            str_p = "Person: "
            if f[line].find('Polit') > -1:
                analiz_text += str(f[line - 1][:-1])
                while True:
                    str_p += str(f[line + 2][12:-1]) + "|"
                    line += 4
                    if line >=len(f) or f[line].find('Polit') == -1:
                        break
                str_p += "#\n"
                analiz_text += str_p

            if line >=len(f):
                break

            str_pl = "Place: "
            if f[line].find('Place') > -1:
                analiz_text += str(f[line - 1][:-1])
                while True:
                    str_pl += str(f[line + 2][12:-1]) + "|"
                    line += 4
                    if line >=len(f) or f[line].find('Polit') == -1:
                        break
                str_pl += "#\n"
                analiz_text += str_pl        
            line+=1

        if len(analiz_text) > 0: 
           id_news = news['_id']
           #print(id_news)

           tomita_c = db.tomita
           old_news = tomita_c.find_one_and_delete({'_id': id_news})
           tomita_c.insert_one(
               {
                   '_id': id_news,
                   'text': analiz_text
               }
           )

        count_documents += 1


print("FINISH")
