import datetime
import pymongo
import requests
import unicodedata
from bs4 import BeautifulSoup
from time import sleep
from selenium.common.exceptions import TimeoutException
from pymongo import MongoClient
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
import lxml


data = []
# Create the client
client = MongoClient('localhost', 27017)
# Connect to our database
db = client['cldb']
# Fetch our news collection
news_collection = db['news']

def insert_document(collection, data):
    """ Function to insert a document into a collection and
    return the document's id.
    """
    return collection.insert_one(data).inserted_id

method = 2
s = Service('C:\\Users\\anmal\\Downloads\\chromedriver_win32\\chromedriver')
driver = webdriver.Chrome(service=s)
if method == 2:
    for i in range(0, 8):
        startID = 102040
        link1 = "https://v102.ru/news/" + str(int(startID) - i) + ".html"
        driver.get(link1)
        soup2 = BeautifulSoup(driver.page_source, 'lxml')
        title = soup2.find('div', class_='row new-content').text.replace("\n", "")

        substring = "Новости компаний"
        substring1 = "Реклама"
        flag = 0
        date1 = soup2.find('div', class_='row attrs').find('div', class_='col-lg-6 col-md-6 padding4').find('span',
                                                                                                            class_='date-new').text.replace(
            "\n", "")
        if substring in date1:
            flag = 1
        if substring1 in date1:
            flag = 1
        if flag == 0:
            date_obj = datetime.datetime.strptime(date1, "%d.%m.%Y %H:%M ")
            date = date_obj.strftime('%Y-%m-%d %H:%M')

        # print('Date:', date.date())
        # print('Time:', date.time())

        number_comments = soup2.find('span', class_='attr-comment').text

        text1 = soup2.find('div', class_='n-text')
        if text1 is not None:
            text1 = soup2.find('div', class_='n-text').text.replace("\n", "\xa0")
            clean_text = unicodedata.normalize("NFKD", text1)
        else:
            flag = 1

        # data.append([link, title, date, number_comments, clean_text])
        if flag == 0:
            new_news = {
                "link": link1,
                "title": title,
                "date": date_obj,
                "number_comments": number_comments,
                "text": clean_text
            }
            insert_document(news_collection, new_news)
            print(i)


if method == 1:
    for p in range(473, 477):
        print(p)
        url = f"https://v102.ru/center_line_dorabotka_ajax.php?page={p}"
        r = requests.get(url)
        soup = BeautifulSoup(r.text, 'lxml')
        sleep(2)
        news = soup.findAll('div', class_='new-article')

        for n in news:
            link = "https://v102.ru" + n.find('a', class_='detail-link').get('href')
            #link = "https://v102.ru/news/113126.html"
            driver.get(link)
            soup2 = BeautifulSoup(driver.page_source, 'lxml')
            title = soup2.find('div', class_='row new-content').text.replace("\n", "")

            substring = "Новости компаний"
            substring1 = "Реклама"
            flag = 0
            date1 = soup2.find('div', class_='row attrs').find('div', class_='col-lg-6 col-md-6 padding4').find('span', class_='date-new').text.replace("\n", "")
            if substring in date1:
                flag = 1
            if substring1 in date1:
                flag = 1
            if flag == 0:
                date_obj = datetime.datetime.strptime(date1, "%d.%m.%Y %H:%M ")
                date = date_obj.strftime('%Y-%m-%d %H:%M')

            #print('Date:', date.date())
            #print('Time:', date.time())

            number_comments = soup2.find('span', class_='attr-comment').text
            text1 = soup2.find('div', class_='n-text')
            if text1 is not None:
                text1 = soup2.find('div', class_='n-text').text.replace("\n", "\xa0")
                clean_text = unicodedata.normalize("NFKD", text1)
            else:
                flag = 1
            #data.append([link, title, date, number_comments, clean_text])
            if flag == 0:
                new_news = {
                    "link": link,
                    "title": title,
                    "date": date_obj,
                    "number_comments": number_comments,
                    "text": clean_text
                }
                insert_document(news_collection, new_news)


