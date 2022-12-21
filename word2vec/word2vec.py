from pyspark.sql import SparkSession
from pyspark.ml.feature import Tokenizer
from pyspark.ml.feature import StopWordsRemover
from pyspark.ml.feature import CountVectorizer
from pyspark.ml.feature import IDF
from pyspark.ml.feature import Word2Vec
import pandas as pd

spark = SparkSession \
    .builder \
    .appName("SimpleApplication") \
    .getOrCreate()

# Построчная загрузка файла в RDD
input_file = spark.sparkContext.textFile('news.txt')

print(input_file.collect())
prepared = input_file.map(lambda x: ([x]))
df = prepared.toDF()
prepared_df = df.selectExpr('_1 as text')

# Разбить на токены
tokenizer = Tokenizer(inputCol='text', outputCol='words')
words = tokenizer.transform(prepared_df)

# Удалить стоп-слова
stop_words = StopWordsRemover.loadDefaultStopWords('russian')
remover = StopWordsRemover(inputCol='words', outputCol='filtered', stopWords=stop_words)
filtered = remover.transform(words)

# Вывести стоп-слова для русского языка
print(stop_words)

# Вывести таблицу filtered
filtered.show()

# Вывести столбец таблицы words с токенами до удаления стоп-слов
words.select('words').show(truncate=False, vertical=True)

# Вывести столбец "filtered" таблицы filtered с токенами после удаления стоп-слов
filtered.select('filtered').show(truncate=False, vertical=True)

# Посчитать значения TF
vectorizer = CountVectorizer(inputCol='filtered', outputCol='raw_features').fit(filtered)
featurized_data = vectorizer.transform(filtered)
featurized_data.cache()
vocabulary = vectorizer.vocabulary

# Вывести таблицу со значениями частоты встречаемости термов.
featurized_data.show()

# Вывести столбец "raw_features" таблицы featurized_data
featurized_data.select('raw_features').show(truncate=False, vertical=True)

# Посчитать значения DF
idf = IDF(inputCol='raw_features', outputCol='features')
idf_model = idf.fit(featurized_data)
rescaled_data = idf_model.transform(featurized_data)

# Вывести таблицу rescaled_data
rescaled_data.show()

# Вывести столбец "features" таблицы featurized_data
rescaled_data.select('features').show(truncate=False, vertical=True)

# Построить модель Word2Vec
word2Vec = Word2Vec(vectorSize=3, minCount=0, inputCol='words', outputCol='result')
model = word2Vec.fit(words)
w2v_df = model.transform(words)
w2v_df.show()
model.write().overwrite().save("model")

print(vocabulary)
print(type(vocabulary))

persons_and_sights = ["Бочаров", "Григоров", "Марченко", "Стадион", "Писемская", "Савченко", "Мержоева", "Быкадорова",
                      "Казанский кафедральный собор",
                      "Волгоградская областная филармония", "Авангард", "библиотека им. М. Горького",
                      "Площадь Павших Борцов", "Памятник Саше Филиппову", "Музей истории Кировского района",
                      "Памятник чекистам", "Армянская церковь Святого Георгия", "Музей истории Кировского района",
                      "Трамвай-памятник", "Воинский эшелон", "герградта", "Памятник Дзержинскому",
                      "Здание Царицынской пожарной команды", "Дом Павлова", "Челябинский колхозник",
                      "Памятник Гоголю", "БК-13", "Бейт Давид", "Фонтан Бармалей"]

'''for word in persons_and_sights:
    word = word.lower()
    print(word)
    if word in vocabulary:
        synonyms = model.findSynonyms(word, 4)
        synonyms.show(synonyms.count(), False)
        print('Синонимы для "{}"'.format(word))
    else:
         print('{} - отсутствует в словаре'.format(word))'''


for word in persons_and_sights:
    word = word.lower()
    if word in vocabulary:
        synonyms = model.findSynonymsArray(word, 10)
        print('Синонимы для  "{}": {}'.format(word, synonyms))

spark.stop()
