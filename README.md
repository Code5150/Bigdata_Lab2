# Лабораторная работа № 2: Формирование отчётов в Apache Spark
***

## Задание:
Сформировать отчёт с информацией о 10 наиболее популярных языках программирования по итогам года за период с 2010 по 2020 годы. Получившийся отчёт сохранить в формате Apache Parquet.
Для выполнения задания вы можете использовать любую комбинацию Spark API: _RDD API, Dataset API, SQL API_.

## Анализ данных о языках программирования

Так как обрабатываемые данные имеют большой объём, установим в конфигурации Spark количество памяти, достаточное для их обработки:

```
    val config = new SparkConf().setAppName(appName).setMaster(master).setAll(List.apply(
      ("spark.driver.memory", "45g"),
      ("spark.executor.memory", "6g"),
      ("spark.driver.maxResultSize", "8g")
    ))
    val sc = new SparkContext(config)
    val spark = SparkSession.builder().appName(appName).config(config).getOrCreate()
```

Загрузим данные из файлов в датафреймы:

```
    val langDf = spark.read.option("header", value = true).csv(langPath)
    val languages = langDf.withColumn("name", trim(lower(langDf("name"))))
      .select("name").rdd.map(_.getString(0)).collect().toSet

    val postsDf = spark.read.format("com.databricks.spark.xml")
      .option("rowTag", "row")
      .load(postsPath)
```

Выберем названия языков программирования в коллекцию, т.к. с ней удобнее работать при фильтрации RDD.

Выборка о языках программирования получилась следующей:

![Выборка о языках программирования](https://github.com/Code5150/Bigdata_Lab2/blob/master/img/langDf.jpg)

Схема DataFrame о постах получилась следующей:

![Схема DataFrame о постах](https://github.com/Code5150/Bigdata_Lab2/blob/master/img/postsSchema.jpg)

Создадим для таблицы постов временное представление для дальнейшей работы с ней в качестве SQL-таблицы:

```
postsDf.createOrReplaceTempView("posts")
```

Найдем количество всех тегов, которые находятся в списке языков программирования, в постах за каждый указанный год:

```
    val postsByYearAndTagWithCount = spark.sql("SELECT _Id as id, _Tags as tags, EXTRACT(year from _CreationDate) as year FROM posts " +
      "WHERE _CreationDate BETWEEN cast('2010-01-01' as timestamp) and cast('2021-01-01' as timestamp) " +
      "and _Tags is not null").rdd.flatMap(
        row => row.getString(1).strip().drop(1).dropRight(1)
          .split("><")
          .map(s => (LangYear(s, row.getInt(2)), 1L)))
      .reduceByKey(_ + _)
      .filter(l => languages.contains(l._1.lang))
      .map(s => (s._1.year, s._1.lang, s._2))
      .toDF("year", "language", "count")
```

Получившуюся таблицу также преобразуем во временное представление:

```
postsByYearAndTagWithCount.createOrReplaceTempView("postsYearLangCount")
```

После при помощи SQL для каждого года выберем верхние 10 тегов по количеству постов и объединим выборки в один датафрейм:

```
    val result = (2010 until 2021).map(
      i => spark.sql(s"SELECT * FROM postsYearLangCount WHERE year = $i ORDER BY year ASC, count DESC LIMIT 10")
    ).reduce((df1, df2) => df1.union(df2))
```

Выведем его и сохраним его в parquet:

```
    result.show(100)
    result.write.parquet(reportPath)
```

Получился следующий результат:

| Year | Language | Count |
|:---:|:----:|:---:|
|2010|c#|75340          |
|2010|java|54340        |
|2010|php|51554         |
|2010|javascript|43445  |
|2010|c++|31953         |
|2010|python|27010      |
|2010|objective-c|17875 |
|2010|c|15265           |
|2010|ruby|10171        |
|2010|perl|5009         |
|2011|c#|112737         |
|2011|java|98907        |
|2011|php|96013         |
|2011|javascript|89958  |
|2011|c++|47937         |
|2011|python|42127      |
|2011|objective-c|38781 |
|2011|c|22584           |
|2011|ruby|18727        |
|2011|perl|6738         |
|2012|java|144571       |
|2012|c#|138340         |
|2012|javascript|136190 |
|2012|php|131597        |
|2012|python|64166      |
|2012|c++|63082         |
|2012|objective-c|46665 |
|2012|c|30979           |
|2012|ruby|24361        |
|2012|r|12175           |
|2013|javascript|195606 |
|2013|java|190510       |
|2013|php|166994        |
|2013|c#|164979         |
|2013|python|96250      |
|2013|c++|82019         |
|2013|objective-c|47436 |
|2013|c|40579           |
|2013|ruby|29260        |
|2013|r|22225           |
|2014|javascript|235501 |
|2014|java|217091       |
|2014|php|177884        |
|2014|c#|162626         |
|2014|python|116219     |
|2014|c++|82578         |
|2014|objective-c|48282 |
|2014|c|41740           |
|2014|r|30845           |
|2014|ruby|30025        |
|2015|javascript|255484 |
|2015|java|215201       |
|2015|php|170772        |
|2015|c#|154553         |
|2015|python|137054     |
|2015|c++|79745         |
|2015|r|40640           |
|2015|c|40024           |
|2015|objective-c|35741 |
|2015|ruby|27994        |
|2016|javascript|263998 |
|2016|java|196683       |
|2016|php|161221        |
|2016|python|158310     |
|2016|c#|148713         |
|2016|c++|71400         |
|2016|r|44323           |
|2016|c|34525           |
|2016|ruby|24030        |
|2016|objective-c|23348 |
|2017|javascript|249897 |
|2017|python|191870     |
|2017|java|174341       |
|2017|php|141344        |
|2017|c#|131293         |
|2017|c++|61980         |
|2017|r|50973           |
|2017|c|30568           |
|2017|typescript|21968  |
|2017|ruby|17311        |
|2018|javascript|208617 |
|2018|python|205995     |
|2018|java|145840       |
|2018|c#|108678         |
|2018|php|102680        |
|2018|r|52193           |
|2018|c++|52005         |
|2018|c|25699           |
|2018|typescript|25603  |
|2018|bash|13186        |
|2019|python|224824     |
|2019|javascript|188939 |
|2019|java|126703       |
|2019|c#|99774          |
|2019|php|78331         |
|2019|r|50197           |
|2019|c++|49503         |
|2019|typescript|29061  |
|2019|c|25555           |
|2019|kotlin|13684      |


***

## Заключение

В ходе выполнения лабораторной работы были изучены возможности _Spark SQL API_, успешно выполнен анализ данных о популярности языков программирования с 2010 по 2020 годы, а данные, полученные в результате анализа, сохранены в отчёт в формате _Parquet_.