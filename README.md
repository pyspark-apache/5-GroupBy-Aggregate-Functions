# 5- GroupBy And Aggregate Functions

# 1. Groupby
~~~python
df_pyspark.groupby("Name").sum()
DataFrame[Name: string, sum(salary): bigint]
~~~
### Look, only sum salary because is int

~~~python
df_pyspark.groupby("Name").sum().show()
+---------+-----------+
|     Name|sum(salary)|
+---------+-----------+
|Sudhanshu|      35000|
|    Sunny|      12000|
|    Krish|      19000|
|   Mahesh|       7000|
+---------+-----------+
~~~
### Groupby to find the maximun salary
~~~python
df_pyspark.groupby("Name").max().show()
+---------+-----------+
|     Name|max(salary)|
+---------+-----------+
|Sudhanshu|      20000|
|    Sunny|      10000|
|    Krish|      10000|
|   Mahesh|       4000|
+---------+-----------+

~~~
### Groupby Departments wich gives maximun salary
~~~python
df_pyspark.groupby("Departments").sum().show()
+------------+-----------+
| Departments|sum(salary)|
+------------+-----------+
|         IOT|      15000|
|    Big Data|      15000|
|Data Science|      43000|
+------------+-----------+
df_pyspark.groupby("Departments").mean().show()
+------------+-----------+
| Departments|avg(salary)|
+------------+-----------+
|         IOT|     7500.0|
|    Big Data|     3750.0|
|Data Science|    10750.0|
+------------+-----------+
~~~
There are more functions, `count(), avg()...`

# 2. Agg
~~~python
df_pyspark.agg({"Salary":"sum"}).show()
+-----------+
|sum(Salary)|
+-----------+
|      73000|
+-----------+
~~~