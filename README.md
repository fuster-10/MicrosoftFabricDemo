# Microsoft Fabric Demo
Bienvenidos a la Demo de Microsoft Fabric. Este README.md describe los pasos seguidos durante la demo. Se recomienda seguir este documento al mismo tiempo que la presentación para sacarle el máximo partido.

Para el ejercicio escogido, simularemos la existencia de una tienda online de bicicletas, "Bluetab Bikes". Desde Dirección, se ha solicitado al equipo de Analítica de Datos que desarrolle un dashboard en Power BI que permita analizar las ventas de las bicicletas por Región. Además, quieren que la solución se abordada mediante Microsoft Fabric.

En concreto, quieren que demos respuesta a las siguiente preguntas:
- ¿Cuál fue el Pais con más ventas en el año 2014?
- Dentro de ese Pais, ¿cuál fue la Región con más ventas y cuál fue el producto que mayor volumen de negocio produjo?
- Para el segundo pais con mayor volumen de negocio, ¿coinciden los productos más vendidos con los del país anterior?
- Para el año 2013, en Europa, ¿cuál es el mejor periodo para la venta de bicicletas?¿a qué podría deberse este motivo?

## Paso 1: Origen de datos (operacional)
Para simular los datos disponibles en el operacional, partiremos de la BBDD disponible en el siguiente repo:
https://github.com/microsoft/sql-server-samples/tree/master/samples/databases/adventure-works/data-warehouse-install-script

Donde las archivos que utilizaremos serán:
- DimProduct.csv (Dimensión)
- DimDate.csv (Dimensión)
- DimSalesTerritory.csv (Dimensión)
- DimCustomer.csv (Dimensión)
- FactInternetSales.csv (Hecho)

## Paso 2: Ingesta inicial de los datos
Como parte de las buenas prácticas dentro de Fabric para realizar la ingesta, se recomienda aplicar una operación de "Copy data" con un Pipeline de Data Factory para generar una extracción inicial de los datos, desde el origen a nuestro Lakehouse. Esto generará una copia inicial de los datos en la capa de RAW siguiendo la metodología definida en la arquitectura "Medallion".

El Pipeline desarrollado para realizar esta acción, se encuentra ubicado en la siguiente dirección (desafortunadamente, no se puede visualiazar el contenido del pipeline sin tener acceso al worksapce. En caso de querer conocer más detealle, por favor, contactadme por privado):
https://app.fabric.microsoft.com/groups/61830d7c-c332-46d5-95d3-249737c2e475/pipelines/d673ab56-e5d7-4996-aa02-913107e44561?experience=data-engineering

## Paso 3: Capa intermedia - Curated
En esta fase, se aplicarán unas sencillas tranformaciones a los datos usando PySpark. Las mismas están disponibles en el notebook Demo_Fabric_Curated (https://github.com/fuster-10/MicrosoftFabricDemo/blob/main/Demo_Fabric_Curated.ipynb)

En resumen, las transformaciones aplicadas en este punto son:
- Seleccion de campos deseados de cada fichero.
- Conversión al tipo de dato correcto.
- Generación de columnas calculadas para determinadas entidades.
- Guardado de cada fichero transformado en formato _.parquet_ en la capa CURATED.
  
### Importación inicial de funciones
```python
from pyspark.sql.types import *
from pyspark.sql.functions import concat, lit, datediff, current_date, when, col, substring, sum, length, countDistinct
```
### DimProduct
```python
df_dim_product = spark.read.format("parquet").load("Files/RAW/DimProduct.parquet")

df_dim_product = df_dim_product.withColumnsRenamed(
    {
        'Prop_0': 'ProductKey', 
        'Prop_5': 'ProductName'
    })

df_dim_product = df_dim_product.withColumn(
    'ProductKey', df_dim_product['ProductKey'].cast('integer')
)

df_dim_product = df_dim_product['ProductKey','ProductName']

df_dim_product.printSchema() 
display(df_dim_product)

df_dim_product.write.mode("overwrite").parquet('Files/CURATED/DimProduct.parquet')
print ("Dimensión producto transformada y guardada")

```
### DimDate
```python
# Date
df_dim_date = spark.read.format("parquet").load("Files/RAW/DimDate.parquet")

df_dim_date = df_dim_date['Prop_0','Prop_1','Prop_2','Prop_3','Prop_6','Prop_9','Prop_12','Prop_14']

df_dim_date = df_dim_date.withColumnsRenamed(
    {
        'Prop_0': 'DateKey', 
        'Prop_1': 'Date',
        'Prop_2': 'WeekDayNumber',
        'Prop_3': 'WeekDayName',
        'Prop_6': 'MonthDayNumber',
        'Prop_9': 'MonthName',
        'Prop_12': 'MonthNumber',
        'Prop_14': 'YearNumber'
    })

df_dim_date = df_dim_date.withColumn(
   'DateKey', df_dim_date['DateKey'].cast('integer')
).withColumn(
   'Date', df_dim_date['Date'].cast('date')
).withColumn(
   'WeekDayNumber', df_dim_date['WeekDayNumber'].cast('integer')
).withColumn(
   'MonthDayNumber', df_dim_date['MonthDayNumber'].cast('integer')
).withColumn(
   'MonthNumber', df_dim_date['MonthNumber'].cast('integer')
).withColumn(
   'YearNumber', df_dim_date['YearNumber'].cast('integer')
)

df_dim_date.printSchema() 
display(df_dim_date)

df_dim_date.write.mode("overwrite").parquet('Files/CURATED/DimDate.parquet')
print ("Dimensión Calendario transformada y guardada")
```
### DimSalesTerritory
```python
df_dim_sales_territory = spark.read.format("parquet").option("header","false").load("Files/RAW/DimSalesTerritory.parquet")
df_dim_sales_territory.printSchema() 

df_dim_sales_territory = df_dim_sales_territory.drop('Prop_1')
df_dim_sales_territory = df_dim_sales_territory.drop('Prop_5')

df_dim_sales_territory = df_dim_sales_territory.withColumnsRenamed(
    {
        'Prop_0': 'SalesTerritoryKey', 
        'Prop_2': 'SalesTerritoryRegion',
        'Prop_3': 'SalesTerritoryCountry',
        'Prop_4': 'SalesTerritoryGroup',
    })

df_dim_sales_territory = df_dim_sales_territory.withColumn(
    'SalesTerritoryKey', df_dim_sales_territory['SalesTerritoryKey'].cast('integer')
)

df_dim_sales_territory.printSchema() 
display(df_dim_sales_territory)

df_dim_sales_territory.write.mode("overwrite").parquet('Files/CURATED/DimSalesTerritory.parquet')
print ("Dimensión SalesTerritory transformada y guardada")
```
### DimCustomer
```python


df_dim_customer = spark.read.format("parquet").load("Files/RAW/DimCustomer.parquet")

df_dim_customer = df_dim_customer['Prop_0','Prop_4','Prop_6','Prop_8','Prop_9','Prop_11','Prop_13']

df_dim_customer.printSchema() 

df_dim_customer = df_dim_customer.withColumnsRenamed(
    {
        'Prop_0': 'CustomerKey', 
        'Prop_4': 'FirstName',
        'Prop_6': 'LastName',
        'Prop_8': 'BirthDate',
        'Prop_9': 'CivilState',
        'Prop_11': 'Gender',
        'Prop_13': 'YearlyIncome'
    })

df_dim_customer = df_dim_customer.withColumn(
    'CustomerKey', df_dim_customer['CustomerKey'].cast('integer')
).withColumn(
   'FullName', concat(df_dim_customer['FirstName'],lit(" "),df_dim_customer['LastName'])
).withColumn(
   'BirthDate', df_dim_customer['BirthDate'].cast('date')
).withColumn(
   'YearlyIncome', df_dim_customer['YearlyIncome'].cast('double')
).withColumn(
   'Age', when(col('BirthDate').isNotNull(), datediff(current_date(), col('BirthDate'))/365)

)

df_dim_customer.printSchema() 
display(df_dim_customer)

df_dim_customer.write.mode("overwrite").parquet('Files/CURATED/DimCustomer.parquet')
print ("Dimensión Customer transformada y guardada")
```
### FactInternetSales
```python
df_fact_sales = spark.read.format("parquet").load("Files/RAW/FactInternetSales.parquet")

df_fact_sales = df_fact_sales['Prop_0','Prop_1','Prop_2','Prop_3','Prop_4','Prop_6','Prop_7','Prop_8','Prop_11','Prop_12']

df_fact_sales.printSchema() 

df_fact_sales = df_fact_sales.withColumnsRenamed(
    {
        'Prop_0': 'ProductKey', 
        'Prop_1': 'OrderDateKey',
        'Prop_2': 'DueDateKey',
        'Prop_3': 'ShipDateKey',
        'Prop_4': 'CustomerKey',
        'Prop_6': 'CurrencyKey',
        'Prop_7': 'SalesTerritoryKey',
        'Prop_8': 'SalesOrderNumber',
        'Prop_11': 'OrderQuantity',
        'Prop_12': 'UnitPrice'
    })

df_fact_sales = df_fact_sales.withColumn(
    'ProductKey', df_fact_sales['ProductKey'].cast('integer')
).withColumn(
    'OrderDateKey', df_fact_sales['OrderDateKey'].cast('integer')
).withColumn(
    'DueDateKey', df_fact_sales['DueDateKey'].cast('integer')
).withColumn(
    'ShipDateKey', df_fact_sales['ShipDateKey'].cast('integer')
).withColumn(
    'CustomerKey', df_fact_sales['CustomerKey'].cast('integer')
).withColumn(
    'CurrencyKey', df_fact_sales['CurrencyKey'].cast('integer')
).withColumn(
    'SalesTerritoryKey', df_fact_sales['SalesTerritoryKey'].cast('integer')
).withColumn(
    'OrderQuantity', df_fact_sales['OrderQuantity'].cast('integer')
).withColumn(
    'UnitPrice', df_fact_sales['UnitPrice'].cast('double')
).withColumn(
    'TotalSales', col('OrderQuantity') * col('UnitPrice')
)

df_fact_sales.printSchema() 
display(df_fact_sales)

df_fact_sales.write.mode("overwrite").parquet('Files/CURATED/FactInternetSales.parquet')
print ("Fact Sales transformada y guardada")
```

## Paso 4: Capa final - Consumption
En este punto, como operación final a aplicar sobre los datos, generaremos una versión agregada de la tabla _FactInternetSales_, llamada _FactSalesSummary_. 

Las operaciones realizadas están disponibles en el notebook [Demo_Fabric_Consumption] (https://github.com/fuster-10/MicrosoftFabricDemo/blob/main/Demo_Fabric_Consumption.ipynb)

Nuestra tabla _FactInternetSales_ contiene los siguientes campos:
- ProductKey: integer.
- OrderDateKey: integer.
- DueDateKey: integer.
- ShipDateKey: integer.
- CustomerKey: integer.
- CurrencyKey: integer.
- SalesTerritoryKey: integer.
- SalesOrderNumber: string.
- OrderQuantity: integer.
- UnitPrice: double.
- TotalSales: double.

Con la agregación realizada, pasaremos a disponer de la siguiente tabla:
- ProductKey: integer.
- CustomerKey: integer.
- SalesTerritoryKey: integer.
- OrderYear: integer.
- OrderMonth: integer.
- TotalSalesAmount: double (sum('TotalSales')).
- TotalItems: long (sum('OrderQuantity')).
- TotalOrders: long (countDistinct('SalesOrderNumber')).
- DateKey: integer.

Es decir, pasaremos de tener el detalle de nuestras ventas online, determinadas por el campo _SalesOrderNumber_, a disponer de una tabla agregada a nivel de _ProductKey_, _CustomerKey_, _SalesTerritoryKey_, _OrderYear_, y _OrderMonth_. Además, se crearán las tablas deltas correspondientes a cada una de las entidades tratadas. 

### Importación inicial de funciones
```python
from pyspark.sql.types import *
from pyspark.sql.functions import concat, lit, datediff, current_date, when, col, substring, sum, length, countDistinct
```
### FactSalesSummary
```python
df_fact_summary = spark.read.format("parquet").load("Files/CURATED/FactInternetSales.parquet")

df_fact_summary = df_fact_summary.withColumn(
    'OrderYear', substring(df_fact_summary['OrderDateKey'],0,4).cast('integer')
).withColumn(
    'OrderMonth', substring(df_fact_summary['OrderDateKey'],5,2).cast('integer')
)

df_fact_summary = df_fact_summary.groupBy('ProductKey', 'CustomerKey', 'SalesTerritoryKey', 'OrderYear', 'OrderMonth') \
    .agg(sum('TotalSales').alias('TotalSalesAmount'), sum('OrderQuantity').alias('TotalItems'), countDistinct('SalesOrderNumber').alias('TotalOrders'))

df_fact_summary = df_fact_summary.withColumn(
    'DateKey', when(length(df_fact_summary['OrderMonth'].cast('string'))==2,concat(df_fact_summary['OrderYear'].cast('string'),df_fact_summary['OrderMonth'].cast('string'),lit('01'))).otherwise(concat(df_fact_summary['OrderYear'].cast('string'),lit('0'),df_fact_summary['OrderMonth'].cast('string'),lit('01'))).cast('integer')
)

df_fact_summary.printSchema() 
display(df_fact_summary)


df_fact_summary.write.mode("overwrite").parquet('Files/CONSUMPTION/FactSalesSummary.parquet')
print ("FactSalesSummary agregada y guardada")

```
### FactInternetSales
```python
df_fact_internet_sales = spark.read.format("parquet").load("Files/CURATED/FactInternetSales.parquet")

df_fact_internet_sales.write.mode("overwrite").parquet('Files/CONSUMPTION/FactInternetSales.parquet')
print ("FactInternetSales guardada en capa CONSUMPTION")

```
### DimCustomer
```python
df_dim_customer = spark.read.format("parquet").load("Files/CURATED/DimCustomer.parquet")

df_dim_customer.write.mode("overwrite").parquet('Files/CONSUMPTION/DimCustomer.parquet')
print ("DimCustomer guardada en capa CONSUMPTION")
```
### DimDate
```python
df_dim_date = spark.read.format("parquet").load("Files/CURATED/DimDate.parquet")

df_dim_date.write.mode("overwrite").parquet('Files/CONSUMPTION/DimDate.parquet')
print ("DimDate guardada en capa CONSUMPTION")
```
### DimProduct
```python
df_dim_product = spark.read.format("parquet").load("Files/CURATED/DimProduct.parquet")

df_dim_product.write.mode("overwrite").parquet('Files/CONSUMPTION/DimProduct.parquet')
print ("DimProduct guardada en capa CONSUMPTION")
```
### DimSalesTerritory
```python
df_dim_sales_territory = spark.read.format("parquet").load("Files/CURATED/DimSalesTerritory.parquet")

df_dim_sales_territory.write.mode("overwrite").parquet('Files/CONSUMPTION/DimSalesTerritory.parquet')
print ("DimSalesTerritory guardada en capa CONSUMPTION")
```
### Creación de las tablas delta
```python
df_fact_summary.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable("fact_summary")
df_fact_internet_sales.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable("fact_internet_sales")
df_dim_customer.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable("dim_customer")
df_dim_date.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable("dim_date")
df_dim_product.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable("dim_product")
df_dim_sales_territory.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable("dim_sales_territory")
```
En este punto, disponemos de nuestras tablas delta en el Lakehouse "Demo Fabric":
![image](https://github.com/fuster-10/MicrosoftFabricDemo/assets/29040162/5afa6523-9fa7-43af-836b-ab6892c55c1d)

## Paso 5: Creación del modelo semántico en Power BI
Enhorabuena, ya estamos cerca de poder finalizar nuestro proyecto y reportar a la Dirección la información que nos había solicitado. Sólo queda un último punto antes de poder comenzar a desarrollar nuestra informe de Power BI: *Construir el modeo semántico*. Esto es posible, debido a que ya tenemos en disposición las tablas finales con las que poder construir el modelo de datos a usar en el dashboard. Sin embargo, para abordar esta operación podemos optar por dos alternativas:
- Opción 1: Definición de las relaciones del modelo semántico desde el servicio de Power BI.
- Opción 2: Definición de las relaciones del modelo semántico a través Power BI Desktop.

Como buena práctica, es recomendable el uso de la opción 1. No obstante, la opción 2, es posible y será explicada en la siguiente demo.
### Opción 1: Definición de las relaciones del modelo semántico desde el servicio de Power BI.
Par ello, accederemos a la opción "Punto de conexión de análisis SQL":
![image](https://github.com/fuster-10/MicrosoftFabricDemo/assets/29040162/394fcfe5-f950-4407-b2bc-c7558e74a4d5)

En esta pestaña se puede observar lo siguiente:
![image](https://github.com/fuster-10/MicrosoftFabricDemo/assets/29040162/f7217122-7d33-4eb3-a98d-5b4a8daf1b5a)

Tras la creación de las tabla delta, Microsoft Fabric crea de forma automática un SQL end point (sólo lectura) al que poder conectarte y realizar queries a los datos generados. Hay tres pestañas disponibles:
- Datos: Visualizamos los datos disponibles.
- Consulta: Escribimos consultas a los datos.
- Modelo: Construimos el modelo semántico de Power BI.

Para pasar a establecer las relaciones entre nuestras tablas, haremos click en Creación de informes -> Administrar modelo semántico predeterminado. En este apartado, estableremos las relaciones entre nuestras tablas, de acuerdo a la cardinalidad con la que se encuentren relacionadas, por lo tanto, crearemos:
- Relación 1:N entre dim_date y fact_internet_sales mediante los campos DateKey y OrderDateKey.
- Relación 1:N entre dim_product y fact_internet_sales mediante el campo Product_Key.
- Relación 1:N entre dim_sales_territory y fact_internet_sales mediante el campo Sales_Territory_Key.
- Relación 1:N entre dim_customer y fact_internet_sales mediante el campo Customer_Key.
- Relación 1:N entre dim_date y fact_summary mediante el campo DateKey.
- Relación 1:N entre dim_product y fact_summary mediante el campo Product_Key.
- Relación 1:N entre dim_sales_territory y fact_summary mediante el campo Sales_Territory_Key.
- Relación 1:N entre dim_customer y fact_summary mediante el campo Customer_Key.







