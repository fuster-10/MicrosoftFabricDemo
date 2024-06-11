# Microsoft Fabric Demo
Bienvenidos a la Demo de Microsoft Fabric. Este README.md describe los pasos seguidos durante la demo. Se recomienda seguir este documento al mismo tiempo que la presentación para sacarle el máximo partido.

Para el ejercicio escogido, **simularemos la existencia de una tienda online de bicicletas, "Bluetab Bikes"**. Desde Dirección, se ha solicitado al equipo de Analítica de Datos que desarrolle un **dashboard en Power BI que permita analizar las ventas de las bicicletas por Región**. Además, quieren que la **solución sea abordada mediante Microsoft Fabric**.

En concreto, **se desea dar respuesta a las siguientes preguntas**:
- ¿Cuál fue el País con más ventas en el año 2014?
- En este país, ¿cuál fue la Región con más ventas y qué producto produjo mayor volumen de negocio?
- Para el segundo país con mayor volumen de negocio, ¿coinciden los productos más vendidos con los del país anterior?
- Para el año 2013, en Europa, ¿cuál es el mejor periodo para la venta de bicicletas? ¿A qué podría deberse este motivo?

## Paso 1: Origen de datos (operacional)
Para simular los datos disponibles en el operacional, partiremos de la BBDD disponible en el siguiente repositorio de Microsoft [AdventureWorksDW](
https://github.com/microsoft/sql-server-samples/tree/master/samples/databases/adventure-works/data-warehouse-install-script).

Donde los archivos que utilizaremos serán:
- DimProduct.csv - Dimensión producto
- DimDate.csv - Dimensión calendario
- DimSalesTerritory.csv - Dimensión territorio
- DimCustomer.csv - Dimensión cliente
- FactInternetSales.csv - Hechos, venta de bicicletas

## Paso 2: Ingesta inicial de los datos
Como parte de las buenas prácticas dentro de Fabric para realizar la ingesta, se recomienda aplicar una operación de "Copy data" con un Pipeline de Data Factory para generar una extracción inicial de los datos, desde el origen a nuestro Lakehouse. Esto generará una copia inicial de los datos en la capa de RAW siguiendo la metodología definida en la arquitectura "Medallion".

El Pipeline desarrollado para realizar esta acción tiene el siguiente aspecto:

![image](https://github.com/fuster-10/MicrosoftFabricDemo/assets/29040162/168e054c-9c18-4c75-9f30-57e76944495f)

Los pasos llevados a cabo en el Pipeline anterior son:
1. Inicializamos variables para la gestión del proceso:
   - Archivos de entrada:
     ```python
     ["DimProduct.csv","DimCustomer.csv","DimSalesTerritory.csv","DimDate.csv","FactInternetSales.csv"]
     ```
   - Archivos de salida:
     ```python
     ["DimProduct.parquet","DimCustomer.parquet","DimSalesTerritory.parquet","DimDate.parquet","FactInternetSales.parquet"]
     ```
     
2. Entramos en bucle For-Each, donde dentro:
   1. Realizamos operación Copy data para el primero de los elementos de la lista.
   2. Guardamos elemento en el LakeHouse con el nombre definido en la segunda lista en formato _.parquet_.
   3. Modificamos variables para la siguiente iteración del proceso.


## Paso 3: Capa intermedia - Curated
En esta fase, se aplicarán unas sencillas transformaciones a los datos usando PySpark. Las mismas están disponibles en el notebook [Demo_Fabric_Curated](https://github.com/fuster-10/MicrosoftFabricDemo/blob/main/Jupyter%20Notebooks/Demo_Fabric_Curated.ipynb).

En resumen, las transformaciones aplicadas en este punto son:
- Selección de campos deseados de cada fichero.
- Conversión de campos al tipo de dato correcto.
- Generación de columnas calculadas para determinadas entidades.
- Guardado de cada fichero transformado en formato _.parquet_ en la capa CURATED.

A continuación, se muestran las operaciones realizadas:
  
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

Las operaciones realizadas están disponibles en el script [Demo_Fabric_Consumption](https://github.com/fuster-10/MicrosoftFabricDemo/blob/main/Jupyter%20Notebooks/Demo_Fabric_Consumption.py).

Esta fase será realizada a través de un Spark Job.

Nuestra tabla _FactInternetSales_ contiene los siguientes campos:

| Campo              | Tipo de dato  |
| -------------      | ------------- |
| ProductKey         | integer       |
| OrderDateKey       | integer       |
| DueDateKey         | integer       |
| ShipDateKey        | integer       |
| CustomerKey        | integer       |
| CurrencyKey        | integer       |
| SalesTerritoryKey  | integer       |
| SalesOrderNumber   | string        | 
| OrderQuantity      | integer       |
| UnitPrice          | double        |
| TotalSales         | double        |

Con la agregación realizada, pasaremos a disponer de la siguiente tabla, _FactSalesSummary_:

| Campo              | Tipo de dato  |
| -------------      | ------------- |
| ProductKey         | integer       |
| CustomerKey        | integer       |
| SalesTerritoryKey  | integer       |
| OrderYear          | integer       | 
| OrderMonth         | integer       |
| TotalSalesAmount   | double        |
| TotalItems         | long          |
| TotalOrders        | long          |
| DateKey            | integer       |

Es decir, pasaremos de tener el detalle de nuestras ventas online, determinadas por el campo _SalesOrderNumber_, a disponer de una tabla agregada a nivel de _ProductKey_, _CustomerKey_, _SalesTerritoryKey_, _OrderYear_, y _OrderMonth_. Además, se crearán las tablas deltas correspondientes a cada una de las entidades tratadas. 

### Importación inicial de funciones
```python
from pyspark.sql.types import *
from pyspark.sql.functions import concat, lit, datediff, current_date, when, col, substring, sum, length, countDistinct
import sys
import os
#import Constant
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
```
### Definición de la Spark Session
```python
#Spark session builder
    spark = (SparkSession
          .builder
          .appName("example") 
          .config("spark.some.config.option", "some-value")
          .getOrCreate())
    
    spark_context = spark.sparkContext
    spark_context.setLogLevel("DEBUG")
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

## Paso 5: Creación de las tablas deltas

Esta operación será realizada por medio de un dataflow. Para cada una de las entidades del modelo, seleccionaremos como origen de datos Azure Data Lake Storage Gen 2, y filtraremos el nombre del archivo parquet para que corresponda con el que fue escrito en la fase previa del proceso.

El código de Power Query para la creación de la tabla delta correspondiente a **DimCustomer** sería:

```python
let
  Origen = AzureStorage.DataLake("https://onelake.dfs.fabric.microsoft.com/61830d7c-c332-46d5-95d3-249737c2e475/fe75465a-14f3-406b-b5e8-1a17a61cbc78/Files/CONSUMPTION"),
  #"Filas filtradas" = Table.SelectRows(Origen, each Text.Contains([Folder Path], "DimCustomer")),
  #"Filas filtradas 1" = Table.SelectRows(#"Filas filtradas", each ([Extension] = ".parquet")),
  #"Elegir columnas" = Table.SelectColumns(#"Filas filtradas 1", {"Content"}),
  #"Archivos ocultos filtrados" = Table.SelectRows(#"Elegir columnas", each [Attributes]?[Hidden]? <> true),
  #"Invocar función personalizada" = Table.AddColumn(#"Archivos ocultos filtrados", "Transformar archivo", each #"Transformar archivo"([Content])),
  #"Se han quitado otras columnas." = Table.SelectColumns(#"Invocar función personalizada", {"Transformar archivo"}),
  #"Columna de tabla expandida" = Table.ExpandTableColumn(#"Se han quitado otras columnas.", "Transformar archivo", Table.ColumnNames(#"Transformar archivo"(#"Archivo de ejemplo"))),
  #"Tipo de columna cambiado" = Table.TransformColumnTypes(#"Columna de tabla expandida", {{"CustomerKey", Int64.Type}, {"FirstName", type text}, {"LastName", type text}, {"BirthDate", type date}, {"CivilState", type text}, {"Gender", type text}, {"YearlyIncome", Int64.Type}, {"FullName", type text}, {"Age", type number}})
in
  #"Tipo de columna cambiado"
```
El código completo asociado a este paso se encuentra en:
[Creación de deltas con dataflows](https://github.com/fuster-10/MicrosoftFabricDemo/blob/main/Jupyter%20Notebooks/Delta%20Creation.txt)

En este punto, disponemos de nuestras tablas delta en el Lakehouse "Demo Fabric" (denotadas por el icono del triángulo):

![image](https://github.com/fuster-10/MicrosoftFabricDemo/assets/29040162/5afa6523-9fa7-43af-836b-ab6892c55c1d)

Tras la realización de este punto, recapitulamos los pasos seguidos:
1. Ingesta da datos - Pipeline Data Factory.
2. Transformaciones iniciales - Jupyter Notebook.
3. Creación de la tabla agregada - Spark Job.
4. Creación de las tablas delta - Dataflow.

Estos cuatro pasos mostrados, representan los distintos medios de procesamiento de datos disponibles en Fabric. Una vez comprobado el correcto funcionamiento de cada uno de los pasos del proceso, se puede definir un elemento de orquestación principal. Una posibilidad sería la creación de un Pipeline de Data Factory que lanzara cada uno de los pasos mostrados anteriormente.
![image](https://github.com/fuster-10/MicrosoftFabricDemo/assets/29040162/32162ae5-0a7a-4dcd-9d6c-bb017b30e861)



## Paso 6: Creación del modelo semántico en Power BI

Enhorabuena, ya estamos cerca de poder finalizar nuestro proyecto y poder reportar la información solicitada. Solo queda un último punto antes de poder comenzar a desarrollar nuestra informe de Power BI: *Construir el modelo semántico*. Esto es posible, debido a que ya tenemos en disposición las tablas finales con las que poder construir el modelo de datos a usar en el dashboard. Sin embargo, para abordar esta operación podemos optar por dos alternativas:
- Opción 1: Definición de las relaciones del modelo semántico desde el servicio de Power BI.
- Opción 2: Definición de las relaciones del modelo semántico a través Power BI Desktop.

Como buena práctica, es recomendable el uso de la opción 1, ya que garantiza una mayor reusabilidad. No obstante, la opción 2, es posible y también será explicada.

### Opción 1: Definición de las relaciones del modelo semántico desde el servicio de Power BI.

Par ello, accederemos a la opción "Punto de conexión de análisis SQL":

![image](https://github.com/fuster-10/MicrosoftFabricDemo/assets/29040162/394fcfe5-f950-4407-b2bc-c7558e74a4d5)

En esta pestaña se puede observar lo siguiente:

![image](https://github.com/fuster-10/MicrosoftFabricDemo/assets/29040162/f7217122-7d33-4eb3-a98d-5b4a8daf1b5a)

Durante la creación del Lakehouse, Microsoft Fabric crea de forma automática un SQL end point (solo lectura) al que poder conectarse y contra el que ejecutar queries para leer información de los datos. En el mismo, hay tres pestañas disponibles (esquina inferior izquierda):
- Datos: Visualización de los datos disponibles.
- Consulta: Desarrollo de queries SQL para realizar consultas sobre los datos.
- Modelo: Construcción del modelo semántico de Power BI para ser usado en la generación de informes.

Para pasar a establecer las relaciones entre nuestras tablas, haremos clic en **Creación de informes -> Administrar modelo semántico predeterminado**. En este apartado, estableceremos las relaciones entre nuestras tablas, de acuerdo a la cardinalidad con la que se encuentren relacionadas, por lo tanto, crearemos:
- Relación 1:N entre ***dim_date*** y ***fact_internet_sales*** mediante los campos DateKey y OrderDateKey.
- Relación 1:N entre ***dim_product*** y ***fact_internet_sales*** mediante el campo Product_Key.
- Relación 1:N entre ***dim_sales_territory*** y ***fact_internet_sales*** mediante el campo Sales_Territory_Key.
- Relación 1:N entre ***dim_customer*** y ***fact_internet_sales*** mediante el campo Customer_Key.
- Relación 1:N entre ***dim_date*** y ***fact_summary mediante*** el campo DateKey.
- Relación 1:N entre ***dim_product*** y ***fact_summary*** mediante el campo Product_Key.
- Relación 1:N entre ***dim_sales_territory*** y ***fact_summary*** mediante el campo Sales_Territory_Key.
- Relación 1:N entre ***dim_customer*** y ***fact_summary*** mediante el campo Customer_Key.

El resultado final es el siguiente:

![image](https://github.com/fuster-10/MicrosoftFabricDemo/assets/29040162/232ceda8-c866-4378-b667-79778ffcee9e)

En el mismo se puede observar que nuestro modelo consta de cuatro tablas de dimensión y dos tablas de hechos (una detallada y otra agregada).
En este punto, estamos en virtud de desarrollar nuestro informe de Power BI.

## Paso 6: Desarrollo del Dashboard en Power BI

A continuación, pasaremos a desarrollar los informes basándonos en el modelo semántico construido. Podríamos partir de cero y crear reportes de la nada desde el servicio o facilitar a usuarios de negocio la conexión al modelo de datos para creación de autoservicios. No obstante, como nuestros requerimientos son claros vamos a optar por la generación de un Dashboard de Power BI para cubrir nuestras necesidades.

Para ello, seguiremos los siguientes pasos:
1. Selección del origen de datos:
   1. Abrimos un nuevo archivo desde Power BI desktop.
   2. Seleccionamos la opción "Get Data" y "Power BI Datasets" (opciones disponibles para Power BI en Inglés).
   3. Seleccionamos la desarrollada para la demo, "Demo_Fabric".
2. Esperamos a que se genere una conexión en vivo a nuestro modelo de datos (aparecerá abajo a la derecha como "Connected live to the Power BI dataset: Nombre del modelo semántico".
3. Guardamos el archivo con el nombre "Bluetab Bikes" en nuestro directorio de trabajo deseado.
4. En este punto, ya tendríamos todo lo necesario para generar las gráficas de nuestro informe. No obstante, esta parte va a ser obviada en la presentación por el tiempo que conlleva. En su lugar mostraremos el informe finalizado.
5. Una vez finalizados los desarrollos, publicamos el informe en el workspace deseado.

¡¡Y ya habríamos finalizado!! Enhorabuena por haber llegado hasta aquí porque eso quiere decir que ya hemos conseguido completar nuestros desarrollos (o mejor dicho, ¡qué lástima! con lo bien que nos lo estábamos pasando... :( )

Ya solo quedaría usar el dashboard para dar respuesta a las peticiones de dirección.

## Paso 7: Contestando a las preguntas de Dirección






