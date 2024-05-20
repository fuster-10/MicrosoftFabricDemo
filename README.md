# Microsoft Fabric Demo
Bienvenidos a la Demo de Microsoft Fabric. Este README.md describe los pasos seguidos durante la demo. Se recomienda seguir este documento al mismo tiempo que la presentación para sacarle el máximo partido.

Para el ejercicio escogido, simularemos la existencia de una tienda online de bicicletas, "Bluetab Bikes". Desde negocio, se ha solicitado al equipo de Analítica de Datos que desarrolle un dashboard en Power BI que permita analizar las ventas de las bicicletas por Región. Además, quieren que la solución se abordada mediante Microsoft Fabric.

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

### Importación inicial
```python
from pyspark.sql.types import *
from pyspark.sql.functions import concat, lit, datediff, current_date, when, col, substring, sum, length, countDistinct
```

## Paso 3: Capa intermedia - Curated
En esta fase, se aplicarán unas sencillas tranformaciones a los datos usando PySpark. Las mismas están disponibles en el notebook Demo_Fabric_Curated (https://github.com/fuster-10/MicrosoftFabricDemo/blob/main/Demo_Fabric_Curated.ipynb)

En resumen, las transformaciones aplicadas en este punto son:
- Seleccion de campos deseados de cada fichero.
- Conversión al tipo de dato correcto.
- Generación de columnas calculadas para determinadas entidades.
- Guardado de cada fichero transformado en formato _.parquet_ en la capa CURATED.

## Paso 3: Capa final - Consumption
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



