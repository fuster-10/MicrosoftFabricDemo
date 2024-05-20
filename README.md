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
- DimSalesTerritory.csv (Dimensión)
- DimCustomer.csv (Dimensión)
- FactInternetSales.csv (Hecho)

## Paso 2: Ingesta inicial de los datos
Como parte de las buenas prácticas dentro de Fabric para realizar la ingesta, se recomienda aplicar una operación de "Copy data" con un Pipeline de Data Factory para generar una extracción inicial de los datos, desde el origen a nuestro Lakehouse. Esto generará una copia inicial de los datos en la capa de RAW siguiendo la metodología definida en la arquitectura "Medallion".

El Pipeline desarrollado para realizar esta acción, se encuentra ubicado en la siguiente dirección (desafortunadamente, no se puede visualiazar el contenido del pipeline sin tener acceso al worksapce. En caso de querer conocer más detealle, por favor, contactadme por privado):
https://app.fabric.microsoft.com/groups/61830d7c-c332-46d5-95d3-249737c2e475/pipelines/d673ab56-e5d7-4996-aa02-913107e44561?experience=data-engineering


