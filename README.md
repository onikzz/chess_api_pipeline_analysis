The system implements an automated data flow that integrates cloud-based extraction, processing, and storage, structured under modern 
data architecture principles. The main objective is to transform raw data from an Application Programming Interface (API) into information 
assets ready for business analysis.

 --- General Pipeline Operation ---
The process begins with the Ingestion phase, where requests are made to the Chess.com API to retrieve player profiles and game statistics. 
This stage handles authentication and JSON document recovery. These files are stored in their original state within an Amazon S3 bucket, 
serving as a raw data repository or Bronze layer.

The Data Processing phase employs Apache Spark to transform raw JSON files by flattening nested structures, converting data types, and 
cleaning records. Results are stored in Parquet format, optimizing the efficiency of subsequent queries through columnar storage.

The final Analytics stage executes aggregations to generate performance indicators, such as average ELO by country. Using window functions, 
leaders are identified by region, and the results are deposited into an optimized layer for business intelligence tools like Power BI.

 --- Orchestration and Infrastructure --- 
The coordination of all these tasks is performed via Apache Airflow. This system manages script dependencies, ensuring that data cleaning 
does not begin until the extraction has successfully completed.

The foundation of this system was built using Terraform. This tool allows for the definition of cloud resources, such as S3 storage buckets, 
through configuration files. This ensures that the data environment is predictable and can be recreated identically across different 
accounts or cloud regions without manual intervention.

 --- Project Key Learnings and Conclusions ---
This project represents the transition from theory to practice in the use of professional-level data engineering tools. The development 
process consolidated fundamental knowledge in several areas:

 - Infrastructure Management: Understanding the importance of defining resources through code (IaC) to avoid manual configurations.

 - utomated Workflows: Using Airflow provided an understanding of the difference between running isolated scripts and building an 
   orchestrated pipeline that logically handles failures, retries, and dependencies.

 - Data Handling at Scale: The implementation of PySpark demonstrated how to process data volumes exceeding the capacity of traditional 
   tools, successfully structuring information into layers using a Medallion Architecture to ensure data traceability.

 - Ecosystem Integration: A major learning point was the connection of components, enabling Python code to interact with cloud services, 
   distributed processing engines, and Docker containers.
------------------------------------------------------------------------------------------------------------------------------------------

El sistema implementa un flujo de datos automatizado que integra la extracción, el procesamiento y el almacenamiento en la nube, 
estructurado bajo los principios de la arquitectura de datos moderna. El objetivo principal es transformar datos crudos provenientes 
de una interfaz de programación de aplicaciones (API) en activos de información listos para el análisis de negocio.

 --- Funcionamiento General del Pipeline ---
El proceso comienza con la fase de Ingesta, donde se realizan peticiones a la API de Chess.com para obtener perfiles de jugadores y 
sus estadísticas de juego. Esta etapa maneja la autenticación y la recuperación de documentos en formato JSON. Estos archivos se 
almacenan en su estado original dentro de un bucket (Amazon S3), cumpliendo con la función de un 
repositorio de datos crudos o capa Bronze.

La fase de Procesamiento de Datos emplea Apache Spark para transformar archivos JSON crudos mediante el aplanado de estructuras anidadas, 
conversión de tipos de datos y limpieza de registros. Los resultados se almacenan en formato Parquet, optimizando la eficiencia de las 
consultas posteriores gracias a su almacenamiento columnar.

La etapa final de Analítica ejecuta agregaciones para generar indicadores de rendimiento, como el promedio de ELO por país. Mediante 
funciones de ventana, se identifica a los líderes por región y los resultados se depositan en una capa optimizada para herramientas de 
inteligencia de negocios como Power BI.

 --- Orquestación e Infraestructura --- 
La coordinación de todas estas tareas se realiza mediante Apache Airflow. Este sistema se encarga de gestionar las dependencias entre los 
scripts, asegurando que la limpieza de los datos no comience hasta que la extracción haya finalizado exitosamente.

La base donde reside este sistema fue construida utilizando Terraform. Esta herramienta permite definir los recursos de la nube, como los 
contenedores de almacenamiento (buckets de S3), mediante archivos de configuración. Esto garantiza que el entorno de datos sea predecible 
y pueda ser recreado de forma idéntica en diferentes cuentas o regiones de la nube sin intervención manual.

 --- Aprendizajes y Conclusiones del Proyecto  --- 
Este proyecto representa la transición de la teoría a la práctica en el uso de herramientas de nivel profesional para la ingeniería de datos. 
El desarrollo ha permitido consolidar conocimientos fundamentales en diversas áreas:

 - Gestión de Infraestructura: Se comprendió la importancia de definir recursos mediante código (IaC) para evitar configuraciones manuales.

 - Flujos de Trabajo Automatizados: El uso de Airflow permitió entender la diferencia entre ejecutar scripts de forma aislada y construir 
   un pipeline orquestado que maneja fallos, reintentos y dependencias de manera lógica.

 - Manejo de Datos a Escala: La implementación de PySpark permitió procesar volúmenes de datos superiores a la capacidad de herramientas 
   tradicionales, logrando estructurar la información en capas mediante una Medallion Architecture para asegurar la trazabilidad del dato.

 - Integración de Ecosistemas: Uno de los mayores aprendizajes fue la conexión de componentes, logrando que el código en Python interactúe 
   con servicios en la nube, motores de procesamiento distribuido y contenedores de Docker.

![alt text](image.png)