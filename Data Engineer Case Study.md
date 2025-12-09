Descripción de los datos
Hemos configurado una cola de AWS SQS que recibe continuamente datos simulados de eventos de comercio electrónico.
Su tarea consistirá en leer mensajes de esta cola, transformarlos y diseñar una
tabla analítica. A continuación, encontrará instrucciones detalladas para acceder a los datos.
Esta cola emitirá continuamente eventos de usuario que siguen el mismo esquema descrito a continuación.
Los mensajes se codifican en formato JSON.
Cada mensaje de la cola contiene un único evento con los siguientes campos:
Columna Tipo Descripción

event_timestamp int Marca de tiempo del evento en milisegundos
user_id string Identificador único para el usuario
event_name string Tipo de evento:
● view_item_list: el usuario vio una lista de artículos
● view_item: el usuario vio una página de detalles del producto
● begin_checkout: el usuario inició el pago
● purchase: el usuario completó una compra
platform string Ya sea IOS o ANDROID
items json Carga útil JSON que describe los artículos o listas involucrados en el evento

Objetivo
Your task is to design and build a table that allows analysts from the Growth Team to create
dashboards and reports.
You do not need to build the dashboard itself, only the analytical table(s). However, if time
possible, a brief exploratory analysis or key insights would be a plus.
Entregables
1. Modelado de datos
Propose the schema for one or more analytical tables that will help answer:
● Which lists engage users the most?
● Which products perform best (views → purchases)?
Your table design should make it straightforward to calculate top e-commerce metrics such as
Click-through rate (CTR), Bounce rate, Product impressions, Conversion rate, etc.
Puede optar por diseñar una única tabla unificada o varias tablas derivadas (por ejemplo,
product_metrics, list_metrics, etc.).

2. Tubería ETL/ELT
Implemente un proceso local para consumir datos de la cola de SQS y crear sus
tablas analíticas.
Puede usar el enfoque que prefiera (p. ej., Python, Spark Structured Streaming, etc.).
Su solución debe:
● Consumir mensajes de la cola
● Gestionar datos JSON anidados en el campo de elementos
● Analizar y transformar los datos en su esquema analítico.
Puede usar una base de datos local, DuckDB, SQLite, Postgres o un conjunto de datos Parquet como
almacén analítico.

3. Discusión de la solución
Describa brevemente su solución.
● Qué tecnologías utilizó y por qué
● Describa el flujo de datos de extremo a extremo
● Consideraciones sobre la consistencia y la latencia de los datos
● Compensaciones y decisiones de diseño
● Analice brevemente cómo se escalará su diseño a medida que aumenta el volumen de datos
Debe estar preparado para justificar su propuesta en caso de que necesitemos analizar sus decisiones
con el equipo de ingeniería.

Acceso y credenciales de SQS
Los datos analíticos se escriben en una cola de SQS: data-engineering-case-analytics-queue.
Puede acceder a la consola de AWS y obtener claves de acceso programático iniciando sesión en:
https://d-9067bc89f6.awsapps.com/start/

Pautas de presentación
Envíe lo siguiente:
● Su código o cuaderno (.py, .ipynb, .sql, etc.)
● Un documento breve (Google Doc, Markdown o PDF) que describa:

○ Su enfoque y suposiciones
○ El esquema de su(s) tabla(s) final(es)
○ Su diseño de transmisión de datos