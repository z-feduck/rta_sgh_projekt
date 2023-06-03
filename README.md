# rta_sgh_projekt
1. Odpalamy kontener
2. W konsoli na dockerze instalujemy bibliotekę pythonową confluent_kafka
*pip install confluent_kafka*
4. Tworzymy nowy topic o nazwie np test (lokalna konsola)
*docker exec broker kafka-topics --bootstrap-server broker:9092 --create --topic test*
4. Towrzymy sztuczy strumień danych z pliku csv. W konsoli na dockerze odpalamy komendę *python sendStream.py covid.csv test*
5. Tworzymy producera
*spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1 app.py*
