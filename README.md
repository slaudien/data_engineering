# New Yorker - Data Engineering Excerise

# 1. Verwendete Smack-Technologie: Spark

## Begründung:
- Spark ermöglicht das Einlesen von JSONs file und
  das Anzeigen von Daten in tabellarischer Form.
  Außerdem können auf Spark Dataframes bereits joins und 
  Analysen ausgeführt werden, die die Korrektheit der Daten überprüfen können

## Erweiterbarkeit durch die anderen SMACK Technologien ...
   ... wenn das System u.a. über mehrere Knoten verteilt werden könnte. 
- Mesos wäre die erste Technologie, die die Performance dieses
  Programms verbessern könnte. Dadurch wäre eine Lastverteilung auf mehrere 
  Konten möglich. Ein Docker Container könnte dabei pro Knoten erstellt werden.
- Kafka könnte seinen Einsatz finden, damit die Daten direkt beim Entpacken
  der tar-files in eine Kafka Queue gestreamt werden. Zudem könnte
  mit Hilfe von Kafka bereits eine Verteilung der Daten auf mehrere Knoten erfolgen.
- Cassandra könnte verwendet werden um die von Spark eingelesen json files
  in tabellarischer Form abzuspeichern. Darauf könnten dann weitere Analysen
  erstellt werden. Dafür müsste man vorab ein entsprechendes Cassandra Datenbank
  Schema erstellen. Für die aktuelle Aufgabenstellung war dies aufgrund
  der analytischen Fähigkeiten von Spark nicht notwendig.


# 2. Verwendung des Programs

## Setup des Docker containers

### Erstellung des Docker Containers
`docker build -t ny_data_engineering:latest .`

### Starten des Docker images (Anpassen von Pfaden)
`shared_docker_host_path=<Pfad zu diesem Projekt>/shared`

z.B.: 
`shared_docker_host_path=/Users/NY_USER/git/data_engineering/shared`

`docker run -d -it --name=ny_spark_container -v ${shared_docker_host_path}:/home/host  ny_data_engineering:latest`

## Entpacken der Tar-files

Bitte laden Sie das Yelp Datenfile selbstständig herunter.
Im Weiteren wird erklärt wo diese abgelegt werden sollten.

### Option 1 - Ausführung im Docker Container
Das tar-file Befindet sich in einem beliebigen Ordner des Docker Containers.
(Zum Kopieren des tar-files in den Docker Container kann unter Anderem das shared directory verwendet werden.)

Einloggen in den Container: `docker exec -it ny_spark_container bash`

Ausführen des Unpack-Programms: 
`/home/spark/bin/spark-submit unpack.py --tar-location <path_to_tar_in_container>`

### Option 2 - Ausführung auf dem Docker Host
Das tar-file mit den Daten befindet sich im shared directory des Docker Containers mit dem Docker Host:

`docker exec -it ny_spark_container /home/spark/bin/spark-submit /home/host/unpack.py --tar-location /home/host/<tar-file-name>`

z.B.: 
`docker exec -it ny_spark_container /home/spark/bin/spark-submit /home/host/unpack.py --tar-location /home/host/yelp_dataset.tar`

## Analysieren der Json-Files
(Beinhaltet Einlesen der Json-Files in Spark)

### Option 1 - Ausführung im Docker Container
Einloggen in den Container: `docker exec -it ny_spark_container bash`
Ausführen des Analyse-Programms: `/home/spark/bin/spark-submit /home/host/analyze_json.py <option>`

### Option 2 - Ausführung auf dem Docker Host
`docker exec -it ny_spark_container /home/spark/bin/spark-submit /home/host/analyze_json.py <option>`

z.B.:
`docker exec -it ny_spark_container /home/spark/bin/spark-submit /home/host/analyze_json.py --query1`


### Verfügbare Optionen zur Analyse der JSON Dateien

usage: analyze_json.py [-h] [--all-queries] [--query1] [--query2] [--query3]
                       [--query4] [--query5] --number-of-lines NUMBER_OF_LINES

New Yorker Application

optional arguments:
  -h, --help            show this help message and exit
  --all-queries         Run all available queries
  --query1              Query1: Most useful reviews for a business with at least 3 stars
  --query2              Query2: Most reviews by user for a city.
  --query3              Query3: Show businesses which have photos order by stars
  --query4              Query4: Most tips written by user joined Yelp before 2017
  --query5              Query5: Most reviews in a city
  --number-of-lines NUMBER_OF_LINES
                        Specify how many rows should be returned (Default: 20)


## Zusätzliche Kommentare

- Eine Zusammenführung von beiden Programmteilen wäre ebenfalls möglich

## Nächste Schritte:

- Error-Handling von allen Programmteilen
- Test-Programme (Unit-tests, ...)


