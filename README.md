## Data_engineering_project
Design and implement a data pipeline using Apache Airflow for internet memes


## How to spin the webserver up

### Prepping

First, get your **id**:
```sh
id -u
```

Now edit the **.env** file and swap out 1000 for your own.

Run the following command to creat the volumes needed in order to send data to airflow:
```sh
mkdir -p ./logs ./plugins
```

Download this file and add it to the ./dags folder:

```sh
https://owncloud.ut.ee/owncloud/index.php/s/g4qB5DZrFEz2XLm/download/kym.json
```

And this **once**:

```sh
docker-compose up airflow-init
```
If the exit code is 0 then it's all good.

### Running

```sh
docker-compose up
```

After it is up, go to
```sh
http://localhost:8080/
```
Log in:
```sh
* Username - airflow
* Password - airflow
```

Add a new connection:
```sh
* Name - postgres_default
* Conn type - postgres
* Host - localhost
* Port - 5432
* Database - airflow
```
