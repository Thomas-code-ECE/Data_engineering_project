## Requirements

* To have docker *and* docker-compose installed.
* Install docker and docker-compose exactly as it is described in the website.
* **do not do do apt install docker or docker-compose**

## How to spin the webserver up

### Prepping

First, get your **id**:
```sh
id -u
```

Now edit the **.env** file and swap out 1000 for your own.

Run the following command to creat the volumes needed in order to send data to airflow:
```sh
mkdir -p ./dags ./logs ./plugins
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
