# Скачать данные в папку data
```
mkdir -p ./dags ./logs ./plugins
echo -e "AIRFLOW_UID=$(id -u)" > .env
sudo chmod 777 ./dags
sudo chmod 777 ./logs
sudo chmod 777 ./plugins
sudo chmod 777 ../data
```

# Сбилдить и запустить airflow
```
sudo docker-compose down
sudo docker-compose build
sudo docker-compose up
```

# Перейти в веб - интерфейс Airflow
```
В строке браузера localhost:8080,
в окне авторизации user: airflow  password:airflow

Искомый DAG с dag_id 'download_and_write_to_db'
```

# Результаты работы:
в файлах parquet в папке data и в таблицах public.views_count, public.purchases_count базы данных postgres

