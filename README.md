# Скачать данные в папку data
```
sudo docker-compose build download_habr_data
sudo docker-compose up download_habr_data
```

# Запустить цеппелин
```
sudo docker-compose build download
sudo docker-compose up -d zeppelin
```

# Зайти внутрь контейнера
убрать CMD EXEC из Dockerfile
сбилдить образ: sudo docker build -t task_2 .    
запустить: sudo docker run -d task_2 sleep 300 
узнать container_id: sudo docker ps -a    
зайти sudo docker exec  -it 030952ced479 bash

