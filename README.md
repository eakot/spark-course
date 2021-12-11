# Гуйд

## Зайти внутрь контейнера
1. Убрать CMD EXEC из Dockerfile  
2. Сбилдить образ: 
```
sudo docker build -t task_2 .  
```  
3. Запустить: 
```
sudo docker run -d task_2 sleep 300 
```
4. Узнать container_id: 
```
sudo docker ps -a
```
5. Зайти:
```
sudo docker exec -it 030952ced479 bash
```

## Запуск спарк-тасок по загрузке и трансформации данных
1. В *powershell*:
```
run
```
2. После того, как таски отработали, можно остановить postgresql + удалить контейнеры и скаченные данные:
```
stop_hard
```
3. Либо остановить postgresql, оставить контейнеры и данные:
```
stop_soft
```