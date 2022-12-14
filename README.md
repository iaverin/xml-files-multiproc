# xml-files-multiproc

## Задача
Написать программу на Python, которая делает следующие действия:

1. Создает 50 zip-архивов, в каждом 100 xml файлов со случайными данными следующей структуры:
   ```xml
    <root>
        <var name=’id’ value=’<случайное уникальное строковое значение>’/>
        <var name=’level’ value=’<случайное число от 1 до 100>’/>
        <objects>
            <object name=’<случайное строковое значение>’/>
            <object name=’<случайное строковое значение>’/>
            …
        </objects>
    </root>
    ```
    В тэге objects случайное число (от 1 до 10) вложенных тэгов object.

2. Обрабатывает директорию с полученными zip архивами, разбирает вложенные xml файлы и формирует 2 csv файла:
Первый: id, level - по одной строке на каждый xml файл
Второй: id, object_name - по отдельной строке для каждого тэга object (получится от 1 до 10 строк на каждый xml файл)
Очень желательно сделать так, чтобы задание 2 эффективно использовало ресурсы многоядерного процессора.


## Решение

### Как работает
1. Программа создает директорию ```zip-files``` и складывает туда zip файлы с XML . Если директории не было, то она создается. 
2. Zip архивы перезаписываются  и добавляются в ```zip-files```
3. Результат работы: ```csv_file_1.csv``` и ```csv_file_2.csv```

### Запуск
Тестировалось на Python 3.11.1.
Внешних зависимостей нет.
```shell
# запуск программы
python run.py

# запуск юнет тестов
python -m unittest
```

### Структура

Основные модули:
```
.
├── ngxmlzip
├──├── utils                  утилитарные функции для работы с файлами и выводом 
│  ├── ...
├──├── workers                воркеры для многопоточной обработки данных 
│  ├── ...
│  ├── tests                  юнит тесты для менеджера воркеров
│  ├── ...
│  ├── data_types.py          общие классы данных
│  ├── queue_manager.py       классы менеджера воркеров для обработки очередей
│  ├── runner_create.py       создание zip файлов с xml
│  ├── runner_process.py      обработка файлов и формирования результата
└── run.py                    основная программа
```

### Настройка:
В файле run.py возможно указать следующие константы
```python
MAX_OBJECTS_IN_XML = 10 # максимальное количество объектов в XML файле
XML_FILES_IN_ZIP = 100  # количество xml файлов в одном zip файле   
ZIP_FILES = 50  # количество xml файлов в одном zip файле   
ZIP_DIRECTORY = "zip-files"  # директория с zip файлами 
CSV_FILE_1 = "csv_file_1.csv" # csv файл результата №1
CSV_FILE_2 = "csv_file_2.csv" # csv файл результата №2
```
### [todo] Удобство использования
Для повышения удобства можно реализовать как и проброску параметров через командную строку так и возможность отдельного запуска частей программы. На уровне дизайна модулей этот функционал реализуем. 

## Сценарий обработки созданных файлов
- Основной процесс формирует список zip файлов, распаковывает их и складывает в очередь xml данных (одна запись - один xml файл)
- Группа дочерних процессов в количестве половины  физических процессоров забирают данные из очереди с содержанием xml файлов, обрабатывают и записывают результат в 2 очереди результатов для записи выходных файлов. Количество процессов может быть любым, однако если делать больше, то на данном в задаче объеме данных, время на инициацию процессов перевешивает экономию времени на параллельной обработке. (здесь хорошо бы еще добавить проверку для случая одного процессора ;)  
- 2 отдельных процесса берут данные из очередей с подготовленными данными для записи и записывают в соответствующие файлы
- Воркер, который записывает файл со списком объектов, совершает запись чанками (размер чанка можно настраивать), остальные воркеры обрабатывают очереди по 1 элементу 

## Контроль ошибок 
- Основная программа контролирует корректность работы, сравнивая кол-во сгенерированных элементов с количеством обработанных
- Результаты работы воркеров (количество записанных файлов и объектов) вычисляются из технических метрик работы воркеров
- Мониторинг работы воркеров в ран-тайме не реализован, однако его можно относительно легко реализовать посредством специальной очереди (использовался на раннем этапе разработки, выпилен, так как не было требований)  

## Метрики работы воркеров 
Технические результаты работы воркеров выводятся после их завершения в формате:

```python
QueueAllWorkerInstancesResult(worker_name='csv_file_2',     # название воркера
                              instances=1,                  # количество инстансов  
                              errors=[],                    # ошибки при работе (исключения) собранные со всех инстансов воркеров
                              total_worker_calls=3487,      # общее кол-во вызовов всех инстансов воркеров 
                              successful_worker_calls=3487, # общее кол-во успешных вызова всех инстансов воркеров  
                              records_processed=27035,      # количество обработанных элементов (этот параметр настраивается на этапе реализации воркера)
                              max_queue_size=384,           # максимальная длина очереди, ассоциированной с воркером 
                              queue_size_on_start=383,      # максимальная длина очереди на старте воркера (среди всех инстансов)
                              max_chunk_size=990,           # максимальное количество обработанных чанков за один вызов воркера (среди всех инстансов)
                              context={})                   # не используется, планировался использоваться для возможности обмениваться контекстом менеджера воркера и инстанса воркера

```


Спасибо за внимание.
Задача очень крутая ) 


