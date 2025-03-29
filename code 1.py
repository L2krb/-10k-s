import time
from urllib.request import urlopen
import os 
from joblib import Parallel, delayed
from tqdm import tqdm 
import glob 
import requests
import json

# * **time**: Используется для управления задержками между повторными попытками загрузки файлов.
# * **os**: Предоставляет функции для взаимодействия с операционной системой, такие как создание каталогов.
# * **joblib**: Используется для параллельной обработки, позволяя выполнять несколько загрузок одновременно.
# * **tqdm**: Библиотека для отображения индикаторов выполнения в циклах, улучшающая взаимодействие с пользователем во время загрузки.
# * **glob**: Используется для сопоставления шаблонов файлов, помогая находить уже загруженные файлы.
# * **requests**: Популярная библиотека для выполнения HTTP-запросов, используемая для извлечения данных с веб-сайта SEC.
# * **json**: Используется для обработки данных JSON, в частности при сохранении главного индекса.


headers = {'user-agent': 'Download for Research QMUL. At the moment, I need the 10-K files. name: AU'}
headers = {'Host': 'www.sec.gov', 'Connection': 'close',
         'Accept': 'application/json, text/javascript, */*; q=0.01', 'X-Requested-With': 'XMLHttpRequest',
         'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.163 Safari/537.36',
         }
# * Этот словарь содержит заголовки HTTP, которые отправляются с запросами для идентификации клиента, делающего запрос. Это помогает избегать блокировок со стороны сервера.


class MasterIndexRecord:
    def __init__(self, line):
        self.err = False
        parts = line.split('|')
        if len(parts) == 5:
            self.cik = int(parts[0])
            self.name = parts[1]
            self.form = parts[2]
            self.filingdate = int(parts[3].replace('-', ''))
            self.path = parts[4]
            self.fname= str(self.filingdate) + '_' + self.form.replace('/', '-') + '_' + self.path.replace('/', '_')
        else:
            self.err = True
        return
    
# * **Назначение**: Этот класс представляет запись из файла главного индекса. Он инициализирует атрибуты на основе строки текста из индекса.
# * **Атрибуты**:
# * `cik`: Центральный ключ индекса, уникальный идентификатор для компаний.
# * `name`: Название компании.
# * `form`: Тип подачи (например, 10-K).
# * `filingdate`: Дата подачи, отформатированная как целое число.
# * `path`: Путь к документу на сервере SEC.
# * `fname`: Отформатированное имя файла для сохранения документа локально.


def download_masterindex(year, qtr, save=False):
    number_of_tries = 10
    sleep_time = 10  # Note sleep time accumulates according to err
    sec_url = 'https://www.sec.gov/Archives/edgar/full-index/' + str(year) + '/QTR' + str(qtr) + '/master.idx'
    masterindex = []

#     * **Цель**: Загружает главный индекс за определенный год и квартал с веб-сайта SEC.
# * **Процесс**:
# * Пытается извлечь файл индекса несколько раз (до 10) в случае сбоев.
# * Анализирует файл индекса в объекты `MasterIndexRecord` и сохраняет их в списке.
# * При желании сохраняет необработанные данные индекса в виде файла JSON.




    for i in range(1, number_of_tries + 1):
        try:
            records = requests.get(sec_url, headers=headers, timeout=5).text.splitlines()[10:]
            if '-----------' in records[0]: records = records[1:]
            break
        except Exception as e:
            print("Master Index Problem: "+str(e))
            if i == number_of_tries:
                return False
            print('     Retry in {0} seconds'.format(sleep_time))
            time.sleep(sleep_time)
            sleep_time += sleep_time

    # Load m.i. records into masterindex list
    for line in records:
        mir = MasterIndexRecord(line)
        if not mir.err:
            masterindex.append(mir)

    if save:
        with open(fr'C:\Users\Antoine\Downloads\MasterIndex_{year}_{qtr}.json', 'w+') as f:
            json.dump(records, f)

    return masterindex





def download_to_file(_url, _fname):
    number_of_tries = 10
    sleep_time = 10  

# * **Цель**: Загружает файл с указанного URL-адреса и сохраняет его под указанным именем файла.
# * **Процесс**:
# * Подобно `download_masterindex`, он повторяет загрузку несколько раз, если обнаруживает ошибки.
# * Он обрабатывает исключения и выводит сообщения об ошибках, если загрузка не удалась.

    for i in range(1, number_of_tries + 1):
        try:
            r=requests.get(_url, headers=headers)
            with open(_fname, 'wb') as f:
                f.write(r.content)
            return
        except Exception as exc:
            if i == 1:
                print('\n==>requests error in download_to_file.py')
            print('  {0}. _url:  {1}'.format(i, _url))
            print('     _fname:  {0}'.format(_fname))
            print('     Warning: {0}  [{1}]'.format(str(exc), time.strftime('%c')))
            if '404' in str(exc):
                break
            print('     Retry in {0} seconds'.format(sleep_time))
            time.sleep(sleep_time)
            sleep_time += sleep_time

    print('\n  ERROR:  Download failed for')
    print('          url:  {0}'.format(_url))
    print('          _fname:  {0}'.format(_fname))

    return


def download_file(item, path):
    PARM_EDGARPREFIX = 'https://www.sec.gov/Archives/'
    url = PARM_EDGARPREFIX + item.path
    fname = item.fname
#     * **Цель**: Создает URL для определенного файла и вызывает `download_to_file` для его загрузки.
# * **Процесс**:
# * Проверяет, существует ли файл, чтобы избежать избыточных загрузок.
# * Использует атрибуты `path` и `fname` из `MasterIndexRecord` для корректного сохранения файла.

    
    if os.path.exists(path+'\\'+fname): return 
    download_to_file(url, path+'\\'+fname)
    return 


###############################################################################
###############################################################################
###############################################################################

# EDGAR FORMS #
f_10K = ['10-K', '10-K405', '10KSB', '10-KSB', '10KSB40']
f_10KA = ['10-K/A', '10-K405/A', '10KSB/A', '10-KSB/A', '10KSB40/A']
f_10KT = ['10-KT', '10KT405', '10-KT/A', '10KT405/A']
f_10Q = ['10-Q', '10QSB', '10-QSB']
f_10QA = ['10-Q/A', '10QSB/A', '10-QSB/A']
f_10QT = ['10-QT', '10-QT/A']
f_10X = f_10K + f_10KA + f_10KT + f_10Q + f_10QA + f_10QT
f_1X = ['1-A', '1-A/A', '1-K', '1-SA', '1-U', '1-Z']
# * Эти списки определяют типы отчетов SEC, которые скрипт заинтересован загрузить. Это позволяет легко фильтровать записи на основе их типа формы.

PARM_FORMS = f_10K+f_10KA+f_10KT  # or, for example, PARM_FORMS = ['8-K', '8-K/A']
PARM_BGNYEAR = 2020  # User selected bgn period.  Earliest available is 1994
PARM_ENDYEAR = 2024  # User selected end period.
PARM_PATH = r'E:\EDGAR 10-K'


def already_downloaded():
    files = glob.glob(PARM_PATH+'\**\*.txt', recursive=True)
    files = [os.path.basename(fname) for fname in files]
    print('Already done: '+str(len(files)))
    return files
# * **Цель**: Проверяет, какие файлы уже загружены, чтобы избежать дубликатов.
# * **Процесс**:
# * Использует `glob` для поиска всех файлов `.txt` в указанном каталоге и возвращает их имена.


def download_forms():
    done = already_downloaded()
# * **Цель**: Основная функция, которая организует процесс загрузки.
# * **Процесс**:
# * Выполняет итерации по указанным годам и кварталам.
# * Создает необходимые каталоги для хранения загруженных файлов.
# * Вызывает `download_masterindex` для получения списка файлов.
# * Фильтрует файлы на основе предопределенных форм и уже загруженных файлов.
# * Использует `Parallel` из `joblib` для одновременной загрузки нескольких файлов.


    # For each year and quarter
    for year in range(PARM_BGNYEAR, PARM_ENDYEAR + 1):
        for qtr in range(1, 5):
            
            # Create the folders if needed
            #path = '{0}\\{1}\\QTR{2}\\'.format(PARM_PATH, str(year), str(qtr))
            path = f'{PARM_PATH}/{year}/QTR{qtr}/'
            if not os.path.exists(path): 
                os.makedirs(path)
                
            # Get the master index
            masterindex = download_masterindex(year, qtr)
            if not masterindex:
                continue
            
            to_download = [item for item in masterindex if item.form in PARM_FORMS]
            to_download = list(set(to_download)-set(done))

            print(f'\nYear {year}, QTR {qtr}: {len(to_download)} files to download')
            print(f'Path: {path}')
            time.sleep(1)
            res = Parallel(n_jobs=4, prefer="threads")(delayed(download_file)(item, path) for item in tqdm(to_download))
    
###############################################################################
###############################################################################
###############################################################################


if __name__ == '__main__':
    download_forms()
    pass 
# * Этот блок проверяет, запускается ли скрипт напрямую. Если да, он может вызвать функцию `download_forms`, чтобы начать процесс загрузки. В настоящее время он закомментирован, то есть функция не будет выполняться, пока ее не раскомментируют.