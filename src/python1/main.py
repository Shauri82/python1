import os
import sys

import requests

from python1.decorators import decorator
from python1.exceptions import NotFoundError, AccessDeniedError

# domyślny katalog na tworzone pliki
DOWNLOAD_DIR = "downloads"

def download_file(url, filename=None):
    """
        Pobiera plik z podanego adresu URL i zapisuje go lokalnie.

        Args:
            url : Adres URL pliku do pobrania.
            filename (opcjonalny): Nazwa pliku do zapisania. Jeśli nie podano, zostanie użyta "latest.csv".

        Raises:
            NotFoundError: Gdy serwer zwraca kod 404 (plik nie znaleziony).
            AccessDeniedError: Gdy serwer zwraca kod 403 (brak dostępu).
            IOError: W przypadku błędów zapisu pliku na dysku.
    """
    try:
        response = requests.get(url, stream=True)

        if response.status_code == 200:
            if filename is None:
                filename = "latest.csv"

            file_path = generate_file_path(filename)

            with open(file_path, 'wb') as file:
                file.write(response.content)

        elif response.status_code == 404:
            raise NotFoundError()
        elif response.status_code == 403:
            raise AccessDeniedError()

    except NotFoundError as e:
        print(f"Błąd podczas pobierania pliku: {e}")
    except AccessDeniedError as e:
        print(f"Brak dostepu do plikuu: {e}")
    except IOError as e:
        print(f"Błąd podczas zapisu pliku: {e}")
    else:
        print("Plik został pobrany")

def generate_file_path(filename):
    """
        Tworzy pełną ścieżkę do pliku w katalogu `downloads`.

        Args:
            filename : Nazwa pliku.

        Returns:
            str: Pełna ścieżka do pliku.
    """
    if not os.path.exists(DOWNLOAD_DIR):
        os.makedirs(DOWNLOAD_DIR)
    return os.path.join(DOWNLOAD_DIR, filename)


class SimpleETL:
    def __init__(self, url, filename):
        self.url = url
        self.filename = filename

    def __read_file(self):
        """
           Generator wczytujący plik linia po linii.

           Yields:
               str: Kolejna linia pliku (bez znaku końca linii).
        """
        with open(generate_file_path(self.filename), 'r') as csv_file:
            for line in csv_file:
                yield line.strip()

    @decorator
    def download_and_process_file(self):
        """
            Pobiera plik CSV z sieci, przetwarza go i zapisuje wyniki do dwóch plików:
              - `values.csv`: zawiera ID, sumę i średnią dla każdej linii
              - `missing_values.csv`: zawiera ID i indeksy brakujących danych
        """
        download_file(url=self.url, filename=self.filename)
        if os.path.isfile(generate_file_path(self.filename)):
            with (open(generate_file_path('values.csv'), 'a', encoding='utf-8') as f,
                  open(generate_file_path('missing_values.csv'), 'a', encoding='utf-8') as f2):

                for item in self.__process_file():

                    wynik = ','.join(str(x) for x in item[0])
                    f.write(wynik)
                    f.write('\n')

                    wynik2 = ','.join(str(x) for x in item[1])
                    f2.write(wynik2)
                    f2.write('\n')
            print("Plik przetworzony poprawnie")

    def __process_file(self):
        """
            Przetwarza plik CSV i oblicza sumy i średnie.
            Dodatkowo identyfikuje numeyr kolumn z brakującymi wartościami (oznaczone jako '-').

            Yields:
                - line_result : [id, suma, średnia] lista z poprawnie przetworzonymi danymi
                - missing_result : [id, indeksy brakujących wartości] lista brakujących wartości w pliku
        """
        for data in self.__read_file():
            split_line = data.split(",")
            existing_data = [float(split_line[n]) for n in range(1, len(split_line)) if split_line[n] != '-']
            missing = [n for n in range(1, len(split_line)) if split_line[n] == '-']
            suma = sum(existing_data)
            if(len(existing_data)>0):
                avg = suma / len(existing_data)
            else:
                avg = 0
            line_result = [int(split_line[0]), suma, avg]
            missing_result = [int(split_line[0])]
            missing_result.extend(missing)
            yield line_result, missing_result


etl = SimpleETL("https://...", "dane.csv")
etl.download_and_process_file()



