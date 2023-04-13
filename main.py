# This is a sample Python script.

import re
import threading


class MapReduce:

    def __init__(self, num_threads=4, block_size=10000):
        self.num_threads = num_threads
        self.block_size = block_size
        self.maps = []
        self.lock = threading.Lock()

    def map(self, block):
        # Función Map para cada bloque de datos
        word_count = {}
        for line in block:
            for word in re.findall(r'\w+', line):
                word = word.lower()
                word_count[word] = word_count.get(word, 0) + 1
        with self.lock:
            self.maps.append(word_count)

    def reduce(self):
        # Función Reduce para combinar los resultados de cada bloque
        result = {}
        for word_count in self.maps:
            for word, count in word_count.items():
                result[word] = result.get(word, 0) + count
        return result

    def process_files(self, files):
        # Función principal para procesar los archivos de texto
        results = {}
        for file in files:
            blocks = []
            with open(file, 'r') as f:
                lines = f.readlines()
                for i in range(0, len(lines), self.block_size):
                    blocks = lines[i:i + self.block_size]
                    #blocks.append(block)

            self.maps = []
            threads = []
            for i in range(self.num_threads):
                threads.append(threading.Thread(target=self.map, args=(blocks[i::self.num_threads],)))
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()

            results[file] = self.reduce()

        return results


def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print_hi('PyCharm')
    mr = MapReduce()
    results = mr.process_files(['ArcTecSw_2023_BigData_Practica_Part1_Sample'])
    for file, result in results.items():
        print(f"File {file}: {result}")

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
