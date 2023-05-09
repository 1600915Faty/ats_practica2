import random
import sys
import multiprocessing
import time
def read_file(filename):
    """Función auxiliar para leer el contenido de un archivo."""
    with open(filename, 'r', encoding='utf-8') as f:
        return f.readlines()

def map_function(lines):
    """Función Map que cuenta la frecuencia de las palabras en una lista de líneas."""
    list = []
    for line in lines:
        line = line.replace(',', ' ').replace('.', ' ')
        words = line.strip().lower().split()

        for word in words:
            w = {}
            w[word] = 1
            list.append(w)
    return list

def reduce_function(frequencies_list):
    """Función Reduce que fusiona varias frecuencias parciales en una sola."""
    freq = {}
    total = 0
    for block in frequencies_list:
        if block not in freq:
            freq[block] = 0
        freq[block] += len(frequencies_list[block])
        total += len(frequencies_list[block])
    return freq, total

def shuffle_function(mapped_list):
    """Función Shuffle que agrupa las frecuencias por palabra."""
    results = {}
    for block in mapped_list:
        for word in block:
            w = word.items()
            for name, value in w:
                if name not in results:
                    results[name] = []
                results[name].append(1)
    return results

def process_file(filename):
    """Función que ejecuta el Map-Reduce en un archivo de texto."""
    lines = read_file(filename)
    blocks = [lines[i:i+1000] for i in range(0, len(lines), 1000)]  # dividir el archivo en bloques de 1000 líneas
    with multiprocessing.Pool(processes=8) as pool:
        mapped = pool.map(map_function, blocks)

    shuffled = shuffle_function(mapped)
    with multiprocessing.Pool(processes=8) as pool:
        result, total = reduce_function(shuffled)
    return result, total

if __name__ == '__main__':

    # parsear los argumentos de línea de comandos
    num_processes = 8  # valor por defecto
    optional_mode = False  # valor por defecto
    filenames = ["words7.txt"]

    """
    for arg in sys.argv[1:]:
        if arg.startswith('-p'):
            num_processes = int(arg[2:])
        elif arg.startswith('-o'):
            optional_mode = True
        else:
            filenames.append(arg)
    """



    # ejecutar el Map-Reduce en cada archivo
    for filename in filenames:
        start = time.time()
        result, total = process_file(filename)
        final = time.time()

        # imprimir el resultado
        print(f'{filename}:')
        for word, count in sorted(result.items(), key=lambda x: x[1], reverse=True):
            percentage = 100.0 * count / total
            print(f'{word} : {percentage:.2f}%')
        print()
        print(final - start)
    """
    #input_files = sys.argv[1:]
    for i in noms_arxius:
        resultats = main(i, tam_bloc)
        print(i + ":")
        for resultat in resultats:
            print(resultat[0] + ": " + str(round(resultat[1], 2)) + "%")
    """
    """
    # Lista de palabras aleatorias en inglés
    words = ['car', 'apple', 'house', 'dog', 'pepa', 'coco']

    # Crear un archivo de texto y escribir las palabras aleatorias
    with open('words7.txt', 'w') as file:
        for i in range(3200000):  # se generan 5000 líneas de 20 palabras cada una
            line = ''
            for j in range(20):
                word = random.choice(words)
                line += word + ' '
            file.write(line.strip() + '\n')
    """
