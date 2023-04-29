import sys
import os
from multiprocessing import Pool

def read_in_chunks(file_path, chunk_size=100):
    with open(file_path, 'r') as file:
        while True:
            lines = file.readlines(chunk_size)
            if not lines:
                break
            yield ' '.join(lines)

def mapper(file_path, chunk_size=100):
    word_count = {}
    for block in read_in_chunks(file_path, chunk_size):
        block = block.replace(',', ' ').replace('.', ' ')
        words = block.split()
        for word in words:
            word = word.lower()
            if word not in word_count:
                word_count[word] = 0
            word_count[word] += 1
    return word_count

def reducer(mapper_outputs):
    word_count = {}
    for mapper_output in mapper_outputs:
        for word, count in mapper_output.items():
            if word not in word_count:
                word_count[word] = 0
            word_count[word] += count
    return word_count

if __name__ == '__main__':
    # Obtener los argumentos de entrada
    input_files = ["words2.txt"]
    num_processes = 8  # Establecer el número de procesos

    # Lanzar múltiples procesos utilizando la biblioteca multiprocessing
    with Pool(num_processes) as p:
        # Llamar al método mapper para cada archivo de entrada
        mapper_outputs = p.starmap(mapper, [(f, 100) for f in input_files])

    # Reducir la salida del mapper
    word_count = reducer(mapper_outputs)

    # Imprimir los resultados
    for input_file, file_word_count in zip(input_files, mapper_outputs):
        print(f"{input_file}:")
        for word, count in file_word_count.items():
            frequency = (count / sum(file_word_count.values())) * 100
            print(f"{word} : {frequency:.2f}%")
        print()

    """"
    #input_files = sys.argv[1:]
    for i in noms_arxius:
        resultats = main(i, tam_bloc)
        print(i + ":")
        for resultat in resultats:
            print(resultat[0] + ": " + str(round(resultat[1], 2)) + "%")
    """
    """
    # Lista de palabras aleatorias en inglés
    words = ['car', 'apple', 'house', 'dog']

    # Crear un archivo de texto y escribir las palabras aleatorias
    with open('words2.txt', 'w') as file:
        for i in range(50000000):  # se generan 5000 líneas de 20 palabras cada una
            line = ''
            for j in range(20):
                word = random.choice(words)
                line += word + ' '
            file.write(line.strip() + '\n')
            """
