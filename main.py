import sys
import os

def read_in_chunks(file_path, chunk_size=1024):
    with open(file_path, 'r') as file:
        while True:
            data = file.read(chunk_size)
            if not data:
                break
            yield data

def mapper(file_path):
    word_count = {}
    for block in read_in_chunks(file_path):
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
    #input_files = sys.argv[1:]
    input_files = ["ArcTecSw_2023_BigData_Practica_Part1_Sample"]
    mapper_outputs = []
    for input_file in input_files:
        mapper_output = mapper(input_file)
        mapper_outputs.append(mapper_output)
    word_count = reducer(mapper_outputs)
    for input_file, file_word_count in zip(input_files, mapper_outputs):
        print(f"{input_file}:")
        for word, count in file_word_count.items():
            frequency = (count / sum(file_word_count.values())) * 100
            print(f"{word} : {frequency:.2f}%")
        print()
    """"
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
# See PyCharm help at https://www.jetbrains.com/help/pycharm/
