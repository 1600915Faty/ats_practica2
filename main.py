import sys
import multiprocessing as mp

def file_chunks(filename, chunk_size=1024*1024):
    """Genera bloques de líneas del archivo."""
    with open(filename, 'r', encoding='utf-8') as f:
        while True:
            lines = f.readlines(chunk_size)
            if not lines:
                break
            yield lines

def map_func(lines):
    """Cuenta la frecuencia de cada palabra en un bloque de líneas."""
    freq = {}
    total = 0
    for line in lines:
        line = line.replace(',', ' ').replace('.', ' ')
        words = line.strip().lower().split()

        for word in words:
            freq[word] = freq.get(word, 0) + 1
            total += 1
    return freq
    #return [(word, count/total*100) for word, count in freq.items()]

def reduce_func(results):
    """Combina los resultados de varios bloques de líneas."""
    freq = {}
    total = 0
    for block in results:
        for name,num in block.items():
            freq[name] = freq.get(name, 0) + num
            total += num

    return [(word, freq[word]/total*100) for word in sorted(freq.keys())]

def run_mapreduce(filenames, num_processes):
    # Crea un objeto Pool con el número de procesos especificado
    pool = mp.Pool(num_processes)

    # Crea una lista de tareas
    tasks = [(filename, chunk) for filename in filenames for chunk in file_chunks(filename)]

    # Ejecuta las tareas en paralelo
    results = []
    for task in tasks:
        filename, chunk = task
        result = map_func(chunk)
        results.append((filename, result))

    # Agrupa los resultados por archivo y combina los resultados de cada archivo
    file_results = {}
    for filename, result in results:
        if filename not in file_results:
            file_results[filename] = []
        file_results[filename].append(result)
    final_results = {}
    for filename, results in file_results.items():
        final_results[filename] = reduce_func(results)

    # Muestra los resultados por pantalla
    for filename in filenames:
        print(f"{filename}:")
        for word, freq in final_results[filename]:
            print(f"{word} : {freq:.2f}%")

if __name__ == '__main__':
    # Parsea los argumentos de la línea de comandos
    #filenames = sys.argv[1:-1]
    #num_processes = int(sys.argv[-1])

    # Ejecuta el MapReduce
    run_mapreduce(["words2.txt"], 8)
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
