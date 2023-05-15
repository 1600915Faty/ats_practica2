"""
AUTORS
LOANN COSANO ALCUDIA - 1600898
ISMAEL FATY TOLOSA - 1600915

"""
import random
import sys
import multiprocessing
import time
def read_file(filename):
    """Función auxiliar para leer el contenido de un archivo."""
    with open(filename, 'r', encoding='utf-8') as f:
        return f.readlines()
def read_file_optionals(filename):
    """Función auxiliar para leer el contenido de un archivo."""
    with open(filename, 'r', encoding='utf-8') as f:
        return f.read()


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
    for block in frequencies_list:
        if block not in freq:
            freq[block] = 0
        freq[block] += len(frequencies_list[block])
    return freq

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

"""Funció que retorna el numero de paraules/lletres totals que hi ha per fer el %"""
def countTotal(reduced):
    total=0
    for dicc in reduced:
        for key in dicc:
            total+=dicc[key]
    return total

"""Funcion para fusionar los resultados de los dos archivos"""
def files_fusion(results):
    final_dicctionarie={}
    for result in results:
        for dicc in result:
            for key in dicc:
                if key not in final_dicctionarie:
                    final_dicctionarie[key]=0
                final_dicctionarie[key]+=dicc[key]

    return [final_dicctionarie]
def process_file(filename, num_processes, optional_mode):
    """Función que ejecuta el Map-Reduce en un archivo de texto."""
    if optional_mode == 0:
        lines = read_file(filename)
        blocks = [lines[i:i + 2000] for i in range(0, len(lines), 2000)]  # dividir el archivo en bloques de 1000 líneas
    else:
        lines = read_file_optionals(filename)
        blocks = [lines[i:i + 800] for i in range(0, len(lines), 800)]  # dividir el archivo en bloques de 1000 líneas
    """ LOOP LINEAL PER FER COMPARACIONS EN EL INFORME
    mapped=[]
    for block in blocks:
        mapped.append(map_function(block))
    """
    with multiprocessing.Pool(processes=num_processes) as pool:
        mapped = pool.map(map_function, blocks)

    # Obtener los resultados de todas las tareas
    shuffled = shuffle_function(mapped)
    with multiprocessing.Pool(processes=num_processes) as pool:
        result = pool.map(reduce_function, [shuffled])

    """ LOOP LINEAL PER FER COMPARACIONS EN EL INFORME
    result = []
    for item in [shuffled]:
        results = reduce_function(item)
        result.append(results)
    """
    total = countTotal(result)
    return result, total

if __name__ == '__main__':

    # parsear los argumentos de línea de comandos
    num_processes = 12  # valor por defecto
    optional_mode = 0  # valor por defecto
    filenames = []

    #Leer los parametros
    for arg in sys.argv[1:]:
        if arg.startswith('-p'):
            num_processes = int(arg[3:])
        elif arg.startswith('-o'):
            optional_mode = int(arg[3:])
        else:
            filenames.append("/data/" + arg)

    #for optional_mode 0 and 1
    if optional_mode!=2:
        # ejecutar el Map-Reduce en cada archivo
        for filename in filenames:
            #start = time.time()
            result, total = process_file(filename, num_processes, optional_mode)
            #final = time.time()

            # imprimir el resultado
            print(f'{filename[6:]}:')
            for dicc in result:
                for word, count in sorted(dicc.items(), key=lambda x: x[1], reverse=True):
                    percentage = 100.0 * count / total
                    print(f'{word} : {percentage:.2f}%')
            print()
            #print(final - start)
            #print()
    #for optional mode 2, mix different files
    else:
        #start = time.time()
        results=[]
        totals = 0
        print('Files mixed:')

        for filename in filenames:
            print(f'{filename[6:]}')
            result, total = process_file(filename, num_processes, optional_mode)
            totals+=total
            results.append(result)
        final_result= files_fusion(results)
        #final = time.time()
        print('Results:')
        for dicc in final_result:
            for word, count in sorted(dicc.items(), key=lambda x: x[1], reverse=True):
                percentage = 100.0 * count / totals
                print(f'{word} : {percentage:.2f}%')
        print()
        #print(final - start)

    """
    # Lista de palabras aleatorias en inglés
    words = ['car', 'apple', 'house', 'dog', 'pepa', 'coco']

    # Crear un archivo de texto y escribir las palabras aleatorias
    with open('words7.txt', 'w') as file:
        for i in range(5200000):  # se generan 5000 líneas de 20 palabras cada una
            line = ''
            for j in range(20):
                word = random.choice(words)
                line += word + ' '
            file.write(line.strip() + '\n')
    """
