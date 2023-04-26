# This is a sample Python script.

import threading

# Dividir els arxius en blocs
def splitting(nom_arxiu, tam_bloc):
    blocs = []
    total_words = 0
    with open(nom_arxiu, 'r') as f:
        lines = f.readlines()
        for i in range(0, len(lines), tam_bloc):
            blocs = lines[i:i + tam_bloc]
            total_words += sum(len(line.split()) for line in blocs)

    return blocs, total_words

# Funció de map
def map_func(bloc):
    bloc_sense_comas = bloc.replace(',', ' ').replace('.', ' ')
    paraules = [paraula.lower() for paraula in bloc_sense_comas.split()]
    parells_clau_valor = [(paraula, 1) for paraula in paraules]
    return parells_clau_valor

# Funció de reduce
def reduce_func(parell_clau_valor, num_caracters):
    paraula, freq = parell_clau_valor[0], sum(parell_clau_valor[1])/num_caracters*100
    return (paraula, freq)

# Shuffle
def shuffle(parells_clau_valor):
    # Función Shuffle para unir todas las palabras y mostrar cuántas hay sin hacer la suma
    parells_clau_valor_agrupats = {}
    for tupla in parells_clau_valor:
        for paraula, freq in tupla:
            if paraula not in parells_clau_valor_agrupats:
                parells_clau_valor_agrupats[paraula] = []
            parells_clau_valor_agrupats[paraula].append(freq)
    return parells_clau_valor_agrupats
def main(nom_arxiu, tam_bloc):
    blocs = []
    total_words = 0
    # Dividir els arxius en blocs
    #for nom_arxiu in noms_arxius:
     #   blocs, total_words = splitting(nom_arxiu, tam_bloc)
    blocs, total_words = splitting(nom_arxiu, tam_bloc)
    # Crear threads per a la funció de map
    parells_clau_valor = []
    threads_map = []
    for bloc in blocs:
        t = threading.Thread(target=lambda q, arg1: q.append(map_func(arg1)), args=(parells_clau_valor, bloc))
        t.start()
        threads_map.append(t)
        for t in threads_map:
            t.join()
    # Crear threads per a la funció de shuffle
    parells_clau_valor_agrupats = shuffle(parells_clau_valor)
    # Executar la funció de reduce
    parells_clau_valor_reduits = []
    for paraula, freqs in parells_clau_valor_agrupats.items():
        parells_clau_valor_reduits.append(reduce_func((paraula, freqs), total_words))
    return parells_clau_valor_reduits
if __name__ == '__main__':
    noms_arxius = ["ArcTecSw_2023_BigData_Practica_Part1_Sample"]
    tam_bloc = 1000
        
    for i in noms_arxius:
        resultats = main(i, tam_bloc)
        print(i + ":")
        for resultat in resultats:

            print(resultat[0] + ": " + str(round(resultat[1],2)) + "%")

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
