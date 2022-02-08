from os import path
from glob import glob 
from multiprocessing import Lock, Manager, Pool, Process
import tabula
import pandas as pd
import pickle


COLUNAS = ["data", "local", "tipo_leito", "total", "ocupados", "percentual_ocupados"]
PATH = "C:\\Users\\jmess\\Documents\\Workspace\\Data_Science\\data\\dados_ocupacao\\"

def processar_dados_1(novos_dados, data, dados):
    try:
        dados.columns = dados.loc[0]
        dados.drop([0, len(dados.index) - 1], axis=0, inplace=True) 

        dados["local"] = dados.iloc[:,0] 

        for i in range(1, (len(dados.index) // 3) + 1):
            dados.iloc[(3 * (i - 1)) + 1: 3*i, 0] = dados.iloc[(3 * (i - 1)), 0]
            dados.iloc[(3 * (i - 1)), 4] = 'Estado'

        novos_dados.tipo_leito = dados.iloc[:,0]
        novos_dados.total = dados.iloc[:,1]
        novos_dados.ocupados = dados.iloc[:,2]
        novos_dados.percentual_ocupados = dados.iloc[:,3]
        novos_dados.local = dados.local
        novos_dados.data = data      
    except Exception:
        raise Exception
    return novos_dados


def save(l, path="list_files"):
    with open(path, "wb") as fp:
        pickle.dump(l, fp)


def load(path="list_files"):
    with open(path, "rb") as fp:
        b = pickle.load(fp)

    return b


def save_aux(locker, path, novos_dados):
    if locker is None:
        return

    locker.acquire()

    try:
        list_files = load()
        list_files[path] = novos_dados  
        save(list_files)
    finally:
        locker.release()

def processar_dados_2(novos_dados, data, dados):
    test = False
    if len(dados.columns) > 3 and dados.columns[3] == "Unnamed: 2":
        dados.drop([0, len(dados.index) - 1], axis=0, inplace=True)
        test = True
    elif pd.isna(dados.iloc[2,0]):
        dados.drop([0, 1, 2, len(dados.index) - 1], axis=0, inplace=True)
    else:
        dados.drop([0, 1, len(dados.index) - 1], axis=0, inplace=True)

    if test:
        total = dados.iloc[:,1]
        ocupados = dados.iloc[:,2]
        percetual_ocupados = dados.iloc[:,3]
    else:
        total = []
        ocupados = []
        percetual_ocupados = dados.iloc[:,2]
        for i in dados.iloc[:,1]:
            split_list = i.split(' ')
            total.append(int(split_list[0])) 
            ocupados.append(int(split_list[1]))
    
    novos_dados.tipo_leito = dados.iloc[:,0]
    novos_dados.total = total
    novos_dados.ocupados = ocupados
    novos_dados.percentual_ocupados = percetual_ocupados
    novos_dados.data = data
    
    return novos_dados

def mes_para_numero(month):

    return {
            'janeiro' : '01',
            'fevereiro' : '02',
            'março' : '03',
            'abril' : '04',
            'maio' : '05',
            'junho' : '06',
            'julho' : '07',
            'agosto' : '08',
            'setembro' : '09', 
            'outubro' : '10',
            'novembro' : '11',
            'dezembro' : '12'
    }[month.lower()]


def processa_data(path):
    try:
        split_path = path.split('\\')
        ano = split_path[3]
        nome = split_path[-1]
        
        split_nome = nome.split('-')

        dia = split_nome[1]
        mes = mes_para_numero(split_nome[3])
        return f'{dia}-{mes}-{ano}'

    except Exception:
        a = 1
        raise Exception

def processar_pdf(attr):
    locker = attr[0]
    path = attr[1]

    df = tabula.read_pdf(path, pages=1, multiple_tables=True, encoding='utf-8',
        area=[
            [21.20, 499.59, 30.87, 545.72],
            [29.38, 358.98, 191.58, 579.20],
            [40.16, 489.9, 60.81, 549.65],
            [57.5, 357.63, 179.39, 599.37],
            [28, 498.20, 40.55, 547.32],
            [39, 350.86, 202.03, 582.29],
            [28.64, 483.32, 42.78, 536.16],
            [40.55, 337.47, 191.61, 581.55],
            [19.71, 496.72, 30.13, 546.57],
            [30.13, 355.33, 158.13, 580.80],
            [18.97, 501.18, 30.88, 548.06],
            [30.88, 358.56, 193.10, 583.78],
            [29.39, 494.48, 42.04, 545.08],
            [39.06, 344.91, 206.50, 581.55],
            [19.71, 499.69, 32.37, 547.32],
            [29.39, 357.56, 201.29, 580.80],
            [23.44, 470.67, 33.11, 512.34],
            [29.39, 355.33, 158.87, 578.57],
            [31.23, 622.22, 43.13, 684.69],
            [41.15, 481.42, 246.41, 793.77],
            [1.86, 313.65, 147.71, 592.71],
            [56.99, 491.51, 75.35, 549.65],
            [73.05, 357.63, 200.04, 609.32]
        ])

    novos_dados = pd.DataFrame(columns=COLUNAS)
    
    if len(df) > 1 and df[1].columns[0] == 'POR TIPO DE LEITO' and ('/' in df[0].columns[0]):
        novos_dados = processar_dados_1(novos_dados, df[0].columns[0], df[1])
    elif len(df) > 2 and len(df[2].columns) > 1 and df[2].columns[1] == 'POR TIPO DE LEITO':
        novos_dados = processar_dados_2(novos_dados, df[1].columns[0], df[2])
    elif len(df) > 4 and df[4].columns[0] == 'POR TIPO DE LEITO':
        novos_dados = processar_dados_1(novos_dados, df[3].columns[0], df[4])
    elif len(df) > 5 and df[5].columns[0] == 'POR TIPO DE LEITO':
        novos_dados = processar_dados_1(novos_dados, df[3].columns[0], df[5])
    elif len(df) > 6 and df[6].columns[0] == 'POR TIPO DE LEITO' and ('/' in df[5].columns[0]):
        novos_dados = processar_dados_1(novos_dados, df[5].columns[0], df[6])
    elif len(df) > 13 and df[13].columns[0] == 'POR TIPO DE LEITO':
        novos_dados = processar_dados_1(novos_dados, df[12].columns[0], df[13])
    elif len(df) > 12 and len(df[12].columns) > 1 and df[12].columns[1] == 'POR TIPO DE LEITO':
        dados = df[12]
        dados.drop(dados.columns[0], axis=1, inplace=True)
        novos_dados = processar_dados_1(novos_dados, df[11].columns[0], dados)
    elif len(df) > 11 and df[11].columns[0] == 'POR TIPO DE LEITO' and ('/' in df[10].columns[0]):
        novos_dados = processar_dados_1(novos_dados, df[10].columns[0], df[11])
    elif len(df) > 16 and df[16].columns[0] == 'POR TIPO DE LEITO':
        novos_dados = processar_dados_1(novos_dados, df[15].columns[0], df[16])
    elif len(df) > 20 and df[20].columns[0] == 'POR TIPO DE LEITO':
        novos_dados = processar_dados_1(novos_dados, df[19].columns[0], df[20])
    elif len(df) > 10 and len(df[10].columns) > 1 and df[10].columns[1] == 'POR TIPO DE LEITO':
        dados = df[10]
        dados.drop([dados.columns[0], dados.columns[4]], axis=1, inplace=True)
        novos_dados = processar_dados_1(novos_dados, df[9].columns[0], dados)
    elif len(df) > 19 and len(df[19].columns) > 1 and df[19].columns[1] == 'POR TIPO DE LEITO':
        data = processa_data(path)
        novos_dados = processar_dados_2(novos_dados, data, df[19])
    elif len(df) > 17 and len(df[17].columns) > 1 and df[17].columns[1] == 'POR TIPO DE LEITO':
        dados = df[17]
        data = processa_data(path)
        novos_dados = processar_dados_2(novos_dados, data, df[17])
    elif len(df) > 14 and len(df[14].columns) > 1 and df[14].columns[1] == 'POR TIPO DE LEITO':
        novos_dados = processar_dados_2(novos_dados, df[13].columns[0], df[14])
    elif len(df) > 28  and df[19].columns[0] == 'POR TIPO DE LEITO' and type(df[20].columns[0]) == str:
        novos_dados = processar_dados_2(novos_dados, df[20].columns[0], df[19])
    else:
        # print(list(enumerate(df)))
        # print(len(df))
        print(f'Error: {path}')
        return novos_dados

    save_aux(locker, path, novos_dados)

    return novos_dados


def main():
    dados = pd.DataFrame(columns=COLUNAS)
    
    try:
        list_data = load()

        files_ready = list_data.keys()
        data_ready = list_data.values()

        print(len(files_ready))
        files = glob(path.join(path.relpath(PATH), "**", "*.pdf"), recursive=True)
        list_files = [x for x in files if x not in files_ready]

        for i in data_ready:
            dados = dados.append(i, ignore_index=True)
    except Exception:
        list_files = glob(path.join(path.relpath(PATH), "**", "*.pdf"), recursive=True)
        save({})
    
    with Manager() as manager:
        locker = manager.Lock()

        p = Pool(4)

        for i in p.map(processar_pdf, [(locker, x) for x in list_files]):
            dados = dados.append(i, ignore_index=True)

    dados.to_csv('dados.csv')


def test():
    resultado = processar_pdf((None, '../data/dados_ocupacao/2021/ocupacao_leitos_covid_abril_2021/Mapa-Diário-Ocupação-Leitos-COVID-19-02.03-16H.-3.pdf'))
    print(resultado)


if __name__ == '__main__':
    # test()
    main()
