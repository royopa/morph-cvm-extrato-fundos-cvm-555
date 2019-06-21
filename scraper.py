
# -*- coding: utf-8 -*-
import os
import datetime
import scraperwiki
import pandas as pd


def main():
    # morph.io requires this db filename, but scraperwiki doesn't nicely
    # expose a way to alter this. So we'll fiddle our environment ourselves
    # before our pipeline modules load.
    os.environ['SCRAPERWIKI_DATABASE_NAME'] = 'sqlite:///data.sqlite'

    today = datetime.date.today()
    ano_inicial = 2015
    ano_final = int(today.strftime('%Y'))

    for ano in range(ano_inicial, ano_final+1):
        processa_arquivo(ano)

    return True


def processa_arquivo(ano):
    url_base = 'http://dados.cvm.gov.br/dados/FI/DOC/EXTRATO/DADOS/extrato_fi_{}.csv'
    url = url_base.format(ano)
    print(url)

    file_name = url.split('/')[-1]

    try:
        df = pd.read_csv(
            url,
            sep=';',
            encoding='latin1',
            low_memory=True
        )
    except Exception:
        print('Erro ao baixar arquivo', url)
        return False

    print(df.dtypes)

    # converte as colunas
    df['OPER_DERIV'] = df['OPER_DERIV'].astype(str)
    df['FINALIDADE_OPER_DERIV'] = df['FINALIDADE_OPER_DERIV'].astype(str)
    df['OPER_VL_SUPERIOR_PL'] = df['OPER_VL_SUPERIOR_PL'].astype(str)
    df['CONTRAP_LIGADO'] = df['CONTRAP_LIGADO'].astype(str)
    df['INVEST_EXTERIOR'] = df['INVEST_EXTERIOR'].astype(str)
    df['ATIVO_CRED_PRIV'] = df['ATIVO_CRED_PRIV'].astype(str)
    df['TAXA_PERFM'] = df['TAXA_PERFM'].astype(str)
    df['PARAM_TAXA_PERFM'] = df['PARAM_TAXA_PERFM'].astype(str)
    df['EXISTE_TAXA_PERFM'] = df['EXISTE_TAXA_PERFM'].astype(str)
    df['INF_TAXA_PERFM'] = df['INF_TAXA_PERFM'].astype(str)
    df['EXISTE_TAXA_INGRESSO'] = df['EXISTE_TAXA_INGRESSO'].astype(str)
    df['EXISTE_TAXA_SAIDA'] = df['EXISTE_TAXA_SAIDA'].astype(str)
    df['FINALIDADE_OPER_DERIV'] = df['FINALIDADE_OPER_DERIV'].astype(str)
    df['OPER_VL_SUPERIOR_PL'] = df['OPER_VL_SUPERIOR_PL'].astype(str)
    df['CALC_TAXA_PERFM'] = df['CALC_TAXA_PERFM'].astype(str)
    df['TAXA_INGRESSO_REAL'] = df['TAXA_INGRESSO_REAL'].astype(str)
    df['MERCADO'] = df['MERCADO'].astype(str)
    df['TP_PRAZO'] = df['TP_PRAZO'].astype(str)
    df['PRAZO'] = df['PRAZO'].astype(str)
    df['PUBLICO_ALVO'] = df['PUBLICO_ALVO'].astype(str)
    df['REG_ANBIMA'] = df['REG_ANBIMA'].astype(str)
    df['CLASSE_ANBIMA'] = df['CLASSE_ANBIMA'].astype(str)
    df['DISTRIB'] = df['DISTRIB'].astype(str)
    df['POLIT_INVEST'] = df['POLIT_INVEST'].astype(str)
    #df[''] = df[''].astype(str)

    #return True

    # insere o nome do arquivo
    df['NO_ARQUIVO'] = file_name

    # transforma o campo CO_PRD
    df['CO_PRD'] = df['CNPJ_FUNDO'].str.replace('.','')
    df['CO_PRD'] = df['CO_PRD'].str.replace('/','')
    df['CO_PRD'] = df['CO_PRD'].str.replace('-','')
    df['CO_PRD'] = df['CO_PRD'].str.zfill(14)

    df['DT_COMPTC'] = pd.to_datetime(df['DT_COMPTC'], errors='coerce').dt.strftime('%Y-%m-%d')
    df['DT_REF'] = df['DT_COMPTC']

    for row in df.to_dict('records'):
        scraperwiki.sqlite.save(unique_keys=['CO_PRD', 'DT_REF', 'NO_ARQUIVO'], data=row)

    # rename file
    time.sleep(5)
    print('Renomeando arquivo sqlite')
    os.rename('scraperwiki.sqlite', 'data.sqlite')

    print('{} Registros importados com sucesso', len(df))
    return True


if __name__ == '__main__':
    main()