import json
import pandas as pd
import pickle
import boto3
import datetime
from io import BytesIO

s3_client = boto3.client('s3')

#################################
#### Funciones preeliminares ####
#################################

# Lectura desde S3


def read_tbl_csv(file):
    bucket_name = 'portalvendedor'
    s3_file_name = file
    resp = s3_client.get_object(Bucket=bucket_name, Key=s3_file_name)
    return pd.read_csv(BytesIO(resp['Body'].read()), sep=';')


def read_tbl_xlsx(file):
    bucket_name = 'portalvendedor'
    s3_file_name = file
    resp = s3_client.get_object(Bucket=bucket_name, Key=s3_file_name)
    return pd.read_excel(BytesIO(resp['Body'].read()))

###########################
#### Función principal ####
###########################


def lambda_handler(event, context):
    # Fecha de hoy, NOTAR QUE EL FORMATO DE MOMENTO ES DD-MM-YYYY
    hoy = (datetime.datetime.today()).strftime('%Y-%m-%d')
    trad_centro = {'CF6', 'CM6', 'CP6', 'CA3', 'CR3', 'CI1', 'CI2', 'CL5'}
    ficticias = {'D', 'R', 'S'}

# Lectura data de clientes + filtros y tratamiento data
    maestra_clientes = read_tbl_csv('xtract_clientes/portal_vend_clientes.csv')
    maestra_clientes = maestra_clientes[maestra_clientes["EV_CLIENT-SPART"]
                                        == 'Z0'].reset_index(drop=True)
    maestra_clientes['EV_CLIENT-AUFSD'] = maestra_clientes['EV_CLIENT-AUFSD'].fillna(
        '')
    maestra_clientes.dropna(subset=['EV_CLIENT-SROUTE'], inplace=True)
    maestra_clientes = maestra_clientes[(
        maestra_clientes["EV_CLIENT-AUFSD"] == '')].reset_index(drop=True)
    maestra_clientes['valid_route'] = maestra_clientes['EV_CLIENT-SROUTE'].apply(
        lambda x: x[0:3] in trad_centro)
    maestra_clientes = maestra_clientes[maestra_clientes['valid_route']].reset_index(
        drop=True)

# Añadir georreferencias y plan visitas
    georreferencias = read_tbl_xlsx('xtract_clientes/georreferencias.xlsx')
    georreferencias.rename(
        columns={'CLIENTES': 'EV_CLIENT-ERPCLIENTID'}, inplace=True)
    maestra_clientes = maestra_clientes.merge(
        georreferencias, how='left', on='EV_CLIENT-ERPCLIENTID')

    plan_visitas = read_tbl_csv(
        'xtract_plan_visita/portal_vend_plan_visita.csv')
    print(plan_visitas['EV_VISITPLAN-EXDAT'].iloc[32072] == hoy, type(
        plan_visitas['EV_VISITPLAN-EXDAT'].iloc[32072]), plan_visitas['EV_VISITPLAN-EXDAT'].iloc[32072], hoy)
    plan_visitas = plan_visitas[(plan_visitas['EV_VISITPLAN-LAND1'] == 'CL') & (plan_visitas['EV_VISITPLAN-VPTYPT'] == 'salesman') & (
        plan_visitas['EV_VISITPLAN-EXDAT'] == hoy)][['EV_VISITPLAN-KUNNR', 'EV_VISITPLAN-ROUTE', 'EV_VISITPLAN-EXDAT']]
    plan_visitas.rename(columns={'EV_VISITPLAN-KUNNR': 'EV_CLIENT-ERPCLIENTID',
                        'EV_VISITPLAN-ROUTE': 'EV_CLIENT-SROUTE', 'EV_VISITPLAN-EXDAT': 'EXDAT'}, inplace=True)

    plan_visitas['EV_CLIENT-SROUTE'] = plan_visitas['EV_CLIENT-SROUTE'].apply(
        lambda x: x.strip())
    plan_visitas['trad'] = plan_visitas['EV_CLIENT-SROUTE'].apply(
        lambda x: x[0:3] in trad_centro)
    plan_visitas = plan_visitas[plan_visitas['trad']].reset_index(drop=True)
    #plan_visitas['ficticia'] = plan_visitas['EV_CLIENT-SROUTE'].apply(lambda x: x[0] in ficticias)
    print(len(plan_visitas))

    #maestra_ficticias = plan_visitas.merge(maestra_clientes[[x for x in maestra_clientes.columns if x != 'EV_VISITPLAN-ROUTE']],how='left',on=['EV_CLIENT-ERPCLIENTID'])

    #maestra_clientes = maestra_clientes.append(maestra_ficticias).reset_index(drop=True)
    if len(plan_visitas) != 0:
        plan_visitas = plan_visitas[[
            'EV_CLIENT-SROUTE', 'EV_CLIENT-ERPCLIENTID', 'EXDAT']]
        maestra_clientes = maestra_clientes.merge(plan_visitas, how='left', on=[
                                                  'EV_CLIENT-SROUTE', 'EV_CLIENT-ERPCLIENTID'])
    else:
        maestra_clientes['EXDAT'] = ''
    maestra_clientes.EXDAT = maestra_clientes.EXDAT.fillna('')
    maestra_clientes.EXDAT = maestra_clientes.EXDAT.apply(
        lambda x: str((len(x) > 0)*1))
    maestra_clientes['stras'] = maestra_clientes[['EV_CLIENT-STREET', 'EV_CLIENT-DOORNUMBER']
                                                 ].apply(lambda x: str(x['EV_CLIENT-STREET'])+' '+str(x['EV_CLIENT-DOORNUMBER']), axis=1)

    print(maestra_clientes['EV_CLIENT-ERDAT'].iloc[0])
    maestra_clientes['EV_CLIENT-ERDAT'] = maestra_clientes['EV_CLIENT-ERDAT'].apply(lambda x: 1*(datetime.datetime(int(str(
        x).split('-')[0]), int(str(x).split('-')[1]), int(str(x).split('-')[2])) >= datetime.datetime.today()-datetime.timedelta(45)))
    maestra_clientes['EV_CLIENT-ERDAT'] = maestra_clientes['EV_CLIENT-ERDAT'].apply(
        lambda x: str(x).replace('1', 'cliente nuevo'))

# Formalización campos a usar
    cols = ['EV_CLIENT-SROUTE', 'EV_CLIENT-ERPCLIENTID', 'EV_CLIENT-VKGRP_TXT', 'EV_CLIENT-VKBUR_TXT', 'EV_CLIENT-OPCHANNEL', 'EV_CLIENT-KATR7_TXT', 'EV_CLIENT-LQLNUM',
            'EV_CLIENT-ERDAT', 'EV_CLIENT-MCOD3', 'EV_CLIENT-STREET', 'EV_CLIENT-DOORNUMBER', 'EV_CLIENT-FISCALID', 'EV_CLIENT-FISCALNAME', 'EV_CLIENT-TELF1', 'LAT', 'LONG', 'EXDAT']
    cols_txt = {'EV_CLIENT-SROUTE': 'andina_sroute', 'EV_CLIENT-ERPCLIENTID': 'kunnr', 'EV_CLIENT-VKGRP_TXT': 'grupo_venta', 'EV_CLIENT-VKBUR_TXT': 'oficina_ventas',
                'EV_CLIENT-OPCHANNEL': 'opchannel', 'EV_CLIENT-KATR7_TXT': 'subtrd', 'EV_CLIENT-LQLNUM': 'lqlnum', 'EV_CLIENT-ERDAT': 'erdat', 'EV_CLIENT-MCOD3': 'mcod3',
                'EV_CLIENT-FISCALID': 'rut', 'EV_CLIENT-FISCALNAME': 'txtmd', 'EV_CLIENT-TELF1': 'telf1', 'LAT': 'lat', 'LONG': 'long', 'EXDAT': 'exdat'}

    maestra_clientes.rename(columns=cols_txt, inplace=True)
    maestra_clientes['kunnr'] = maestra_clientes['kunnr'].apply(
        lambda x: str(int(x)))
    maestra_clientes['opchannel'] = maestra_clientes['opchannel'].apply(
        lambda x: str(int(x)))
    maestra_clientes['lat'] = maestra_clientes['lat'].apply(lambda x: str(x))
    maestra_clientes['long'] = maestra_clientes['long'].apply(lambda x: str(x))
    maestra_clientes['text1'] = ''
    maestra_clientes['plan'] = ''
    maestra_clientes['zterm'] = ''
    maestra_clientes = maestra_clientes[['andina_sroute', 'kunnr', 'grupo_venta', 'oficina_ventas', 'opchannel',
                                         'subtrd', 'lqlnum', 'erdat', 'mcod3', 'rut', 'txtmd', 'telf1', 'lat', 'long', 'plan', 'exdat', 'text1', 'zterm']]
    maestra_clientes.fillna('', inplace=True)
    print(maestra_clientes.andina_sroute.unique())

# Creación objetos, ruta_clientes objeto principal a utilizar, ruta_grupo objeto pivote
    ruta_clientes = {}
    ruta_grupo = {}
    for i in range(len(maestra_clientes)):
        ruta_clientes.setdefault(maestra_clientes['andina_sroute'][i], []).append(
            {col: maestra_clientes[col][i] for col in maestra_clientes.columns[1:]})
        ruta_grupo[maestra_clientes['andina_sroute']
                   [i]] = maestra_clientes['grupo_venta'][i]

    obj_ruta_grupo = pickle.dumps(ruta_grupo)
    s3_client.put_object(Bucket='portalvendedor',
                         Key='pickles/pivote_grupo_vendedor/ruta_grupo.pickle', Body=obj_ruta_grupo)

    for grupo in list(set(maestra_clientes['grupo_venta'])):
        if len(grupo) == 0:
            continue
        obj_ruta_clientes = pickle.dumps(
            {ruta: ruta_clientes[ruta] for ruta in ruta_clientes if ruta_grupo[ruta] == grupo})
        s3_client.put_object(Bucket='portalvendedor', Key='pickles/' +
                             grupo+'/'+grupo+'ruta_clientes.pickle', Body=obj_ruta_clientes)

    return {
        'statusCode': 200,
        'body': json.dumps('Job done!')
    }
