#Funciones
import re, urllib, urllib3, json, datetime, psycopg2, sys, traceback, smtplib, boto3
import pandas as pd
from modules.constants import *
from email.message import EmailMessage
from boto3.s3.transfer import S3Transfer, TransferConfig

#*****************************************************************************************************************************************************************************************************************************
#Se define la función llamada SendMail que toma dos argumentos: text y mail_subject.
#La función tiene como objetivo enviar un correo electrónico con el contenido
# y el asunto proporcionados a una dirección de correo electrónico especificada.
def SendMail(text, mail_subject):
    msg = EmailMessage() #crea una nueva instancia de la clase EmailMessage() y la asigna a la variable msg. Esta clase se utiliza para crear y enviar mensajes de correo electrónico.
    msg.set_content(text) #se establece el contenido del correo electrónico como el valor del argumento text que se pasó a la función.
    msg['Subject'] = mail_subject #establece el asunto del correo electrónico como el valor del argumento mail_subject que se pasó a la función.
    msg['From'] = 'alarmas-aws@vcmedios.com.co' #Establece la dirección de correo electrónico del remitente como alarmas-aws@vcmedios.com.co.
    msg['To'] = Mail_To #Establece la dirección de correo electrónico del destinatario como el valor de una constante llamada Mail_To.
    conexion = smtplib.SMTP(host='10.10.130.217', port=25) #Se establece una conexión con un servidor SMTP que se encuentra en la dirección IP 10.10.122.17 y el puerto 25. Esta conexión se asigna a la variable conexion.
    conexion.ehlo() #Se utiliza el comando EHLO (abreviatura de "Hola extendido") para iniciar una conexión con el servidor SMTP.
    conexion.send_message(msg) #se envía el mensaje de correo electrónico utilizando la conexión establecida anteriormente.
    conexion.quit() #se cierra la conexión SMTP.
    return #devuelve none.
#*****************************************************************************************************************************************************************************************************************************

#****************************************************************************************************************************************************************
#Se define la función llamada Download_Logs, que toma un argumento llamado DATE_LOG. 
#La función tiene como objetivo descargar todos los archivos de registro de un Bucket 
# de Amazon S3 y guardarlos en una ruta local.
def Download_Logs(DATE_LOG):
    try:
        objects={'Objects':[]} #Se inicializa la variable como un diccionario vacio.
        list_objects=[] #Se inicializa la variable como una lista vacia.
        aws_session=boto3.Session(profile_name=aws_profile) #Se crea una sesion de AWS bajo el perfil definido en la constante aws_profile.
        s3_client=aws_session.client('s3') #Se establece conexion con el servicio S3.
        logs=s3_client.list_objects_v2(Bucket=Bucket_logs, MaxKeys=10000) #se listan todos los objetos (archivos) en el Bucket definido en la constante Bucket_logs de S3.
        if 'Contents' in logs: #Se comprueba si hay objetos en el bucket.   
            for i in range(len(logs['Contents'])): #Si hay objetos se itera sobre cada objeto.
                log_Key=logs['Contents'][i]['Key'] #Captura de la ruta del objeto.
                S3Transfer(s3_client, TransferConfig(max_bandwidth=5000000)).download_file(Bucket_logs,log_Key,f'{Downloads_Path}/{log_Key}')
                objects['Objects'].append({'Key': log_Key,}) #Se agrega al dicionario el nombre del objeto descargado.
                list_objects.append(f"{Downloads_Path}/{log_Key}") #Se agrega la ruta completa del objeto descargado en el folder local.
                #Se realiza un a copia del objeto en el bucket definido en la constante Bucket_logs_old.
                s3_client.copy_object( 
                    Bucket=Bucket_logs_old, #Se establece el bucket de destino.
                    CopySource=f'{Bucket_logs}/{log_Key}', #Se establece la ruta del objeto en el bucket de origen.
                    Key=f'{log_Key}' #Se establece el nombre del objeto en el destino.
                    )
                #Se eliminan los objetos en el bucket de origen.    
            s3_client.delete_objects(
                    Bucket=Bucket_logs, #Se establce el bucket.
                    Delete=objects #Se establce la lista de objetos a eliminar.
                )
            return list_objects #Retorna la lista de objetos cada uno con su ruta en el folder local.
        else:
            text_print=f"Logs not found" #Se define el texto a registrar en el log de eventos.
            print_log(text_print,DATE_LOG) #Se registra el texto en el log de eventos.
            return [] #Retorna lista vacia.
    except:
        error=sys.exc_info()[2] #Captura del error generado por el sistema.
        errorinfo=traceback.format_tb(error)[0] #Cartura del detalle del error.
        return {
            'Error': str(sys.exc_info()[1]),
            'error_info': errorinfo
        }
#*****************************************************************************************************************************************************************

#****************************************************************************************************************
#Se define la funcion llamada Flag_status, que toma un argumento llamado OPTION.
#La funcion tiene como objetivo leer y escribir un archivo json.
def Flag_Status(OPTION):
    with open(json_path, "r") as json_file: #Se abre el archivo json especificado en la constante json_path.
            json_data=json.load(json_file) #Se lee el contenido del archivo.
    if OPTION=="r": #Comprueba si el argumento OPTION es igual al caracter 'r'.
        return json_data["FLAG"] #Retorna el value de la llave "FLAG".
    elif OPTION=="w": #Comprueba si el argumento OPTION es igual al caracter 'w'.
        json_data["FLAG"]=False #Se establece el value de la llave "FLAG" en false.
        with open(json_path, "w") as json_file: #Se abre el archivo json.
            json.dump(json_data, json_file) #Se escribe en el archivo json.
    else:
        pass
#***************************************************************************************************************

#*********************************************************************************************************************************************************************************
#Se define la funcion llamada print_log, que toma dos argumentos llamados TEXT, DATE_LOG.
#La funcion tiene como objetivo registrar en un archivo txt un texto definido en la variable TEXT.
def print_log(TEXT, DATE_LOG):
    log_file=open(f"{log_Path}/{DATE_LOG}_log.txt", "a") #Se abre el archivo de la ruta especificada. Si el archivo no existe este se crea. 
    log_file.write(f"{str(datetime.datetime.strftime(datetime.datetime.now(), '%H:%M:%S'))}\t{TEXT}\n") #Se escribe en el archivo log el texo especificado en el argumento TEXT.
    log_file.close() #Se cierra el archivo.
#**********************************************************************************************************************************************************************************

#Funcion que transforma los datos
def Uri_Transform(x):
    uri_split=x.split('/')
    if uri_split[1] in dict_mso:
        mso_uri=dict_mso[uri_split[1]].split('_')
        mso_name=mso_uri[1]
        country=mso_uri[-1]
    else:
        mso_uri=uri_split[1].split('_')
        if len(mso_uri)>3:
            mso_name='_'.join(mso_uri[1:-1])
        else:
            mso_name=mso_uri[1]
        country=mso_uri[-1]
    uri_id=uri_split[4]

    return [mso_name, country, uri_id]#f"{mso_name}/{country}"

def mso_country_extract(x):
    df=pd.DataFrame()
    for row in x:
        a=row.split('/')
        df=df.append({
            'valor1': a[0],
            'valor2': a[1]
        })
    return df

def request_id_transform(x):
    request_id=re.search(r"(.{25}).*", x).groups()[0]
    return request_id

def Manifest_Query_Transform(x):
    params = urllib.parse.parse_qs(x)
    if 'idp_user_id' in params:
        client_id=params['idp_user_id'][0]
    else:
        client_id='No Id'
    return client_id

def Segments_Query_Transform(x):
    params = urllib.parse.parse_qs(x)
    if len(params['dv']) == 1:
        dv_uri=params['dv'][0]
        dv_uri = dv_uri.split('?')[0] if '?' in dv_uri else dv_uri
    elif len(params['dv']) == 2:
        dv_uri=params['dv'][1]
        dv_uri = dv_uri.split('?')[0] if '?' in dv_uri else dv_uri
    else:
        pass

    manifest_id=params['id'][0]

    return [dv_uri, manifest_id]

def Metadata_Extract(x):
    d= {
        'uri_id': [],
        'assetid': [],      #assetid
        'humanid': [],      #humanid
        'servicetype': [],  #servicetype
        'contenttype': [],  #contenttype
        'channel': [],      #channel
        'title': [],        #title
        'serietitle': [],   #serietitle
        'season': [],       #season
        'episode': [],      #episode
        'genre': [],        #genre
        'rating': [],       #rating
        'releaseyear': [],  #releaseyear
        'duration': [],     #duration
        }
    for URI_ID in x:
        http=urllib3.PoolManager()
        r=http.request(
            'GET',
            f'HTTPS://syndication-prod.vcc/v1/syndication/url-related-asset?urlPath={URI_ID}',
            headers={
                'INTERNAL-API-KEY':'yo1-0rvb7LtIIqZ1ZBw4F161f5fv036'
                }
            )
        response=json.loads(r.data.decode('utf-8'))['responseObject']
        duration=response['displayRuntime']
        if duration != None:
            duration=duration.split(':')
            while len(duration)>3:
                duration.remove('')
            duration_seconds=round(
                datetime.timedelta(
                    hours=int(duration[0]),
                    minutes=int(duration[1]),
                    seconds=int(duration[2])
                    ).total_seconds()
                )
        else:
            duration_seconds=0
            mail_subject='WARNING etlaroundV2 PROD Metadata_Extract error' #Se establece el asunto del correo.
            mail_body=f"Se ha establecido la duracion del contenido en 0 ya que el mismo no esta disponble en la metadata de API assetId: {response['assetId']}"
            SendMail(mail_body, mail_subject) #Se envia correo electronico.
        contenttype=response['assetType']
        d['uri_id'].append(URI_ID)
        d['assetid'].append(response['assetId']),                 #assetid
        d['humanid'].append(response['humanId']),                 #humanid
        d['servicetype'].append(response['serviceType']),         #servicetype
        d['contenttype'].append(contenttype),                     #contenttype
        d['channel'].append(response['brand']),                   #channel
        d['title'].append(response['titleOriginal']),             #title
        d['genre'].append(response['genres'][0] if response['genres'] !=[] else 'None'), #genre
        d['rating'].append(response['ratings'][0]),               #rating
        d['releaseyear'].append(response['releaseYear']),         #releaseyear
        d['duration'].append(duration_seconds),               #duration

        if contenttype =='at_episode':
            d['serietitle'].append(response['seasonOriginalTitle']),  #serietitle
            d['season'].append(str(response['seasonNumber'])),             #season
            d['episode'].append(str(response['number'])),                  #episode

        elif contenttype == 'at_movie':
            d['serietitle'].append(''),                             #serietitle
            d['season'].append(''),                                 #season
            d['episode'].append(''),                                #episode
                
    return pd.DataFrame(d)

def playbacks_task(summary_dict):
    try:
        postgresql=psycopg2.connect(data_base_connect) #Se establece conexion con la base de datos.
        curpsql=postgresql.cursor() #Se activa el cursor en la base de datos.
        for index in summary_dict.keys():
            data=summary_dict[index]
            print(data)
            if len(data)==21:
                curpsql.execute(f"SELECT segmentos FROM {db_table} WHERE manifestid LIKE '{data['manifestid']}';")
                rowcount=curpsql.rowcount
                if rowcount!=0:
                    segment=curpsql.fetchone()[0]+data['segmentos']
                    sql=f"UPDATE {db_table} SET datetime=%s, mso_country=%s, mso_name=%s, clientid=%s, uri=%s, assetid=%s, humanid=%s, servicetype=%s, contenttype=%s, channel=%s, title=%s, serietitle=%s, releaseyear=%s, season=%s, episode=%s, genre=%s, rating=%s, duration=%s, segmentos=%s WHERE manifestid LIKE '{data['manifestid']}';"
                    data_sql=(
                        data['datetime'], data['country'], data['mso'], data['client_id'], data['uri_id'], data['assetid'],
                        data['humanid'], data['servicetype'], data['contenttype'], data['channel'], data['title'], data['serietitle'],
                        data['releaseyear'], data['season'], data['episode'], data['genre'], data['rating'], data['duration'], segment
                        )
                    curpsql.execute(sql, data_sql)
                else:
                    sql=f"INSERT INTO {db_table} VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"
                    data_sql=(
                        data['manifestid'], data['datetime'], data['country'], data['mso'], data['device'], data['client_id'],
                        data['uri_id'], data['assetid'], data['humanid'], data['servicetype'], data['contenttype'], data['channel'],
                        data['title'], data['serietitle'], data['releaseyear'], data['season'], data['episode'], data['genre'],
                        data['rating'], data['duration'], data['segmentos']
                        )
                    curpsql.execute(sql,data_sql)
            elif len(data)==3:
                curpsql.execute(f"SELECT segmentos FROM {db_table} WHERE manifestid LIKE '{data['manifestid']}';")
                rowcount=curpsql.rowcount
                if rowcount!=0:
                    segment=curpsql.fetchone()[0]+data['segmentos']
                    curpsql.execute(f"UPDATE {db_table} SET segmentos={segment} WHERE manifestid LIKE '{data['manifestid']}';")
                else:
                    curpsql.execute(f"INSERT INTO {db_table} (manifestid, segmentos, device) VALUES ('{data['manifestid']}', {data['segmentos']}, '{data['device']}');")
            elif len(data)==20:
                curpsql.execute(f"SELECT segmentos FROM {db_table} WHERE manifestid LIKE '{data['manifestid']}';")
                rowcount=curpsql.rowcount
                if rowcount!=0:
                    sql=f"UPDATE {db_table} SET datetime=%s, mso_country=%s, mso_name=%s, clientid=%s, uri=%s, assetid=%s, humanid=%s, servicetype=%s, contenttype=%s, channel=%s, title=%s, serietitle=%s, releaseyear=%s, season=%s, episode=%s, genre=%s, rating=%s, duration=%s WHERE manifestid LIKE '{data['manifestid']}';"
                    data_sql=(
                        data['datetime'], data['country'], data['mso'], data['client_id'], data['uri_id'], data['assetid'],
                        data['humanid'], data['servicetype'], data['contenttype'], data['channel'], data['title'], data['serietitle'],
                        data['releaseyear'], data['season'], data['episode'], data['genre'], data['rating'], data['duration']
                        )
                    curpsql.execute(sql, data_sql)
                else:
                    sql=f"INSERT INTO {db_table} (manifestid, datetime, mso_country, mso_name, clientid, uri, assetid, humanid, servicetype, contenttype, channel, title, serietitle, releaseyear, season, episode, genre, rating, duration, segmentos) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"
                    data_sql=(
                        data['manifestid'], data['datetime'], data['country'], data['mso'], data['client_id'], data['uri_id'],
                        data['assetid'], data['humanid'], data['servicetype'], data['contenttype'], data['channel'], data['title'],
                        data['serietitle'], data['releaseyear'], data['season'], data['episode'], data['genre'], data['rating'],
                        data['duration'], data['segmentos']
                        )
                    curpsql.execute(sql, data_sql)
        postgresql.commit()
        postgresql.close()
        return {}
    except:
        error=sys.exc_info()[2] #Captura del error generado por el sistema.
        errorinfo=traceback.format_tb(error)[0]
        print(f'Error: {str(sys.exc_info()[1])}', f'error_info : {errorinfo}')
        return {'Error': str(sys.exc_info()[1]), 'error_info' : errorinfo}