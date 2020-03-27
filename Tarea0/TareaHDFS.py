#!/usr/bin/env python
# coding: utf-8

# In[26]:

from os import getenv
from hdfs import InsecureClient
import json


# In[27]:


def conexion(url, user = "hdfs"):
    """
    url --string no null, url del host
    
    Esta función logra la conexion con hdfs 
    
    """
    try:
        client = InsecureClient(url, user)
        return client
    except:
        print("Ocurrio un error favor de verificar la url del host")


# In[34]:


def crear_directorio(client,pathhdfs):
    """
    pathhdfs --string not null 
    Esta funcion crea un directorio en hdfs, pasandole la ruta donde sera creado
    """
    try:
        pathcreado=client.makedirs(pathhdfs)
        print("se creo el directorio " + pathhdfs)
        return pathcreado
    except:
        print("Ocurrio un error favor de verificar")


# Complete las funciones restantes, documente sus funciones.
# 1. Cargar un archivo, elija alguno de su directorio de preferencia un json o un txt.
# 2. Listar archivos dentro de una ruta de hdfs
# 3. Leer un archivo de hdfs.
# 4. Elimine el directorio

# In[40]:


def cargar_archivo(client, pathhdfs, local_path):
    """
    Uploads a file or directory in a client's HDFS path.

    :param client     --hdfs.client.InsecureClient not null: A connected HDFS client.
    :param pathhdfs   --string not null: Target HDFS path.
    :param local_path --string not null: Local path to file or folder.
    """
    try:
        upload = client.upload(pathhdfs, local_path, cleanup: True)
        print("archivo subido a hdfs:/" + upload)
        return upload
    except:
        print("Ocurrió un error, verifica los parámetros")


# In[41]:


def lista_directorio(client, path):
    """
    List names of files contained in a client's HDFS path.

    :param client --hdfs.client.InsecureClient not null: A connected HDFS client.
    :param path   --string not null: Remote path to a directory.
    """
    try:
        insides = client.list(path)
        print("se listan los archivos en hdfs:/" + path)
        for content in insides: print("\t" + content)
        return insides
    except:
        print("Ocurrió un error, el cliente o ruta son")



# In[42]:


def lectura_HDFS(client, path_archivo, as_json = False):
    """
    Read a file from a client HDFS, directly loading it in memory or attempting JSON deserialization

    :param client       --hdfs.client.InsecureClient not null: A connected HDFS client.
    :param path_archivo --string not null: HDFS path to a file.
    :param as_json      --bool: Do you want a JSON deserealization?
    """
    try:
        read = None
        if (as_json):
            with client.read(path_archivo) as reader:
                read = reader.read()
        else:
            with client.read(path_archivo, encoding='utf-8') as reader:
                read = json.load(reader)
        print("se leyó el archivo en " + path_archivo)
        return read
    except:
        print("Ocurrio un error, el archivo no existe o no es legible.")    


# In[43]:


def eliminar_directorio(client,path_hdfs):
    pass


# In[35]:


def main():
    file=getenv('FILE', './data/ola.txt')
    url=getenv('HDFS_URL', 'http://localhost:9870')
    dir=getenv('DIR', '/tarea0')
    client=conexion(url)
    crear_directorio(client,"/prueba/")


# In[ ]:


if __name__=='__main__':
    main()

