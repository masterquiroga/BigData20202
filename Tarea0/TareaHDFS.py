#!/usr/bin/env python
# coding: utf-8
"""
Copyright [2020] [Durán Toledo, Albert Alejandro (oyente)]
Copyright [2020] [González Quiroga, Víctor Gerardo (oyente)]
Copyright [2020] [Pulido Ortega, Israel (oyente)]

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

   This work is a derivative from https://github.com/jimmyhm/BigData20202
"""

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
        upload = client.upload(pathhdfs, local_path, overwrite = True,)
        print("archivo subido a hdfs:/" + upload)
        return upload
    except:
        print("Ocurrió un error, verifica el archivo a subir")


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
        read = client.read(path_archivo)
        #if (as_json):
        #    with client.read(path_archivo) as reader:
        #        read = reader.read()
        #else:
        #    with client.read(path_archivo, encoding='utf-8') as reader:
        #        read = json.load(reader)
        print("se leyó el archivo en " + path_archivo)
        return read
    except:
        print("Ocurrio un error, el archivo no existe o no es legible.")    


# In[43]:


def eliminar_directorio(client, path_hdfs):
    """
    Remove a file or directory from a client's HDFS.

    :param client       --hdfs.client.InsecureClient not null: A connected HDFS client.
    :param path_hdfs --string not null: HDFS path.
    """
    try:
        deleted = client.delete(path_hdfs, recursive = True)
        print("se borró el archivo en" + deleted)
        return deleted
    except:
        print("Ocurrió un error, la ruta existe?")


# In[35]:


def main():
    filename = getenv('FILENAME', 'ola.txt')
    datadir = getenv('DATADIR', './data')
    file = datadir + '/' + filename
    url = getenv('HDFS_URL', 'http://localhost:50070')
    dir = getenv('DIR', '/tarea0')
    client = conexion(url)
    crear_directorio(client, dir)
    cargar_archivo(client, dir, file)
    lista_directorio(client, dir)
    lectura_HDFS(client, dir + '/' + filename)
    eliminar_directorio(client, dir)


# In[ ]:


if __name__=='__main__':
    main()

