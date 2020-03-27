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
    
    """
    pass


# In[41]:


def lista_directorio(client, path):
    """
    client --hdfs.InsecureClient not null
    path   --string not null
    List the path inside the client's HDFS
    """
    try:
        insides = client.list(path)
        print("se listan los archivos en hdfs:/" + path)
        for content in insides: print("\t" + content)
        return insides
    except:
        print("Ocurrió un error, el cliente o ruta son")



# In[42]:


def lectura_HDFS(cliente, path_archivo):
    pass


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

