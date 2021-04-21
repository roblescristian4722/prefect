#!/usr/bin/python3
import requests
import json
from os import system
import sys
from prefect import task, Flow
from prefect.engine import signals
from prefect.schedules import IntervalSchedule
import datetime

# alertas que se ejecutan cada que hay un cambio de estados en un flujo o tarea
def fetch_alert(obj, old_state, new_state):
    if new_state.is_failed():
        print("\nNo se han podido recuperar los usuarios de la url")

def type_alert(obj, old_state, new_state):
    if new_state.is_failed():
        print("\nEl tipo de operación no está reconocido.\nTipos de operación reconocidos:")
        print("->usuarios\n->comentarios\n->posts")

def print_alert(obj, old_state, new_state):
    if new_state.is_failed():
        print("\nLos datos no se han podido imprimir")

def flow_alert():
    print("\nNo ha ingresado argumentos suficientes, pruebe con lo siguiente:")
    print("[file_name].py usuarios comentarios (opcional)programar=[cantidad de minutos]")
    print("[file_name].py usuarios usuarios (opcional)programar=[cantidad de minutos]")
    print("[file_name].py usuarios posts (opcional)programar=[cantidad de minutos]")
    print("[file_name].py usuarios custom [URL...] (opcional)programar=[cantidad de minutos]")

# Función para determinar si el flujo de trabajo de programara por una cantidad
# n de segundos o no lo hará
def get_schedule_time():
    s = 0
    if ("programar" in sys.argv[-1]):
        arg = sys.argv[-1]
        arg = arg.replace("programar=", "")
        s = int(arg)
    return s

# Tarea que obtiene los usuarios de una página de prueba
@task(state_handlers=[fetch_alert])
def fetch_data(url):
    req = requests.get(url)
    data = req.json()
    if (len(data) == 0):
        raise signals.FAIL
    return data

@task(state_handlers=[type_alert])
def data_type(op):
    url = "https://jsonplaceholder.typicode.com"
    if (op == "usuarios"):
        return f"{url}/users"
    elif (op == "comentarios"):
        return f"{url}/comments"
    elif (op == "posts"):
        return f"{url}/posts"
    elif (op == "custom"):
        return sys.argv[2]
    else:
        raise signals.FAIL

@task(state_handlers=[print_alert])
def print_data(data):
    for d in data:
        for key in d:
            print(f"{key}: {d[key]}")
        print("")

if (get_schedule_time() == 0):
    # Se genera un flujo de trabajo sin un schedule
    with Flow("flujo para obtención de datos") as f:
        # Se crean variables que representan las tareas añadidas al flujo de
        # trabajo, es decir, no se ejecutan las funciones, solo se añaden al flujo
        if (len(sys.argv) <= 1):
            flow_alert()
        else:
            data = data_type(sys.argv[1])
            fetched = fetch_data(data)
            printed = print_data(fetched)
else:
    schedule = IntervalSchedule(interval=datetime.timedelta(minutes=get_schedule_time()))
    # Se genera un flujo de trabajo con un schedule para cada N minutos
    with Flow("flujo para obtención de datos", schedule) as f:
        # Se crean variables que representan las tareas añadidas al flujo de
        # trabajo, es decir, no se ejecutan las funciones, solo se añaden al flujo
        if (len(sys.argv) <= 1):
            flow_alert()
        else:
            data = data_type(sys.argv[1])
            fetched = fetch_data(data)
            printed = print_data(fetched)

# Se ejecuta el flujo de trabajo
f.run()
