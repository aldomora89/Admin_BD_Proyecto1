import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import pandas
import pandas.io.sql as psql
import sys
import socket


class color:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    OKYELLOW = '\033[33m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'


def get_ip_address():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(("8.8.8.8", 80))
    print(s.getsockname()[0])
    s.close()


def Connect_DB():
    #dsn_hostname = 'UbuntuVirtAlmora10'
    #dsn_hostname = '192.168.43.253'
    dsn_hostname = '10.2.5.61'
    #dsn_hostname = '192.168.1.17'
    dsn_uid = input("Iniciar Sesión\nUsuario: ")
    # dsn_uid = "postgres"
    dsn_pwd = input("Contraseña: ")
    # dsn_pwd = "12345"
    # dsn_database = "postgres"
    dsn_database = input("Ingrese la base de datos: ")
    dsn_port = "5432"

    conn_string = "host=" + dsn_hostname + " port=" + dsn_port + " dbname=" + dsn_database + " user=" + dsn_uid + " password=" + dsn_pwd
    print("Connecting to database\n  ->%s" % (conn_string))
    global conn
    conn = psycopg2.connect(conn_string)
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    print("Connected!\n")


def printTables(tname):
    my_table = pandas.read_sql('SELECT * FROM {tab}'.format(tab=tname), conn)
    print(my_table)


def printBDs():
    my_BD = pandas.read_sql('\l',conn)
    #my_BD = psql.read_sql("\l", conn)
    print(my_BD)


# Metodos para DDL


def crear_BD(name):
    cur = conn.cursor()
    cur.execute('''CREATE DATABASE {tab}'''.format(tab=name))
    print("Base de Datos creada con exito!")
    cur.close()
    conn.commit()


def borrar_BD(name):
    cur = conn.cursor()
    cur.execute('''DROP DATABASE {tab}'''.format(tab=name))
    print("Base de Datos borrada con exito!")
    cur.close()
    conn.commit()

def crear_schema(nschema):
    cur = conn.cursor()
    p = input("Autorizacion (si / no)")
    if p=='si':
        user = input("Ingrese el usuario a autorizar: ")
        comando = '''CREATE SCHEMA {tab} AUTHORIZATION '''.format(tab=nschema)
        comando = comando + user + ";"
        cur.execute(comando)
        print("Schema creado " + nschema +  " y autorizado para " + user +  " con exito !")
    else:
        cur.execute('''CREATE SCHEMA {tab} '''.format(tab=nschema))
        print("Esquema creado " + nschema + " con exito !")

    cur.close()
    conn.commit()



def crear_usuario(name):
    cur = conn.cursor()
    password = input("Ingrese el password: ")
    comando = '''CREATE USER {tap} WITH PASSWORD '''.format(tap=name)
    comando = comando + "'"+ password + "'" + ";"
    cur.execute(comando)
    print("Usuario "+ name + " creado con exito!" )
    conn.commit()

def borrar_usuario(name):
    cur = conn.cursor()
    #password = input("Ingrese el password: ")
    cur.execute('''DROP USER {tap};'''.format(tap=name))
    print("Usuario "+ name + " borrado con exito!" )
    conn.commit()

def cambiar_usuario(name):
    cur = conn.cursor()
    p = input("Digite: Cambiar nombre (n) Cambiar Contraseña (c) Asignar Rol (r)\n--->")
    if p =='n':
        newName = input("Ingrese el nuevo nombre: ")
        comando = '''ALTER USER {tap} RENAME TO '''.format(tap=name)
        comando = comando + newName + ";"
        cur.execute(comando)
        print("Usuario "+ newName + " actualizado con exito!" )

    elif p =='r':
        newRol= input("Ingrese el Rol a Asignar al usuario " + name + " :" )
        comando = '''ALTER ROLE {tap} '''.format(tap=name)
        comando = comando + newRol + ";"
        cur.execute(comando)
        print("Usuario "+ name + " rol actualizado con exito!" )

    elif p =='c':
        newPass = input("Ingrese la nueva contraseña: ")
        comando = '''ALTER USER {tap} PASSWORD '''.format(tap=name)
        comando = comando + "'"+ newPass + "'" + ";"
        cur.execute(comando)
        print("Contraseña de "+ name + " actualizada con exito!" )

    else:
        print("Ingrese algunos de los valores disponibles")

    conn.commit()

def crear_tabla(name):
    cur = conn.cursor()
    cur.execute('''CREATE TABLE {tab}
          (ID INT PRIMARY KEY     NOT NULL,
          NAME           TEXT    NOT NULL,
          AGE            INT     NOT NULL,
          ADDRESS        CHAR(50),
          SALARY         REAL);'''.format(tab=name))
    print("Tabla Creada Exitosamente")
    printTables(name)
    cur.close()
    conn.commit()


def borrar_tabla(name):
    cur = conn.cursor()
    cur.execute('''DROP TABLE {tab} '''.format(tab=name))
    print("Tabla Borrada Exitosamente")
    cur.close()
    conn.commit()


# Metodos para DML


def insert_to_table(tname):
    cur = conn.cursor()
    # tname = input("Ingrese el nombre de la tabla: ")
    id = input("ID: ")
    name = input("Nombre: ")
    age = input("Edad: ")
    address = input("Dirección: ")
    salary = input("Salario: ")

    cur.execute('''INSERT INTO {tab} (id,name,age,address,salary) VALUES (%s,%s,%s,%s,%s)'''.format(tab=tname),
                (id, name, age, address, salary))

    print("Ingreso de Datos Correctamente!")
    printTables(tname)
    cur.close()
    conn.commit()


def delete_to_table(tname):
    cur = conn.cursor()
    cur.execute('''DELETE FROM {tap}'''.format(tap=tname))
    print("Datos Borrados de Tabla " + tname + " correctamente!")
    cur.close()
    conn.commit()

def select_to_table(tname):
    my_table = pandas.read_sql('SELECT * FROM {tab}'.format(tab=tname), conn)
    print(my_table)


#Metodo interpretador de lenguaje

def interpreta_comando(comando):
    arreglo = comando.split(" ")

    try:
        primerComando = arreglo[0]
        segundoComando = arreglo[1]
        tercerComando = arreglo[2]

        if (primerComando == "crear" and segundoComando == "tabla"):
            crear_tabla(tercerComando)
        elif(primerComando == "crear" and segundoComando == "base_de_datos"):
            crear_BD(tercerComando)
        elif(primerComando == "borrar" and segundoComando == "tabla"):
            borrar_tabla(tercerComando)
        elif(primerComando == "borrar" and segundoComando == "base_de_datos"):
            borrar_BD(tercerComando)
        elif(primerComando =="crear" and segundoComando =="usuario"):
            crear_usuario(tercerComando)
        elif(primerComando == "borrar" and segundoComando =="usuario"):
            borrar_usuario(tercerComando)
        elif(primerComando == "cambiar" and segundoComando =="usuario"):
            cambiar_usuario(tercerComando)
        elif(primerComando == "crear" and segundoComando == "esquema"):
            crear_schema(tercerComando)
        elif(primerComando == "insertar" and segundoComando == "en_tabla"):
            insert_to_table(tercerComando)
        elif(primerComando == "borrar" and segundoComando =="en_tabla"):
            delete_to_table(tercerComando)
        elif(primerComando == "seleccionar" and segundoComando == "tabla"):
            select_to_table(tercerComando)

        else:
            print('Error de Lenguaje PyThonMora Intentelo Nuevamente')


    except (Exception, psycopg2.DatabaseError) as error:
        print('Error de Lenguaje PyThonMora:', error)
        Admin_DB()



# Metodo de Administracion de Base de Datos


def Admin_DB():
    try:
        estado = True
        while estado:

            print(color.OKYELLOW)

            m = input("Ingrese una acción dentro Gestor de BD \n a) DDL \n b) DML \n c) Salir \n --->")

            if m == 'a' or m == 'b' or m == 'c':

                # if para opciones de DDL
                if m == 'a':
                    x = input("Seleccione una opción Data Definition Language \n 1) Base de Datos \n 2) Esquema \n 3) Tabla \n 4) Usuario \n S) Salir \n --->")
                    if   x == '1':
                        interpreta_comando(comando=input("Ingrese Crear o Borrar Base de Datos: "))
                        # crear_BD(name=input("Ingrese el nombre de la Base de Datos: "))
                    elif x == '2':
                        interpreta_comando(comando=input("Ingrese Crear esquema: "))

                    elif x == '3':
                        interpreta_comando(comando=input("Ingrese Crear o Borrar Tabla: "))
                        # crear_tabla(name=input("Ingrese el nombre de la tabla: "))
                    elif x == '4':
                        interpreta_comando(comando=input("Ingrese Crear - Borrar - Cambiar Usuario: "))
                        #borrar_tabla(name=input("Ingrese la tabla que desea borrar: "))
                    elif x == 's':
                        return Admin_DB()
                    elif x == '':
                        print("No puede Ingresar Valores en Blanco")

                    else:
                        print("\nNo ingreso un valor correcto por favor vuelva a intentarlo")

            # if para opciones de DML
                if m == 'b':
                    x = input(
                        "Seleccione una opción Data Manipulation Language \n 1) Insertar Datos a Tabla \n 2) Seleccionar Tabla  \n 3) Borra Filas de Tabla \n 4) Salir \n--->")
                    if x == '1':
                        interpreta_comando(comando=input("Ingrese Insertar en tabla: "))
                        #insert_to_table(tname=input("Ingrese el nombre de tabla: "))
                    elif x == '3':
                        interpreta_comando(comando=input("Ingrese Borrar en Tabla: "))
                        #delete_to_table(tname=input("Ingrese el nombre de la tabla: "))
                    elif x =='2':
                        interpreta_comando(comando=input("Ingrese Seleccionar Tabla: "))
                        #select_to_table(tname=input("ingrese el nombre de la tabla: "))

                if m == 'c':
                    d = input("Desea Salir del Administrador de BD: y/n \n --->")
                    if d == 'y':
                        estado = False

                    else:
                        print("Ingrese un valor correcto y - n")


            else:
                print("Dato Incorrecto Por favor ingrese : a - b - c")

    except (Exception, psycopg2.DatabaseError) as error:
                print('Error en el Administrador BD:', error)
                return (Admin_DB())


try:
    print("MOTOR DE BASE DE DATOS PYTHONMORA")

    estado = True
    while estado:
        m = input("Ingrese una acción dentro Gestor de BD \n a) Iniciar Sesión \n x) Salir \n --->")
        if  m == 'a' or m == 'x':
            if m == 'a':

                # get_ip_address()
                #dsn_hostname = 'UbuntuVirtAlmora10'
                dsn_hostname = '10.2.5.61'
                #dsn_hostname = '192.168.1.17'
                #dsn_hostname = '192.168.43.253'
                dsn_uid = input("Usuario: ")
                dsn_pwd = input("Contraseña: ")
                #dsn_pwd = "12345"
                #dsn_database = "postgres"
                dsn_database = input("Ingrese la base de datos: ")
                dsn_port = "5432"

                # Conexion a Base de Datos

                conn_string = "host=" + dsn_hostname + " port=" + dsn_port + " dbname=" + dsn_database + " user=" + dsn_uid + " password=" + dsn_pwd
                print("Connecting to database\n  ->%s" % (conn_string))
                conn = psycopg2.connect(conn_string)
                conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
                print("Connected!\n")
                estado = False


            elif m == 'x':
                d = input("Realmente desea salir: y/n \n --->")
                if d == 'y':
                    estado = False
                    exit()
                else:
                    print("Valor incorrecto debe ingresar y - n")

        else:
            print("Valor incorrecto debe Ingresar a - x")


except (Exception, psycopg2.DatabaseError) as error:
    print('Error de Conexion a la Base de Datos:', error)
    try:
        Connect_DB()

    except (Exception, psycopg2.DatabaseError) as error:
        print('Error to the database:', error)
        Connect_DB()


Admin_DB()
