import Helpers.mongoDB as m
import inspect

print("=== INFO DEL MÓDULO Helpers.mongoDB ===")
print("m.__file__ ->", m.__file__)

print("\n=== ATRIBUTOS RELACIONADOS CON 'Mongo' EN EL MÓDULO ===")
print([x for x in dir(m) if "Mongo" in x or "mongo" in x])

print("\n=== OBJETO MongoDB QUE SE ESTÁ IMPORTANDO ===")
print("MongoDB =", m.MongoDB)
print("type(MongoDB) =", type(m.MongoDB))

# Intentar ver cuántos parámetros recibe _init_
try:
    print("\n=== INFO DE _init_ DE MongoDB ===")
    print("argcount:", m.MongoDB._init.code_.co_argcount)
    print("co_varnames:", m.MongoDB._init.code_.co_varnames)
except Exception as e:
    print("No se pudo inspeccionar __init__:", e)

# Ver el código fuente de MongoDB
try:
    print("\n=== CÓDIGO FUENTE DE MongoDB SEGÚN PYTHON ===")
    print(inspect.getsource(m.MongoDB))
except Exception as e:
    print("No se pudo obtener el source:", e)