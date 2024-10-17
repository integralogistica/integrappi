def modelo_usuario(usuario)->dict:
    return{
        "id":str(usuario["_id"]),
        "nombre": usuario["nombre"],
        "email": usuario["email"],
        "telefono":usuario["telefono"],
        "fecha_nacimiento": usuario["fecha_nacimiento"],
        "foto_perfil": usuario["foto_perfil"],
        "metodo_pago":usuario["metodo_pago"],
        "es_anfitrion": usuario["es_anfitrion"],
    }

def modelo_usuarios(usuarios)->list:
    return [modelo_usuario(usuario) for usuario in usuarios]