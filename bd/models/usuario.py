def modelo_usuario(usuario) -> dict:
    return {
        "id": str(usuario["_id"]),
        "nombre": usuario["nombre"],
        "email": usuario["email"],
        "tenedor": usuario["tenedor"],
        "telefono": usuario["telefono"],
        "clave": usuario["clave"],
        "rol": usuario.get("rol", "propietario")
    }

def modelo_usuarios(usuarios) -> list:
    return [modelo_usuario(usuario) for usuario in usuarios]
