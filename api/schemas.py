from pydantic import BaseModel
from typing import Optional, List
from datetime import date, time
from pydantic import BaseModel, Field

class EmpleadoResponse(BaseModel):
    """Modelo SOLO para respuestas (GET)"""
    id_empleado: int
    nombre: str
    apellido: str
    tipo_identificacion: str
    numero_identificacion: str
    fecha_nacimiento: date  # Ajusta el tipo según tu BD
    correo_electronico: Optional[str] = None
    telefono: Optional[str] = None
    calle: Optional[str] = None
    numero_calle: Optional[str] = None
    localidad: Optional[str] = None
    partido: Optional[str] = None
    provincia: Optional[str] = None
    genero: Optional[str] = None
    nacionalidad: Optional[str] = None
    estado_civil: Optional[str] = None

class EmpleadoBase(BaseModel):

        nombre: str
        apellido: str
        tipo_identificacion: str
        numero_identificacion: str
        fecha_nacimiento: str
        correo_electronico: str
        telefono: str
        calle: str
        numero_calle: str
        localidad: str
        partido: str
        provincia: str
        genero: str
        pais_nacimiento: str
        estado_civil: str
        id_usuario: str


class EmpleadoBase2(BaseModel):
    nombre: str
    apellido: str
    tipo_identificacion: str
    numero_identificacion: str
    fecha_nacimiento: str
    correo_electronico: str
    telefono: str
    calle: str
    numero_calle: str
    localidad: str
    partido: str
    provincia: str
    genero: str
    pais_nacimiento: str
    estado_civil: str

class EmpleadoUpdate(BaseModel):
        id_usuario: int
        telefono: Optional[str] = None
        correo_electronico: Optional[str] = None
        calle: Optional[str] = None
        numero_calle: Optional[str] = None
        localidad: Optional[str] = None
        partido: Optional[str] = None  # Nueva variable agregada
        provincia: Optional[str] = None

class EmpleadoConsulta(BaseModel):
    numero_identificacion: str

class EmpleadoIDRequest(BaseModel):
    empleado_id: str

class EmpleadoPeriodoRequest(BaseModel):
    empleado_id: str
    año: Optional[int] = None
    mes: Optional[int] = None

class BuscarEmpleadoRequest(BaseModel):
    nombre: Optional[str] = None
    apellido: Optional[str] = None
    dni: Optional[str] = None
    pagina: int = 1
    por_pagina: int = 10

class EmpleadoIDIntRequest(BaseModel):
    empleado_id: int

class LoginRequest(BaseModel):
    username: str
    password: str

class Permisos(BaseModel):
    online_login: bool
    offline_login: bool
    ver_datos_personales: bool
    editar_datos_personales: bool
    ver_datos_laborales: bool
    agregar_datos_laborales: bool
    editar_datos_laborales: bool
    agregar_empleado: bool
    ver_registro_asistencia: bool
    ver_informacion_bancaria: bool
    editar_informacion_bancaria: bool
    ingresar_asistencia: bool
    ingresar_inasistencia: bool
    ver_historial_nominas: bool
    calcular_nomina_manualmente: bool
    calcular_nomina_automaticamente: bool
    agregar_concepto: bool
    agregar_departamento: bool
    agregar_puesto: bool
    agregar_categoria: bool
    agregar_salario_con_vigencia: bool
    ver_vista_previa_recibo_sueldo: bool
    descargar_recibo_sueldo: bool
    ver_reportes: bool
    cerrar_sesion: bool

class LoginResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    permisos: Permisos
    rol: str
    id_empleado: int
    numero_identificacion: str
    id_usuario: int

class RegistroUpdate(BaseModel):
    tipo: Optional[str] = None
    fecha: Optional[date] = None
    hora: Optional[time] = None
    estado_asistencia: Optional[str] = None
    turno_asistencia: Optional[str] = None
    puesto_del_asistente: Optional[str] = None
    vector_capturado: Optional[str] = None

class CrearUsuarioRequest(BaseModel):
    id_empleado: int
    id_rol: int
    nombre_usuario: str
    contrasena: str
    motivo: Optional[str] = None

class UsuarioModel(BaseModel):
    id_usuario: int
    id_empleado: int
    id_rol: int
    nombre_usuario: str
    contrasena: str
    esta_activo: bool
    fecha_activacion: date | None
    fecha_creacion: date | None
    motivo: str | None

    
