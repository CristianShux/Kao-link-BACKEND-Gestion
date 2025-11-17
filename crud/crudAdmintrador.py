from datetime import datetime, date, time
import psycopg2
from psycopg2 import sql
from .database import db
from .crudEmpleado import Empleado
from typing import Optional
from typing import Tuple, List
from api.schemas import EmpleadoResponse
import cloudinary
import cloudinary.uploader
from cloudinary.uploader import upload as cloudinary_upload
import io
from crud import validacion_entrada
from auth.utils import registrar_evento_sistema

class AdminCRUD:

    @staticmethod
    def crear_empleado(id_usuario: int,nuevo_empleado):
        conn = None
        try:
            conn = db.get_connection()
            cur = conn.cursor()

            numero_calle = str(nuevo_empleado.numero_calle) if hasattr(nuevo_empleado, 'numero_calle') else None

            validacion_entrada.validar_datos_empleado(nuevo_empleado)

            # Calcular manualmente el próximo id_empleado
            cur.execute("SELECT MAX(id_empleado) FROM empleado")
            max_id = cur.fetchone()[0]
            nuevo_id = (max_id or 0) + 1

            cur.execute(
                """
                INSERT INTO empleado (
                    id_empleado, nombre, apellido, tipo_identificacion, numero_identificacion,
                    fecha_nacimiento, correo_electronico, telefono, calle,
                    numero_calle, localidad, partido, provincia, genero, 
                    pais_nacimiento, estado_civil
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id_empleado, nombre, apellido, numero_identificacion, 
                          numero_calle, telefono, correo_electronico
                """,
                (
                    nuevo_id, nuevo_empleado.nombre, nuevo_empleado.apellido, nuevo_empleado.tipo_identificacion,
                    nuevo_empleado.numero_identificacion, nuevo_empleado.fecha_nacimiento,
                    nuevo_empleado.correo_electronico, nuevo_empleado.telefono, nuevo_empleado.calle,
                    numero_calle, nuevo_empleado.localidad, nuevo_empleado.partido,
                    nuevo_empleado.provincia, nuevo_empleado.genero, nuevo_empleado.pais_nacimiento,
                    nuevo_empleado.estado_civil
                )
            )

            resultado = cur.fetchone()

            # Registrar evento en evento_sistema
            cur.execute(
                """
                INSERT INTO evento_sistema (id_usuario, tipo_evento, descripcion)
                VALUES (%s, %s, %s)
                """,
                (id_usuario, 'Otro', f'Se creó el empleado {resultado[1]} {resultado[2]} (ID: {resultado[0]})')
            )
            conn.commit()

            return {
                "id_empleado": resultado[0],
                "nombre": resultado[1],
                "apellido": resultado[2],
                "tipo_identificacion": nuevo_empleado.tipo_identificacion,
                "numero_identificacion": resultado[3],
                "fecha_nacimiento": nuevo_empleado.fecha_nacimiento,
                "correo_electronico": resultado[6],
                "telefono": resultado[5],
                "calle": nuevo_empleado.calle,
                "numero_calle": resultado[4],
                "localidad": nuevo_empleado.localidad,
                "partido": nuevo_empleado.partido,
                "provincia": nuevo_empleado.provincia,
                "genero": nuevo_empleado.genero,
                "pais_nacimiento": nuevo_empleado.pais_nacimiento,  # Ajustar nombre del campo si hace falta
                "estado_civil": nuevo_empleado.estado_civil
            }

        except Exception as e:
            if conn:
                conn.rollback()
                try:
                    registrar_evento_sistema(
                        conn,
                        id_usuario=id_usuario,
                        tipo_evento="Otro",
                        descripcion=f"Error al crear empleado: {str(e)}"
                    )
                    conn.commit()
                except Exception as log_error:
                    print(f"[ERROR] Fallo al registrar log: {log_error}")
            print(f"[ERROR] Error al crear empleado: {e}")
            raise

        finally:
            if conn:
                conn.close()




    @staticmethod
    def crear_empleado3(conn, nuevo_empleado): # <--- 1. RECIBE 'conn'
        # 2. YA NO MANEJA LA CONEXIÓN (borramos el try/except/finally de la conexión)
        try:
            cur = conn.cursor()

            # 👇 Tu código de la secuencia (esto está bien)
            cur.execute("SELECT MAX(id_empleado) FROM empleado")
            max_id = cur.fetchone()[0] or 0
            cur.execute("SELECT pg_get_serial_sequence('empleado', 'id_empleado')")
            seq_name = cur.fetchone()[0]
            cur.execute("SELECT setval(%s, %s, true)", (seq_name, max_id))
            
            numero_calle = str(nuevo_empleado.numero_calle) if hasattr(nuevo_empleado, 'numero_calle') else None

            # Tu validación (esto está bien)
            validacion_entrada.validar_datos_empleado(nuevo_empleado)

            cur.execute(
                """
                INSERT INTO empleado (
                    nombre, apellido, tipo_identificacion, numero_identificacion,
                    fecha_nacimiento, correo_electronico, telefono, calle,
                    numero_calle, localidad, partido, provincia, genero, 
                    pais_nacimiento, estado_civil
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id_empleado, nombre, apellido, numero_identificacion, 
                          numero_calle, telefono, correo_electronico
                """,
                (
                    nuevo_empleado.nombre, nuevo_empleado.apellido, nuevo_empleado.tipo_identificacion,
                    nuevo_empleado.numero_identificacion, nuevo_empleado.fecha_nacimiento,
                    nuevo_empleado.correo_electronico, nuevo_empleado.telefono, nuevo_empleado.calle,
                    numero_calle, nuevo_empleado.localidad, nuevo_empleado.partido,
                    nuevo_empleado.provincia, nuevo_empleado.genero, nuevo_empleado.pais_nacimiento,
                    nuevo_empleado.estado_civil
                )
            )

            resultado = cur.fetchone()
            
            cur.close()

            return {
                "id_empleado": resultado[0],
                "nombre": resultado[1],
                "apellido": resultado[2],
                "tipo_identificacion": nuevo_empleado.tipo_identificacion,
                "numero_identificacion": resultado[3],
                "fecha_nacimiento": nuevo_empleado.fecha_nacimiento,
                "correo_electronico": resultado[6],
                "telefono": resultado[5],
                "calle": nuevo_empleado.calle,
                "numero_calle": resultado[4],
                "localidad": nuevo_empleado.localidad,
                "partido": nuevo_empleado.partido,
                "provincia": nuevo_empleado.provincia,
                "genero": nuevo_empleado.genero,
                "pais_nacimiento": nuevo_empleado.pais_nacimiento,
                "estado_civil": nuevo_empleado.estado_civil
            }

        except Exception as e:
        # 4. SOLO LANZAMOS EL ERROR. El endpoint hará el rollback.
            print(f"[ERROR] Error en la lógica de crear_empleado3: {e}")
            raise # Vuelve a lanzar el error para que el endpoint lo atrape

    @staticmethod
    def habilitar_cuenta(id_empleado: int):
        try:
            conn = db.get_connection()
            cur = conn.cursor()
            cur.execute(
                """
                UPDATE usuario
                SET esta_activo = TRUE,
                    fecha_activacion = %s
                WHERE id_empleado = %s
                """,
                (date.today(), id_empleado)
            )
            if cur.rowcount == 0:
                raise ValueError("No se encontró el usuario con ese ID de empleado")
            conn.commit()
        finally:
            if conn:
                db.return_connection(conn)

    @staticmethod
    def obtener_empleado():
        """Lista todos los empleados con información básica"""
        try:
            conn = db.get_connection()
            cur = conn.cursor()
            cur.execute(
                """
                SELECT id_empleado, numero_identificacion, nombre, apellido, correo_electronico, telefono, imagen_perfil_url
                FROM empleado
                ORDER BY apellido, nombre
                """
            )
            return [
                {
                    "id_empleado": row[0],
                    "numero_identificacion": row[1],
                    "nombre": row[2],
                    "apellido": row[3],
                    "correo": row[4],
                    "telefono": row[5],
                    "imagen_perfil_url": row[6]
                }
                for row in cur.fetchall()
            ]
        finally:
            if conn:
                db.return_connection(conn)

    @staticmethod
    def obtener_empleado_por_id(id_empleado):
        """Obtiene la información básica de un empleado por su ID"""
        try:
            conn = db.get_connection()
            cur = conn.cursor()
            cur.execute(
                """
                SELECT id_empleado, numero_identificacion, nombre, apellido,
                       correo_electronico, telefono, imagen_perfil_url
                FROM empleado
                WHERE id_empleado = %s
                """,
                (id_empleado,)
            )
            row = cur.fetchone()
            if row:
                return {
                    "id_empleado": row[0],
                    "numero_identificacion": row[1],
                    "nombre": row[2],
                    "apellido": row[3],
                    "correo": row[4],
                    "telefono": row[5],
                    "imagen_perfil_url": row[6]
                }
            return None
        finally:
            if conn:
                db.return_connection(conn)

    @staticmethod
    def obtener_detalle_empleado(numero_identificacion: str):
        """Obtiene todos los datos de un empleado"""
        try:
            conn = db.get_connection()
            cur = conn.cursor()
            cur.execute(
                """
                SELECT id_empleado, nombre, apellido, tipo_identificacion, numero_identificacion,
                       fecha_nacimiento, correo_electronico, telefono, calle,
                       numero_calle, localidad, partido, provincia, genero, pais_nacimiento, estado_civil, imagen_perfil_url
                FROM empleado
                WHERE numero_identificacion = %s
                """,
                (numero_identificacion,)
            )
            result = cur.fetchone()
            if result:
                return {
                    "id_empleado": result[0],
                    "nombre": result[1],
                    "apellido": result[2],
                    "tipo_identificacion": result[3],
                    "numero_identificacion": result[4],
                    "fecha_nacimiento": result[5],
                    "correo_electronico": result[6],
                    "telefono": result[7],
                    "calle": result[8],
                    "numero_calle": result[9],
                    "localidad": result[10],
                    "partido": result[11],
                    "provincia": result[12],
                    "genero": result[13],
                    "pais_nacimiento": result[14],
                    "estado_civil": result[15],
                    "imagen_perfil_url": result[16]
                }
            return None
        finally:
            if conn:
                db.return_connection(conn)

    @staticmethod
    def registrar_jornada_calendario(id_empleado: int, fecha: date, estado_jornada: str,
                                     hora_entrada: time = None, hora_salida: time = None,
                                     horas_trabajadas: int = None, horas_extras: int = None,
                                     descripcion: str = None):
        """Registra o actualiza una jornada en el calendario"""
        try:
                conn = db.get_connection()
                cur = conn.cursor()
                # Verificar si ya existe registro para esa fecha
                cur.execute(
                    "SELECT 1 FROM calendario WHERE id_empleado = %s AND fecha = %s",
                    (id_empleado, fecha)
                )
                existe = cur.fetchone()

                if existe:
                    # Actualizar registro existente
                    cur.execute(
                        """
                        UPDATE calendario SET
                            estado_jornada = %s,
                            hora_entrada = %s,
                            hora_salida = %s,
                            horas_trabajadas = %s,
                            horas_extras = %s,
                            descripcion = %s
                        WHERE id_empleado = %s AND fecha = %s
                        RETURNING id_asistencia
                        """,
                        (
                            estado_jornada, hora_entrada, hora_salida,
                            horas_trabajadas, horas_extras, descripcion,
                            id_empleado, fecha
                        )
                    )
                else:
                    # Insertar nuevo registro
                    cur.execute(
                        """
                        INSERT INTO calendario (
                            id_empleado, fecha, dia, estado_jornada,
                            hora_entrada, hora_salida, horas_trabajadas,
                            horas_extras, descripcion
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        RETURNING id_asistencia
                        """,
                        (
                            id_empleado, fecha, fecha.strftime("%A"),
                            estado_jornada, hora_entrada, hora_salida,
                            horas_trabajadas, horas_extras, descripcion
                        )
                    )

                db.conn.commit()
                return cur.fetchone()[0]
        except Exception as e:
            db.conn.rollback()
            raise Exception(f"Error al registrar jornada: {e}")
        finally:
            if conn:
                db.return_connection(conn)

    @staticmethod
    def obtener_calendario_empleado(id_empleado: int, mes: int = None, año: int = None):
        """Obtiene el calendario laboral de un empleado"""
        query = """
            SELECT id_asistencia, fecha, dia, estado_jornada,
                   hora_entrada, hora_salida, horas_trabajadas,
                   horas_extras, descripcion
            FROM calendario
            WHERE id_empleado = %s
        """
        params = [id_empleado]

        if mes and año:
            query += " AND EXTRACT(MONTH FROM fecha) = %s AND EXTRACT(YEAR FROM fecha) = %s"
            params.extend([mes, año])

        query += " ORDER BY fecha DESC"

        try:
            conn = db.get_connection()
            cur = conn.cursor()
            cur.execute(query, params)
            return [
                {
                    "id_asistencia": row[0],
                    "fecha": row[1],
                    "dia": row[2],
                    "estado_jornada": row[3],
                    "hora_entrada": row[4].strftime("%H:%M") if row[4] else None,
                    "hora_salida": row[5].strftime("%H:%M") if row[5] else None,
                    "horas_trabajadas": row[6],
                    "horas_extras": row[7],
                    "descripcion": row[8]
                }
                for row in cur.fetchall()
            ]
        finally:
            if conn:
                db.return_connection(conn)

    @staticmethod
    def buscar_empleado_por_numero_identificacion(numero_identificacion: str):
        """Busca un empleado por número de identificación"""
        try:
            conn = db.get_connection()
            cur = conn.cursor()
            cur.execute(
                """
                SELECT id_empleado, numero_identificacion, nombre, apellido, correo_electronico, telefono
                FROM empleado
                WHERE numero_identificacion = %s
                """,
                (numero_identificacion,)
            )
            result = cur.fetchone()
            if result:
                return {
                    "id_empleado": result[0],
                    "numero_identificacion": result[1],
                    "nombre": result[2],
                    "apellido": result[3],
                    "correo": result[4],
                    "telefono": result[5]
                }
            return None
        finally:
            if conn:
                db.return_connection(conn)

    @staticmethod
    def buscar_avanzado(
            # Filtra por nombre o apellido los empleados que coincidan. Tambien se puede usar DNI.
            nombre: Optional[str] = None,
            apellido: Optional[str] = None,
            dni: Optional[str] = None,
            pagina: int = 1,
            por_pagina: int = 10
    ) -> Tuple[List[EmpleadoResponse], int]:
        """Versión con paginación"""
        # Query principal
        base_query = """
               SELECT id_empleado, nombre, apellido, tipo_identificacion, numero_identificacion, 
                   fecha_nacimiento, correo_electronico, telefono, calle, numero_calle, 
                   localidad, partido, provincia, genero, pais_nacimiento, estado_civil
               FROM empleado
               WHERE 1=1
           """

        # Query para contar el total  ( número total de registros que coinciden con los filtros de búsqueda)
        count_query = "SELECT COUNT(*) FROM empleado WHERE 1=1"

        params = []

        # Filtros
        # Insensitive: no distingue mayúsculas/minúsculas
        filters = []
        if nombre:
            filters.append("nombre ILIKE %s")
            params.append(f"%{nombre}%")

        if apellido:
            filters.append("apellido ILIKE %s")
            params.append(f"%{apellido}%")

        if dni:
            filters.append("numero_identificacion LIKE %s")
            params.append(f"%{dni}%")

        if filters:
            where_clause = " AND " + " AND ".join(filters)
            base_query += where_clause
            count_query += where_clause

        # Paginación: subconjunto de empleados a mostrar por página
        base_query += " LIMIT %s OFFSET %s"
        params.extend([por_pagina, (pagina - 1) * por_pagina])

        with db.conn.cursor() as cur:
            # Obtener resultados
            cur.execute(base_query, tuple(params))
            results = cur.fetchall()

            # Obtener conteo total
            cur.execute(count_query, tuple(params[:-2]))  # Excluye LIMIT/OFFSET
            total = cur.fetchone()[0]

            # Cada fila de la base de datos (result) se convierte en un objeto Empleado, psycopg2 devuelve filas como tuplas
            empleados = [
                EmpleadoResponse(
                    id_empleado=row[0],
                    nombre=row[1],
                    apellido=row[2],
                    tipo_identificacion=row[3],
                    numero_identificacion=row[4],
                    fecha_nacimiento=row[5],
                    correo_electronico=row[6],
                    telefono=row[7],
                    calle=row[8],
                    numero_calle=row[9],
                    localidad=row[10],
                    partido=row[11],
                    provincia=row[12],
                    genero=row[13],
                    pais_nacimiento=row[14],
                    estado_civil=row[15]
                )
                for row in results
            ]

        return empleados, total

    @staticmethod
    def buscar_informacion_laboral_por_id_empleado(id_empleado: int):
        """
        Busca la información laboral de un empleado por su ID.

        Args:
            id_empleado: ID del empleado a buscar

        Returns:
            Tupla con los campos: (departamento, puesto, turno, horario_entrada,
            horario_salida, fecha_ingreso, tipo_contrato) o None si no se encuentra.
        """
        try:
            conn = db.get_connection()
            cur = conn.cursor()
            query = """
                SELECT 
                    d.nombre,
                    p.nombre,
                    il.turno,
                    il.hora_inicio_turno,
                    il.hora_fin_turno,
                    il.fecha_ingreso,
                    il.tipo_contrato
                FROM informacion_laboral il
                JOIN departamento d ON il.id_departamento = d.id_departamento
                JOIN puesto p ON il.id_puesto = p.id_puesto
                WHERE il.id_empleado = %s
                ORDER BY il.fecha_ingreso DESC
                LIMIT 1
            """
            cur.execute(query, (id_empleado,))
            return cur.fetchone()  # Retorna directamente la tupla de resultados

        except Exception as e:
            print(f"Error al buscar información laboral: {str(e)}")
            raise ValueError(f"No se pudo obtener la información laboral: {str(e)}")
        finally:
            if conn:
                db.return_connection(conn)


    @staticmethod
    def obtener_puesto_por_id_empleado(id_empleado: int) -> Optional[str]:
        """
        Obtiene el puesto de un empleado por su ID.

        Args:
            id_empleado: ID del empleado a buscar

        Returns:
            Nombre del puesto o None si no se encuentra.
        """
        try:
            conn = db.get_connection()
            cur = conn.cursor()
            query = """
                    SELECT p.nombre
                    FROM informacion_laboral il
                    JOIN puesto p ON il.id_puesto = p.id_puesto
                    WHERE il.id_empleado = %s
                    ORDER BY il.fecha_ingreso DESC
                    LIMIT 1
                """
            cur.execute(query, (id_empleado,))
            result = cur.fetchone()
            return result[0] if result else None

        except Exception as e:
            print(f"Error al buscar puesto del empleado: {str(e)}")
            raise ValueError(f"No se pudo obtener el puesto: {str(e)}")
        finally:
            if conn:
                db.return_connection(conn)

    @staticmethod
    def obtener_categoria_por_id_empleado(id_empleado: int) -> Optional[str]:
        """
        Obtiene la categoría de un empleado por su ID.

        Args:
            id_empleado: ID del empleado a buscar

        Returns:
            Nombre de la categoría o None si no se encuentra.
        """
        try:
            conn = db.get_connection()
            cur = conn.cursor()
            query = """
                    SELECT c.nombre_categoria
                    FROM informacion_laboral il
                    JOIN categoria c ON il.id_categoria = c.id_categoria
                    WHERE il.id_empleado = %s
                    ORDER BY il.fecha_ingreso DESC
                    LIMIT 1
                """
            cur.execute(query, (id_empleado,))
            result = cur.fetchone()
            return result[0] if result else None

        except Exception as e:
            print(f"Error al buscar categoría del empleado: {str(e)}")
            raise ValueError(f"No se pudo obtener la categoría: {str(e)}")
        finally:
            if conn:
                db.return_connection(conn)

    @staticmethod
    def obtener_departamento_por_id_empleado(id_empleado: int) -> Optional[Tuple[str, str]]:
        """
        Obtiene el departamento de un empleado por su ID.

        Args:
            id_empleado: ID del empleado a buscar

        Returns:
            Tupla con (nombre_departamento, descripcion) o None si no se encuentra.
        """
        try:
            conn = db.get_connection()
            cur = conn.cursor()
            query = """
                    SELECT d.nombre, d.descripcion
                    FROM informacion_laboral il
                    JOIN departamento d ON il.id_departamento = d.id_departamento
                    WHERE il.id_empleado = %s
                    ORDER BY il.fecha_ingreso DESC
                    LIMIT 1
                """
            cur.execute(query, (id_empleado,))
            return cur.fetchone()  # Retorna directamente la tupla de resultados

        except Exception as e:
            print(f"Error al buscar departamento del empleado: {str(e)}")
            raise ValueError(f"No se pudo obtener el departamento: {str(e)}")
        finally:
            if conn:
                db.return_connection(conn)

    @staticmethod
    def obtener_rol_por_id_empleado(id_empleado: int) -> Optional[str]:
        """
        Obtiene el rol de un empleado por su ID.

        Args:
            id_empleado: ID del empleado a buscar

        Returns:
            Nombre del rol o None si no se encuentra.
        """
        try:
            conn = db.get_connection()
            cur = conn.cursor()
            query = """
                    SELECT p.nombre
                    FROM informacion_laboral il
                    JOIN rol p ON p.id_rol = p.id_rol
                    WHERE il.id_empleado = %s
                    ORDER BY il.fecha_ingreso DESC
                    LIMIT 1
                """
            cur.execute(query, (id_empleado,))
            result = cur.fetchone()
            return result[0] if result else None

        except Exception as e:
            print(f"Error al buscar rol del empleado: {str(e)}")
            raise ValueError(f"No se pudo obtener el rol: {str(e)}")
        finally:
            if conn:
                db.return_connection(conn)

    @staticmethod
    def obtener_id_rol_por_id_empleado(id_empleado: int):
        conn = db.get_connection()
        cur = conn.cursor()
        cur.execute("SELECT id_rol FROM usuario WHERE id_empleado = %s", (id_empleado,))
        result = cur.fetchone()
        return result[0] if result else None


    @staticmethod
    def actualizar_datos_personales2(id_usuario:int, id_empleado: int, telefono: str = None,
                                    correo_electronico: str = None, calle: str = None,
                                    numero_calle: str = None, localidad: str = None,
                                    partido: str = None, provincia: str = None):
        """
        Permite a un empleado actualizar sus datos personales.
        Solo actualiza los campos que recibe (los demás permanecen igual).

        Args:
            id_empleado: ID del empleado que realiza la actualización
            telefono: Nuevo número de teléfono (opcional)
            correo_electronico: Nuevo correo electrónico (opcional)
            calle: Nueva calle (opcional)
            numero_calle: Nuevo número de calle (opcional)
            localidad: Nueva localidad (opcional)
            partido: Nuevo partido (opcional)
            provincia: Nueva provincia (opcional)

        Returns:
            El objeto Empleado actualizado

        Raises:
            ValueError: Si hay error en los datos o en la operación
        """
        conn = None
        try:
            conn = db.get_connection()
            cur = conn.cursor()

            # Construir la consulta dinámicamente basada en los parámetros proporcionados
            updates = []
            params = []

            validacion_entrada.validar_actualizar_datos_empleado(
                telefono,
                correo_electronico,
                calle,
                numero_calle,
                localidad,
                partido,
                provincia
            )

            if telefono is not None:
                updates.append("telefono = %s")
                params.append(telefono)

            if correo_electronico is not None:
                # Verificar si el correo ya existe (excepto para este empleado)
                cur.execute(
                    "SELECT 1 FROM empleado WHERE correo_electronico = %s AND id_empleado != %s",
                    (correo_electronico, id_empleado)
                )
                if cur.fetchone():
                    raise ValueError("El correo electrónico ya está en uso por otro empleado")
                updates.append("correo_electronico = %s")
                params.append(correo_electronico)

            if calle is not None:
                updates.append("calle = %s")
                params.append(calle)

            if numero_calle is not None:
                updates.append("numero_calle = %s")
                params.append(numero_calle)

            if localidad is not None:
                updates.append("localidad = %s")
                params.append(localidad)

            if partido is not None:
                updates.append("partido = %s")
                params.append(partido)

            if provincia is not None:
                updates.append("provincia = %s")
                params.append(provincia)

            if not updates:
                raise ValueError("No se proporcionaron datos para actualizar")

            # Construir la consulta final
            query = f"""
                UPDATE empleado 
                SET {', '.join(updates)}
                WHERE id_empleado = %s
                RETURNING id_empleado
            """
            params.append(id_empleado)

            cur.execute(query, params)
            if cur.rowcount == 0:
                raise ValueError("No se encontró el empleado con el ID proporcionado")
            print(f"[DEBUG] Tipo de conn: {type(conn)}")

            # Registrar evento en la tabla evento_sistema
            cur.execute("""
                INSERT INTO evento_sistema (id_usuario, tipo_evento, descripcion)
                VALUES (%s, %s, %s)
            """, (
                id_usuario,
                'Otro',
                f'Datos personales actualizados para empleado ID {id_empleado}'
            ))
            conn.commit()
            return Empleado.obtener_por_id(id_empleado)
        finally:
            if conn:
                conn.close()



    @staticmethod
    def actualizar_imagen_perfil(image_bytes: bytes, usuario_id: int):
        """
        Sube una imagen a Cloudinary y actualiza la URL en la base de datos para el usuario indicado.

        Args:
            image_bytes: El contenido de la imagen en bytes
            usuario_id: ID del empleado a actualizar

        Returns:
            URL segura de la imagen subida
        """
        conn = None
        try:
            # Subir a Cloudinary
            result = cloudinary.uploader.upload(io.BytesIO(image_bytes), folder="perfiles")
            image_url = result["secure_url"]

            # Guardar en base de datos
            conn = db.get_connection()
            cur = conn.cursor()
            cur.execute(
                "UPDATE empleado SET imagen_perfil_url = %s WHERE id_empleado = %s",
                (image_url, usuario_id)
            )
            conn.commit()
            return image_url

        except Exception as e:
            raise Exception(f"Error al subir imagen: {e}")

        finally:
            if conn:
                conn.close()

    @staticmethod
    def eliminar_imagen_perfil(id_empleado: int):
        with db.get_connection() as conn:
            cur = conn.cursor()
            cur.execute("""
                   UPDATE empleado
                   SET imagen_perfil_url = NULL
                   WHERE id_empleado = %s
               """, (id_empleado,))
            conn.commit()
            return cur.rowcount

    @staticmethod
    def obtener_numero_identificacion(id_empleado: int):
        conn = db.get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT numero_identificacion FROM empleado WHERE id_empleado = %s", (id_empleado,))
                result = cur.fetchone()
                return result[0] if result else None
        finally:
            conn.close()

    @staticmethod
    def guardar_documento_tipo(empleado_id: int, contenido: bytes, tipo: str, descripcion: str = None):
        tipos_validos = [
            'DNI', 'CUIL', 'Partida de nacimiento', 'CV', 'Título', 'Domicilio',
            'AFIP', 'Foto', 'CBU', 'Certificado médico', 'Licencia de conducir', 'Contrato', 'Otros'
        ]

        if tipo not in tipos_validos:
            raise ValueError(f"Tipo de documento inválido: {tipo}")

        conn = None
        try:
            # Verificar existencia de empleado
            conn = db.get_connection()
            cur = conn.cursor()
            cur.execute("SELECT 1 FROM empleado WHERE id_empleado = %s", (empleado_id,))
            if not cur.fetchone():
                raise ValueError(f"No existe el empleado con ID {empleado_id}")

            # Subir a Cloudinary
            result = cloudinary.uploader.upload(
                io.BytesIO(contenido),
                resource_type="raw",
                folder=f"documentos_{tipo.lower()}",
                public_id=f"{empleado_id}_{tipo.lower()}_{datetime.now().strftime('%Y%m%d%H%M%S')}",
                use_filename=True,
                overwrite=False
            )
            url_archivo = result["secure_url"]

            # Insertar en tabla
            cur.execute("""
                INSERT INTO documento (id_empleado, tipo, archivo_asociado, descripcion)
                VALUES (%s, %s, %s, %s)
            """, (empleado_id, tipo, url_archivo, descripcion))
            conn.commit()

            return url_archivo

        except Exception as e:
            raise Exception(f"Error al guardar documento tipo {tipo}: {e}")
        finally:
            if conn:
                conn.close()

    @staticmethod
    def obtener_documento_tipo(empleado_id: int, tipo: str):
        with db.get_connection() as conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT id_documento, tipo, archivo_asociado, descripcion, fecha_subida
                FROM documento
                WHERE id_empleado = %s AND tipo = %s
                ORDER BY fecha_subida DESC
                LIMIT 1
            """, (empleado_id, tipo))
            row = cur.fetchone()

            if not row:
                raise ValueError(f"No se encontró documento tipo '{tipo}' para el empleado {empleado_id}")

            return {
                "id_documento": row[0],
                "tipo": row[1],
                "url": row[2],
                "descripcion": row[3],
                "fecha_subida": row[4].isoformat()
            }

    @staticmethod
    def tiene_vectores_faciales(id_empleado: int) -> bool:
        with db.get_connection() as conn:
            cur = conn.cursor()

            cur.execute("""
                SELECT tipo_vector
                FROM dato_biometrico_facial
                WHERE id_empleado = %s
            """, (id_empleado,))

            vectores = {row[0] for row in cur.fetchall()}

        return {'Neutro', 'Sonrisa', 'Giro'}.issubset(vectores)
    
#Localidades, partidos, paises y provincias
    @staticmethod
    def listar_paises():
        conn = None
        cur = None
        try:
            conn = db.get_connection()
            cur = conn.cursor()
            cur.execute("SELECT codigo_pais, nombre FROM pais ORDER BY nombre ASC")
            rows = cur.fetchall()
            return [{"codigo_pais": row[0], "nombre": row[1]} for row in rows]
        finally:
            if cur:
                cur.close()
            if conn:
                conn.close()

    @staticmethod
    def listar_provincias():
        conn = None
        cur = None
        try:
            conn = db.get_connection()
            cur = conn.cursor()
            cur.execute("SELECT codigo_provincia, nombre FROM provincia ORDER BY nombre ASC")
            rows = cur.fetchall()
            return [{"codigo_provincia": row[0], "nombre": row[1]} for row in rows]
        finally:
            if cur:
                cur.close()
            if conn:
                conn.close()

    @staticmethod
    def listar_localidades():
        conn = None
        cur = None
        try:
            conn = db.get_connection()
            cur = conn.cursor()
            cur.execute("SELECT codigo_localidad, codigo_provincia, nombre FROM localidad ORDER BY nombre ASC")
            rows = cur.fetchall()
            return [
                {"codigo_localidad": row[0], "codigo_provincia": row[1], "nombre": row[2]}
                for row in rows
            ]
        finally:
            if cur:
                cur.close()
            if conn:
                conn.close()
                
    @staticmethod
    def listar_partidos():
        conn = None
        cur = None
        try:
            conn = db.get_connection()
            cur = conn.cursor()
            cur.execute("SELECT codigo_partido, codigo_provincia, nombre FROM partido ORDER BY nombre ASC")
            rows = cur.fetchall()
            return [
                {"codigo_partido": row[0], "codigo_provincia": row[1], "nombre": row[2]}
                for row in rows
            ]
        finally:
            if cur:
                cur.close()
            if conn:
                conn.close()

    @staticmethod
    def listar_partidos_por_provincia(codigo_provincia: int = None):
        conn = None
        cur = None
        try:
            conn = db.get_connection()
            cur = conn.cursor()
            if codigo_provincia is None:
                cur.execute("SELECT codigo_partido, codigo_provincia, nombre FROM partido ORDER BY nombre")
            else:
                cur.execute(
                    "SELECT codigo_partido, codigo_provincia, nombre FROM partido WHERE codigo_provincia = %s ORDER BY nombre",
                    (codigo_provincia,)
                )
            filas = cur.fetchall()
            return [
                {
                    "codigo_partido": fila[0],
                    "codigo_provincia": fila[1],
                    "nombre": fila[2],
                }
                for fila in filas
            ]
        finally:
            if cur:
                cur.close()
            if conn:
                conn.close()

    @staticmethod
    def listar_localidades_por_provincia(codigo_provincia: int = None):
        conn = None
        cur = None
        try:
            conn = db.get_connection()
            cur = conn.cursor()
            if codigo_provincia is None:
                cur.execute(
                    "SELECT codigo_localidad, codigo_provincia, nombre FROM localidad ORDER BY nombre"
                )
            else:
                cur.execute(
                    "SELECT codigo_localidad, codigo_provincia, nombre FROM localidad WHERE codigo_provincia = %s ORDER BY nombre",
                    (codigo_provincia,),
                )
            filas = cur.fetchall()
            return [
                {
                    "codigo_localidad": fila[0],
                    "codigo_provincia": fila[1],
                    "nombre": fila[2],
                }
                for fila in filas
            ]
        finally:
            if cur:
                cur.close()
            if conn:
                conn.close()
                
#Listar departamentos, categorias, puestos
    @staticmethod
    def listar_departamentos():
        conn = None
        cur = None
        try:
            conn = db.get_connection()
            cur = conn.cursor()
            cur.execute("SELECT id_departamento, nombre, descripcion FROM departamento ORDER BY id_departamento ASC")
            rows = cur.fetchall()
            return [{"id_departamento": row[0], "nombre": row[1], "descripcion": row[2]} for row in rows]
        finally:
            if cur:
                cur.close()
            if conn:
                conn.close()
                
    @staticmethod
    def listar_categorias():
        conn = None
        cur = None
        try:
            conn = db.get_connection()
            cur = conn.cursor()
            cur.execute("SELECT id_categoria, nombre_categoria FROM categoria ORDER BY id_categoria ASC")
            rows = cur.fetchall()
            return [{"id_categoria": row[0], "nombre_categoria": row[1]} for row in rows]
        finally:
            if cur:
                cur.close()
            if conn:
                conn.close()
                
    @staticmethod
    def listar_puestos():
        conn = None
        cur = None
        try:
            conn = db.get_connection()
            cur = conn.cursor()
            cur.execute("SELECT id_puesto, nombre FROM puesto ORDER BY id_puesto ASC")
            rows = cur.fetchall()
            return [{"id_puesto": row[0], "nombre": row[1]} for row in rows]
        finally:
            if cur:
                cur.close()
            if conn:
                conn.close()

    @staticmethod
    def buscar_informacion_laboral_completa_por_id_empleado(id_empleado: int):
        try:
            conn = db.get_connection()
            cur = conn.cursor()
            query = """
                SELECT 
                    il.id_departamento,
                    il.id_puesto,
                    il.id_categoria,
                    il.fecha_ingreso,
                    il.turno,
                    il.hora_inicio_turno,
                    il.hora_fin_turno,
                    il.cantidad_horas_trabajo,
                    il.tipo_contrato,
                    il.estado,
                    il.tipo_semana_laboral
                FROM informacion_laboral il
                WHERE il.id_empleado = %s
                ORDER BY il.fecha_ingreso DESC
                LIMIT 1
            """
            cur.execute(query, (id_empleado,))
            return cur.fetchone()

        except Exception as e:
            print(f"Error al buscar información laboral completa: {str(e)}")
            raise ValueError(f"No se pudo obtener la información laboral completa: {str(e)}")
        finally:
            if conn:
                db.return_connection(conn)

    @staticmethod
    def obtener_periodos_unicos():
        conn = None
        cur = None
        try:
            conn = db.get_connection()
            cur = conn.cursor()
            cur.execute("""
                SELECT DISTINCT periodo_texto
                FROM periodo_empleado
                ORDER BY periodo_texto
            """)
            rows = cur.fetchall()
            return [row[0] for row in rows]
        finally:
            if cur:
                cur.close()
            if conn:
                conn.close()