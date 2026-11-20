"""Agente de reposición de RetailSmart (IA generativa con herramientas).

Gemini (Vertex AI) responde preguntas del negocio llamando a herramientas fijas que consultan las
tablas de la zona Smart en BigQuery. El modelo NO escribe SQL: solo elige una herramienta y sus
parámetros; cada herramienta ejecuta una consulta parametrizada y de solo lectura.

Reglas del agente (también están en las instrucciones del sistema):
  * solo afirma lo que devuelven las herramientas; si no hay dato, lo dice;
  * respeta el estado 'sin_dato' del plan (stock de más de 24 h) y no inventa una cifra;
  * no suma montos de monedas distintas;
  * indica de dónde salió cada cifra (herramienta usada).

Variables de entorno: PROYECTO, REGION_IA (global), MODELO_IA (gemini-3.5-flash).
"""
import os
import time
from collections import deque
from datetime import date, datetime
from decimal import Decimal
from typing import Optional

PROYECTO = os.environ.get("PROYECTO", "retailsmart-bd")
DS = f"{PROYECTO}.retailsmart_smart"
REGION_IA = os.environ.get("REGION_IA", "global")
MODELO_IA = os.environ.get("MODELO_IA", "gemini-3.5-flash")
MAX_PREGUNTA = 500
MAX_LLAMADAS_HERRAMIENTAS = 6
LIMITE_POR_MINUTO = 20

INSTRUCCIONES = """Eres el asistente de reposición de RetailSmart Global (Lumina Retail).
Respondes en español, de forma breve y directa, a gerentes de operaciones y de inventario.

Reglas:
1. Usa SIEMPRE las herramientas para obtener datos. No uses conocimiento propio para cifras.
2. Si una herramienta no devuelve filas o falla, dilo con claridad. No inventes valores.
3. En el plan de reposición, el estado 'sin_dato' significa que el stock tiene más de 24 horas:
   no hay recomendación para esa fila. Nunca la reemplaces con un número.
4. Los montos de ventas están en su moneda de origen (BRL, GBP, USD). No los sumes entre monedas.
5. El pronóstico es una estimación con intervalo de confianza del 90 %, no una promesa.
   Si te preguntan por su calidad, usa la herramienta de metodología y reporta los números tal cual,
   incluso si no son favorables.
6. Al final indica entre paréntesis qué herramientas usaste.
7. No ejecutes acciones (pedidos, cambios de stock): solo informas y recomiendas.
8. No sumes ni cuentes por tu cuenta cuando exista una herramienta que entregue el total
   (resumen_reposicion por centro, reposicion_por_categoria por categoría). Si plan_reposicion indica
   truncado=true, la lista está incompleta: dilo y no calcules totales con ella.
9. Si la pregunta no tiene que ver con el negocio de RetailSmart, responde que solo puedes ayudar
   con reposición, pronóstico, alertas de frío, ventas y tiempo real."""

METODOLOGIA = {
    "pronostico": "Un modelo ARIMA_PLUS de BigQuery ML por centro, entrenado con datos hasta el 10 de "
                  "octubre de 2026, con horizonte de 14 días y 90 % de confianza.",
    "validacion": "Se ocultaron días y se comparó con una línea base ingenua (promedio de los 28 días "
                  "anteriores). WAPE del modelo 0,238 frente a 0,209 de la línea base: el modelo NO "
                  "supera a la línea base y no cumple el objetivo de calidad.",
    "evento_atipico": "Del 11 al 18 de octubre de 2026 la demanda diaria subió de unas 350 unidades a "
                      "1.774 (16 de oct). Se trató como un evento atípico de la fuente y se excluyó del "
                      "entrenamiento y de la desviación estándar.",
    "plan_reposicion": "reposición = máx(0, techo(demanda prevista en el plazo + stock de seguridad - "
                       "disponible)); stock de seguridad = 1,65 x sigma x raíz(plazo) x participación de la "
                       "categoría; sigma de los últimos 90 días sin el evento. Si el stock tiene más de "
                       "24 h, la fila queda 'sin_dato'.",
    "alertas_frio": "Tres reglas: temperatura media de la cámara en la ventana superior a 8 °C; cámara "
                    "sin datos en la ventana; lecturas imposibles (fuera de -30 a 60 °C) que se descartan y "
                    "se registran como error.",
}


def _centro(valor):
    return None if valor is None else str(valor)


def _json_seguro(valor):
    """Fechas y decimales de BigQuery a tipos que el SDK del modelo pueda serializar a JSON."""
    if isinstance(valor, (datetime, date)):
        return valor.isoformat()
    if isinstance(valor, Decimal):
        return float(valor)
    return valor


def construir_herramientas(consultar, texto, entero, traza):
    """Crea las herramientas que verá el modelo. `traza` registra cada llamada."""
    from google.api_core.exceptions import NotFound

    def registrar(nombre, **args):
        traza.append({"herramienta": nombre, "argumentos": {k: v for k, v in args.items() if v is not None}})

    def ejecutar(sql, params):
        try:
            filas = consultar(sql, params)
        except NotFound:
            return []
        return [{k: _json_seguro(v) for k, v in fila.items()} for fila in filas]

    def plan_reposicion(centro_id: Optional[str] = None, categoria: Optional[str] = None,
                        limite: int = 10) -> dict:
        """Plan de reposición sugerido por centro y categoría, de mayor a menor cantidad.
        Filtros opcionales: centro_id (por ejemplo '3') y categoria. Las filas con estado
        'sin_dato' no tienen recomendación porque el stock tiene más de 24 horas. Si 'truncado'
        es verdadero, la lista es parcial: no la uses para sumar totales."""
        limite = max(1, min(int(limite), 30))
        registrar("plan_reposicion", centro_id=centro_id, categoria=categoria, limite=limite)
        sql = f"""
        SELECT centro_id, categoria, disponible, plazo_dias, demanda_en_plazo, stock_seguridad,
               estado, reposicion_sugerida, COUNT(*) OVER() AS filas_totales
        FROM `{DS}.plan_reposicion`
        WHERE (@centro IS NULL OR CAST(centro_id AS STRING) = @centro)
          AND (@categoria IS NULL OR LOWER(categoria) = LOWER(@categoria))
        ORDER BY reposicion_sugerida DESC, centro_id, categoria
        LIMIT @limite"""
        filas = ejecutar(sql, [texto("centro", _centro(centro_id)), texto("categoria", categoria),
                               entero("limite", limite)])
        total = filas[0].get("filas_totales", len(filas)) if filas else 0
        return {"filas": filas, "filas_devueltas": len(filas), "filas_totales": total,
                "truncado": total > len(filas)}

    def resumen_reposicion() -> dict:
        """Resumen por centro: categorías a reponer, unidades sugeridas y filas sin dato."""
        registrar("resumen_reposicion")
        sql = f"""
        SELECT centro_id, COUNTIF(reposicion_sugerida > 0) AS categorias_a_reponer,
               SUM(reposicion_sugerida) AS unidades_sugeridas, COUNTIF(estado = 'sin_dato') AS sin_dato
        FROM `{DS}.plan_reposicion`
        GROUP BY centro_id ORDER BY unidades_sugeridas DESC"""
        return {"filas": ejecutar(sql, [])}

    def reposicion_por_categoria(limite: int = 10) -> dict:
        """Unidades de reposición sugeridas por categoría, sumadas en todos los centros, con el
        número de centros que deben reponerla. Úsala para totales por categoría."""
        limite = max(1, min(int(limite), 30))
        registrar("reposicion_por_categoria", limite=limite)
        sql = f"""
        SELECT categoria, SUM(reposicion_sugerida) AS unidades_sugeridas,
               COUNTIF(reposicion_sugerida > 0) AS centros_a_reponer
        FROM `{DS}.plan_reposicion`
        GROUP BY categoria HAVING SUM(reposicion_sugerida) > 0
        ORDER BY unidades_sugeridas DESC, categoria
        LIMIT @limite"""
        return {"filas": ejecutar(sql, [entero("limite", limite)])}

    def pronostico_demanda(centro_id: Optional[str] = None) -> dict:
        """Pronóstico diario de demanda en unidades (14 días) con intervalo de confianza del 90 %.
        Sin centro_id devuelve el total de todos los centros por día."""
        registrar("pronostico_demanda", centro_id=centro_id)
        sql = f"""
        SELECT dia, ROUND(SUM(unidades_previstas), 1) AS unidades_previstas,
               ROUND(SUM(inferior), 1) AS inferior, ROUND(SUM(superior), 1) AS superior
        FROM `{DS}.pronostico_demanda`
        WHERE (@centro IS NULL OR CAST(centro_id AS STRING) = @centro)
        GROUP BY dia ORDER BY dia"""
        return {"filas": ejecutar(sql, [texto("centro", _centro(centro_id))])}

    def alertas_frio(minutos: int = 60, centro_id: Optional[str] = None) -> dict:
        """Alertas de cadena de frío recientes (sobretemperatura o cámara sin datos) de los
        últimos `minutos` minutos, opcionalmente de un centro."""
        minutos = max(1, min(int(minutos), 1440))
        registrar("alertas_frio", minutos=minutos, centro_id=centro_id)
        sql = f"""
        SELECT ventana_inicio, camara_id, centro_id, regla, ROUND(temp_media, 1) AS temp_media,
               ROUND(temp_max, 1) AS temp_max
        FROM `{DS}.rt_alertas_frio`
        WHERE ventana_inicio >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @minutos MINUTE)
          AND (@centro IS NULL OR CAST(centro_id AS STRING) = @centro)
        ORDER BY ventana_inicio DESC
        LIMIT 30"""
        return {"filas": ejecutar(sql, [entero("minutos", minutos), texto("centro", _centro(centro_id))])}

    def ventas_por_unidad(moneda: Optional[str] = None, limite: int = 10) -> dict:
        """Líneas y monto de venta por unidad de negocio y moneda (BRL, GBP o USD).
        Los montos de monedas distintas no se suman."""
        limite = max(1, min(int(limite), 30))
        registrar("ventas_por_unidad", moneda=moneda, limite=limite)
        sql = f"""
        SELECT p.unidad_negocio, f.moneda, COUNT(*) AS lineas, ROUND(SUM(f.monto), 2) AS monto
        FROM `{DS}.fact_ventas` f
        JOIN `{DS}.dim_producto` p ON f.producto_sk = p.producto_sk
        WHERE (@moneda IS NULL OR f.moneda = @moneda)
        GROUP BY p.unidad_negocio, f.moneda
        ORDER BY monto DESC
        LIMIT @limite"""
        m = moneda.upper() if moneda else None
        return {"filas": ejecutar(sql, [texto("moneda", m), entero("limite", limite)])}

    def ventas_por_region(moneda: Optional[str] = None) -> dict:
        """Líneas y monto de venta por región logística y moneda (BRL, GBP o USD)."""
        registrar("ventas_por_region", moneda=moneda)
        sql = f"""
        SELECT g.region, f.moneda, COUNT(*) AS lineas, ROUND(SUM(f.monto), 2) AS monto
        FROM `{DS}.fact_ventas` f
        JOIN `{DS}.dim_geo` g ON f.geo_sk = g.geo_sk
        WHERE (@moneda IS NULL OR f.moneda = @moneda)
        GROUP BY g.region, f.moneda
        ORDER BY lineas DESC"""
        m = moneda.upper() if moneda else None
        return {"filas": ejecutar(sql, [texto("moneda", m)])}

    def eventos_tiempo_real(minutos: int = 5) -> dict:
        """Eventos de la tienda en línea por tipo en los últimos `minutos` minutos
        (agregados del pipeline de streaming)."""
        minutos = max(1, min(int(minutos), 1440))
        registrar("eventos_tiempo_real", minutos=minutos)
        sql = f"""
        SELECT event_type, SUM(eventos) AS eventos
        FROM `{DS}.rt_eventos_ventana`
        WHERE ventana_inicio >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @minutos MINUTE)
        GROUP BY event_type ORDER BY eventos DESC"""
        return {"filas": ejecutar(sql, [entero("minutos", minutos)])}

    def metodologia(tema: str = "pronostico") -> dict:
        """Explica cómo se calculan el pronóstico, su validación, el plan de reposición y las
        alertas de frío, y el evento atípico de octubre. tema: pronostico, validacion,
        evento_atipico, plan_reposicion o alertas_frio."""
        registrar("metodologia", tema=tema)
        if tema in METODOLOGIA:
            return {"tema": tema, "texto": METODOLOGIA[tema]}
        return {"temas_disponibles": list(METODOLOGIA), "texto": METODOLOGIA["validacion"]}

    return [plan_reposicion, resumen_reposicion, reposicion_por_categoria, pronostico_demanda, alertas_frio,
            ventas_por_unidad, ventas_por_region, eventos_tiempo_real, metodologia]


class LimiteDeUso:
    """Máximo de preguntas por minuto (protege el costo del modelo)."""

    def __init__(self, maximo=LIMITE_POR_MINUTO, reloj=time.monotonic):
        self.maximo, self.reloj, self.marcas = maximo, reloj, deque()

    def permitir(self):
        ahora = self.reloj()
        while self.marcas and ahora - self.marcas[0] > 60:
            self.marcas.popleft()
        if len(self.marcas) >= self.maximo:
            return False
        self.marcas.append(ahora)
        return True


def _cliente_gemini():
    from google import genai
    return genai.Client(vertexai=True, project=PROYECTO, location=REGION_IA)


def responder(pregunta, consultar, texto, entero, cliente=None):
    """Devuelve {'respuesta': str, 'herramientas': [...]} para una pregunta en lenguaje natural."""
    from google.genai import types

    pregunta = (pregunta or "").strip()
    if not pregunta:
        raise ValueError("La pregunta está vacía.")
    if len(pregunta) > MAX_PREGUNTA:
        raise ValueError(f"La pregunta supera {MAX_PREGUNTA} caracteres.")

    traza = []
    herramientas = construir_herramientas(consultar, texto, entero, traza)
    cliente = cliente or _cliente_gemini()
    config = types.GenerateContentConfig(
        system_instruction=INSTRUCCIONES,
        tools=herramientas,
        temperature=0.2,
        max_output_tokens=2048,
        automatic_function_calling=types.AutomaticFunctionCallingConfig(
            maximum_remote_calls=MAX_LLAMADAS_HERRAMIENTAS),
    )
    resp = cliente.models.generate_content(model=MODELO_IA, contents=pregunta, config=config)
    return {"respuesta": (resp.text or "No obtuve una respuesta del modelo.").strip(),
            "herramientas": traza, "modelo": MODELO_IA}
