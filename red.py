import requests
import json
import threading
import websocket
from modelo import Vuelo

SERVER_URL = "https://TU-SERVIDOR.railway.app"
AEROPUERTO  = "SAZM"

_callback_actualizacion = None

def set_callback(fn):
    global _callback_actualizacion
    _callback_actualizacion = fn

def _vuelo_a_dict(vuelo: Vuelo) -> dict:
    d = vuelo.to_dict()
    if isinstance(d.get('plan_de_vuelo'), dict):
        d['plan_de_vuelo'] = json.dumps(d['plan_de_vuelo'], ensure_ascii=False)
    return d

def _dict_a_vuelo(d: dict) -> Vuelo:
    if isinstance(d.get('plan_de_vuelo'), str):
        try:
            d['plan_de_vuelo'] = json.loads(d['plan_de_vuelo'])
        except Exception:
            d['plan_de_vuelo'] = {}
    return Vuelo.from_dict(d)

def cargar():
    try:
        r = requests.get(f"{SERVER_URL}/vuelos/{AEROPUERTO}", timeout=5)
        r.raise_for_status()
        return [_dict_a_vuelo(v) for v in r.json()]
    except Exception as e:
        print(f"Error cargando vuelos: {e}")
        return []

def crear(vuelo: Vuelo):
    try:
        r = requests.post(f"{SERVER_URL}/vuelos/{AEROPUERTO}", json=_vuelo_a_dict(vuelo), timeout=5)
        r.raise_for_status()
        return _dict_a_vuelo(r.json())
    except Exception as e:
        print(f"Error creando vuelo: {e}")
        return None

def actualizar(vuelo: Vuelo):
    try:
        r = requests.put(f"{SERVER_URL}/vuelos/{AEROPUERTO}/{vuelo.id}", json=_vuelo_a_dict(vuelo), timeout=5)
        r.raise_for_status()
        return True
    except Exception as e:
        print(f"Error actualizando vuelo: {e}")
        return False

def eliminar(vuelo_id: str):
    try:
        r = requests.delete(f"{SERVER_URL}/vuelos/{AEROPUERTO}/{vuelo_id}", timeout=5)
        r.raise_for_status()
        return True
    except Exception as e:
        print(f"Error eliminando vuelo: {e}")
        return False

def _iniciar_websocket():
    ws_url = SERVER_URL.replace("https://", "wss://").replace("http://", "ws://")
    ws_url = f"{ws_url}/ws/{AEROPUERTO}"

    def on_message(ws, message):
        if _callback_actualizacion:
            data = json.loads(message)
            if 'vuelo' in data and isinstance(data['vuelo'].get('plan_de_vuelo'), str):
                try:
                    data['vuelo']['plan_de_vuelo'] = json.loads(data['vuelo']['plan_de_vuelo'])
                except Exception:
                    data['vuelo']['plan_de_vuelo'] = {}
            _callback_actualizacion(data)

    def on_error(ws, error):
        print(f"WebSocket error: {error}")

    def on_close(ws, *args):
        print("WebSocket cerrado, reconectando en 5s...")
        threading.Timer(5, _iniciar_websocket).start()

    def on_open(ws):
        print("WebSocket conectado")

    ws = websocket.WebSocketApp(ws_url, on_message=on_message,
        on_error=on_error, on_close=on_close, on_open=on_open)
    ws.run_forever()

def conectar_tiempo_real():
    t = threading.Thread(target=_iniciar_websocket, daemon=True)
    t.start()
