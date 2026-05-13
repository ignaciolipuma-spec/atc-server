from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, List
import asyncpg
import json
import os
import uuid
from datetime import datetime

# ── Configuración ─────────────────────────────────────────────────────────────
DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://postgres:bVxVzQkPBLTPVNFtQUpeuEopUmcKZXsX@yamabiko.proxy.rlwy.net:44651/railway"
)

app = FastAPI(title="ATC Strips Server")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Pool de conexiones ────────────────────────────────────────────────────────
pool = None

@app.on_event("startup")
async def startup():
    global pool
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=10)
    await init_db()

@app.on_event("shutdown")
async def shutdown():
    await pool.close()

async def init_db():
    async with pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS vuelos (
                id TEXT PRIMARY KEY,
                tipo_movimiento TEXT NOT NULL,
                matricula TEXT NOT NULL,
                tipo_aeronave TEXT,
                origen TEXT,
                destino TEXT,
                eta_ten TEXT,
                hora_salida TEXT,
                indicadores TEXT,
                observaciones TEXT,
                completado BOOLEAN DEFAULT FALSE,
                timestamp TEXT,
                aeropuerto TEXT DEFAULT 'SAZM',
                plan_de_vuelo TEXT DEFAULT '{}'
            )
        """)
        # Agregar columna plan_de_vuelo si no existe (para bases ya creadas)
        try:
            await conn.execute("ALTER TABLE vuelos ADD COLUMN IF NOT EXISTS plan_de_vuelo TEXT DEFAULT '{}'")
        except Exception:
            pass

# ── Manager de WebSockets ─────────────────────────────────────────────────────
class ConnectionManager:
    def __init__(self):
        self.connections: dict[str, list[WebSocket]] = {}

    async def connect(self, ws: WebSocket, aeropuerto: str):
        await ws.accept()
        if aeropuerto not in self.connections:
            self.connections[aeropuerto] = []
        self.connections[aeropuerto].append(ws)

    def disconnect(self, ws: WebSocket, aeropuerto: str):
        if aeropuerto in self.connections:
            self.connections[aeropuerto].remove(ws)

    async def broadcast(self, aeropuerto: str, mensaje: dict):
        if aeropuerto in self.connections:
            caidos = []
            for ws in self.connections[aeropuerto]:
                try:
                    await ws.send_text(json.dumps(mensaje))
                except Exception:
                    caidos.append(ws)
            for ws in caidos:
                self.connections[aeropuerto].remove(ws)

manager = ConnectionManager()

# ── Modelos Pydantic ──────────────────────────────────────────────────────────
class Vuelo(BaseModel):
    id: Optional[str] = None
    tipo_movimiento: str
    matricula: str
    tipo_aeronave: Optional[str] = ""
    origen: Optional[str] = ""
    destino: Optional[str] = ""
    eta_ten: Optional[str] = ""
    hora_salida: Optional[str] = ""
    indicadores: Optional[str] = ""
    observaciones: Optional[str] = ""
    completado: Optional[bool] = False
    timestamp: Optional[str] = None
    aeropuerto: Optional[str] = "SAZM"
    plan_de_vuelo: Optional[str] = "{}"  

# ── Endpoints REST ────────────────────────────────────────────────────────────
@app.get("/")
async def root():
    return {"status": "ATC Strips Server online"}

@app.get("/vuelos/{aeropuerto}")
async def get_vuelos(aeropuerto: str):
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM vuelos WHERE aeropuerto=$1 ORDER BY timestamp ASC",
            aeropuerto
        )
        return [dict(r) for r in rows]

@app.post("/vuelos/{aeropuerto}")
async def crear_vuelo(aeropuerto: str, vuelo: Vuelo):
    vuelo.id = str(uuid.uuid4())
    vuelo.timestamp = datetime.now().strftime("%d/%m %H:%M")
    vuelo.aeropuerto = aeropuerto

    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO vuelos
            (id, tipo_movimiento, matricula, tipo_aeronave, origen, destino,
             eta_ten, hora_salida, indicadores, observaciones, completado, timestamp, aeropuerto, plan_de_vuelo)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
        """, vuelo.id, vuelo.tipo_movimiento, vuelo.matricula, vuelo.tipo_aeronave,
            vuelo.origen, vuelo.destino, vuelo.eta_ten, vuelo.hora_salida,
            vuelo.indicadores, vuelo.observaciones, vuelo.completado,
            vuelo.timestamp, aeropuerto, vuelo.plan_de_vuelo or '{}')

    await manager.broadcast(aeropuerto, {"accion": "crear", "vuelo": vuelo.dict()})
    return vuelo

@app.put("/vuelos/{aeropuerto}/{vuelo_id}")
async def actualizar_vuelo(aeropuerto: str, vuelo_id: str, vuelo: Vuelo):
    async with pool.acquire() as conn:
        resultado = await conn.execute("""
            UPDATE vuelos SET
                tipo_movimiento=$1, matricula=$2, tipo_aeronave=$3,
                origen=$4, destino=$5, eta_ten=$6, hora_salida=$7,
                indicadores=$8, observaciones=$9, completado=$10,
                plan_de_vuelo=$11
            WHERE id=$12 AND aeropuerto=$13
        """, vuelo.tipo_movimiento, vuelo.matricula, vuelo.tipo_aeronave,
            vuelo.origen, vuelo.destino, vuelo.eta_ten, vuelo.hora_salida,
            vuelo.indicadores, vuelo.observaciones, vuelo.completado,
            vuelo.plan_de_vuelo or '{}', vuelo_id, aeropuerto)

        if resultado == "UPDATE 0":
            raise HTTPException(status_code=404, detail="Vuelo no encontrado")

    vuelo.id = vuelo_id
    await manager.broadcast(aeropuerto, {"accion": "actualizar", "vuelo": vuelo.dict()})
    return vuelo

@app.delete("/vuelos/{aeropuerto}/{vuelo_id}")
async def eliminar_vuelo(aeropuerto: str, vuelo_id: str):
    async with pool.acquire() as conn:
        await conn.execute(
            "DELETE FROM vuelos WHERE id=$1 AND aeropuerto=$2",
            vuelo_id, aeropuerto
        )
    await manager.broadcast(aeropuerto, {"accion": "eliminar", "id": vuelo_id})
    return {"ok": True}

# ── WebSocket para actualizaciones en tiempo real ─────────────────────────────
@app.websocket("/ws/{aeropuerto}")
async def websocket_endpoint(ws: WebSocket, aeropuerto: str):
    await manager.connect(ws, aeropuerto)
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(ws, aeropuerto)
