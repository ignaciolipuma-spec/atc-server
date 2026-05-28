from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Depends, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from typing import Optional
from jose import JWTError, jwt
from passlib.context import CryptContext
from datetime import datetime, timedelta, timezone
import asyncpg
import json
import os
import uuid

DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL no configurada")

SECRET_KEY   = os.environ.get("SECRET_KEY", "atc-strips-clave-secreta-2024")
ALGORITHM    = "HS256"
TOKEN_EXPIRE = 480

app = FastAPI(title="ATC Strips Server")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])
pwd_ctx       = CryptContext(schemes=["bcrypt"], deprecated="auto")
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")
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
            CREATE TABLE IF NOT EXISTS usuarios (
                id TEXT PRIMARY KEY,
                username TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                nombre TEXT,
                aeropuerto TEXT NOT NULL,
                rol TEXT DEFAULT 'operador',
                activo BOOLEAN DEFAULT TRUE,
                creado TEXT
            )
        """)
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
        try:
            await conn.execute("ALTER TABLE vuelos ADD COLUMN IF NOT EXISTS plan_de_vuelo TEXT DEFAULT '{}'")
        except Exception:
            pass
        existe = await conn.fetchval("SELECT COUNT(*) FROM usuarios WHERE username='admin'")
        if existe == 0:
            await conn.execute("""
                INSERT INTO usuarios (id,username,password_hash,nombre,aeropuerto,rol,activo,creado)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
            """, str(uuid.uuid4()), "admin", pwd_ctx.hash("admin123"),
                "Administrador", "SAZM", "admin", True,
                datetime.now().strftime("%d/%m/%Y %H:%M"))

def crear_token(data: dict):
    to_encode = data.copy()
    to_encode["exp"] = datetime.now(timezone.utc) + timedelta(minutes=TOKEN_EXPIRE)
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

async def get_usuario_actual(token: str = Depends(oauth2_scheme)):
    exc = HTTPException(status_code=401, detail="Token inválido", headers={"WWW-Authenticate": "Bearer"})
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username = payload.get("sub")
        if not username:
            raise exc
    except JWTError:
        raise exc
    async with pool.acquire() as conn:
        user = await conn.fetchrow("SELECT * FROM usuarios WHERE username=$1 AND activo=TRUE", username)
    if not user:
        raise exc
    return dict(user)

async def require_admin(u=Depends(get_usuario_actual)):
    if u["rol"] != "admin":
        raise HTTPException(status_code=403, detail="Se requiere rol administrador")
    return u

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

class UsuarioCreate(BaseModel):
    username: str
    password: str
    nombre: Optional[str] = ""
    aeropuerto: str
    rol: Optional[str] = "operador"

class UsuarioUpdate(BaseModel):
    nombre: Optional[str] = None
    password: Optional[str] = None
    aeropuerto: Optional[str] = None
    rol: Optional[str] = None
    activo: Optional[bool] = None

class CambiarPassword(BaseModel):
    password_actual: str
    password_nuevo: str

class ConnectionManager:
    def __init__(self):
        self.connections: dict = {}
    async def connect(self, ws, aeropuerto):
        await ws.accept()
        self.connections.setdefault(aeropuerto, []).append(ws)
    def disconnect(self, ws, aeropuerto):
        if aeropuerto in self.connections:
            try: self.connections[aeropuerto].remove(ws)
            except ValueError: pass
    async def broadcast(self, aeropuerto, mensaje):
        caidos = []
        for ws in self.connections.get(aeropuerto, []):
            try: await ws.send_text(json.dumps(mensaje))
            except: caidos.append(ws)
        for ws in caidos:
            try: self.connections[aeropuerto].remove(ws)
            except: pass

manager = ConnectionManager()

@app.get("/")
async def root():
    return {"status": "ATC Strips Server online"}

@app.post("/token")
async def login(form: OAuth2PasswordRequestForm = Depends()):
    async with pool.acquire() as conn:
        user = await conn.fetchrow("SELECT * FROM usuarios WHERE username=$1 AND activo=TRUE", form.username)
    if not user or not pwd_ctx.verify(form.password, user["password_hash"]):
        raise HTTPException(status_code=401, detail="Usuario o contraseña incorrectos")
    token = crear_token({"sub": user["username"], "aeropuerto": user["aeropuerto"], "rol": user["rol"]})
    return {"access_token": token, "token_type": "bearer",
            "aeropuerto": user["aeropuerto"], "rol": user["rol"],
            "nombre": user["nombre"], "username": user["username"]}

@app.get("/me")
async def me(u=Depends(get_usuario_actual)):
    return {k: v for k, v in u.items() if k != "password_hash"}

@app.post("/me/password")
async def cambiar_password(datos: CambiarPassword, u=Depends(get_usuario_actual)):
    if not pwd_ctx.verify(datos.password_actual, u["password_hash"]):
        raise HTTPException(status_code=400, detail="Contraseña actual incorrecta")
    async with pool.acquire() as conn:
        await conn.execute("UPDATE usuarios SET password_hash=$1 WHERE username=$2",
                           pwd_ctx.hash(datos.password_nuevo), u["username"])
    return {"ok": True}

@app.get("/usuarios")
async def listar_usuarios(admin=Depends(require_admin)):
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT id,username,nombre,aeropuerto,rol,activo,creado FROM usuarios ORDER BY creado DESC")
    return [dict(r) for r in rows]

@app.post("/usuarios")
async def crear_usuario(datos: UsuarioCreate, admin=Depends(require_admin)):
    uid = str(uuid.uuid4())
    try:
        async with pool.acquire() as conn:
            await conn.execute("""INSERT INTO usuarios (id,username,password_hash,nombre,aeropuerto,rol,activo,creado)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8)""",
                uid, datos.username, pwd_ctx.hash(datos.password), datos.nombre,
                datos.aeropuerto, datos.rol, True, datetime.now().strftime("%d/%m/%Y %H:%M"))
    except asyncpg.UniqueViolationError:
        raise HTTPException(status_code=400, detail="El usuario ya existe")
    return {"ok": True, "id": uid}

@app.put("/usuarios/{uid}")
async def actualizar_usuario(uid: str, datos: UsuarioUpdate, admin=Depends(require_admin)):
    async with pool.acquire() as conn:
        if datos.password: await conn.execute("UPDATE usuarios SET password_hash=$1 WHERE id=$2", pwd_ctx.hash(datos.password), uid)
        if datos.nombre is not None: await conn.execute("UPDATE usuarios SET nombre=$1 WHERE id=$2", datos.nombre, uid)
        if datos.aeropuerto is not None: await conn.execute("UPDATE usuarios SET aeropuerto=$1 WHERE id=$2", datos.aeropuerto, uid)
        if datos.rol is not None: await conn.execute("UPDATE usuarios SET rol=$1 WHERE id=$2", datos.rol, uid)
        if datos.activo is not None: await conn.execute("UPDATE usuarios SET activo=$1 WHERE id=$2", datos.activo, uid)
    return {"ok": True}

@app.delete("/usuarios/{uid}")
async def eliminar_usuario(uid: str, admin=Depends(require_admin)):
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM usuarios WHERE id=$1", uid)
    return {"ok": True}

@app.get("/vuelos/{aeropuerto}")
async def get_vuelos(aeropuerto: str, u=Depends(get_usuario_actual)):
    if u["rol"] != "admin" and u["aeropuerto"] != aeropuerto:
        raise HTTPException(status_code=403, detail="Sin acceso a este aeropuerto")
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM vuelos WHERE aeropuerto=$1 ORDER BY timestamp ASC", aeropuerto)
    return [dict(r) for r in rows]

@app.post("/vuelos/{aeropuerto}")
async def crear_vuelo(aeropuerto: str, vuelo: Vuelo, u=Depends(get_usuario_actual)):
    if u["rol"] != "admin" and u["aeropuerto"] != aeropuerto:
        raise HTTPException(status_code=403, detail="Sin acceso a este aeropuerto")
    vuelo.id = str(uuid.uuid4())
    vuelo.timestamp = datetime.now().strftime("%d/%m %H:%M")
    vuelo.aeropuerto = aeropuerto
    async with pool.acquire() as conn:
        await conn.execute("""INSERT INTO vuelos
            (id,tipo_movimiento,matricula,tipo_aeronave,origen,destino,eta_ten,hora_salida,
             indicadores,observaciones,completado,timestamp,aeropuerto,plan_de_vuelo)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)""",
            vuelo.id, vuelo.tipo_movimiento, vuelo.matricula, vuelo.tipo_aeronave,
            vuelo.origen, vuelo.destino, vuelo.eta_ten, vuelo.hora_salida,
            vuelo.indicadores, vuelo.observaciones, vuelo.completado,
            vuelo.timestamp, aeropuerto, vuelo.plan_de_vuelo or '{}')
    await manager.broadcast(aeropuerto, {"accion": "crear", "vuelo": vuelo.model_dump()})
    return vuelo

@app.put("/vuelos/{aeropuerto}/{vuelo_id}")
async def actualizar_vuelo(aeropuerto: str, vuelo_id: str, vuelo: Vuelo, u=Depends(get_usuario_actual)):
    if u["rol"] != "admin" and u["aeropuerto"] != aeropuerto:
        raise HTTPException(status_code=403, detail="Sin acceso a este aeropuerto")
    async with pool.acquire() as conn:
        await conn.execute("""UPDATE vuelos SET
            tipo_movimiento=$1,matricula=$2,tipo_aeronave=$3,origen=$4,destino=$5,
            eta_ten=$6,hora_salida=$7,indicadores=$8,observaciones=$9,completado=$10,plan_de_vuelo=$11
            WHERE id=$12 AND aeropuerto=$13""",
            vuelo.tipo_movimiento, vuelo.matricula, vuelo.tipo_aeronave,
            vuelo.origen, vuelo.destino, vuelo.eta_ten, vuelo.hora_salida,
            vuelo.indicadores, vuelo.observaciones, vuelo.completado,
            vuelo.plan_de_vuelo or '{}', vuelo_id, aeropuerto)
    vuelo.id = vuelo_id
    await manager.broadcast(aeropuerto, {"accion": "actualizar", "vuelo": vuelo.model_dump()})
    return vuelo

@app.delete("/vuelos/{aeropuerto}/{vuelo_id}")
async def eliminar_vuelo(aeropuerto: str, vuelo_id: str, u=Depends(get_usuario_actual)):
    if u["rol"] != "admin" and u["aeropuerto"] != aeropuerto:
        raise HTTPException(status_code=403, detail="Sin acceso a este aeropuerto")
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM vuelos WHERE id=$1 AND aeropuerto=$2", vuelo_id, aeropuerto)
    await manager.broadcast(aeropuerto, {"accion": "eliminar", "id": vuelo_id})
    return {"ok": True}

@app.websocket("/ws/{aeropuerto}")
async def websocket_endpoint(ws: WebSocket, aeropuerto: str):
    await manager.connect(ws, aeropuerto)
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(ws, aeropuerto)

@app.get("/admin", response_class=HTMLResponse)
async def panel_admin():
    html_path = os.path.join(os.path.dirname(__file__), "admin.html")
    if os.path.exists(html_path):
        return HTMLResponse(content=open(html_path, encoding="utf-8").read())
    return HTMLResponse(content="<h1>Panel no disponible</h1>")
