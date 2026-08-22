# web_server.py
from pathlib import Path

import uvicorn
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

app = FastAPI()
BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "./static"

# 挂载静态目录
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

# 映射根路径 "/" 到 index.html
@app.get("/")
async def index():
    return FileResponse(STATIC_DIR /"index.html")

if __name__ == "__main__":
    uvicorn.run("web_server:app", host="0.0.0.0", port=18500, reload=False)
