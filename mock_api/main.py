from fastapi import FastAPI
from api.routes import router

app = FastAPI(title="Mock OLTP API")

app.include_router(router)

@app.get("/health")
def health():
    return {"status": "ok"}