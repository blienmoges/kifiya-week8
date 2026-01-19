from fastapi import FastAPI

app = FastAPI(title="Medical Telegram Warehouse API")

@app.get("/health")
def health():
    return {"status": "ok"}
