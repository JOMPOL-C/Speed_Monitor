from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.routers import downtime_events, health, reasons


app = FastAPI(title="Speed Monitor API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(health.router)
app.include_router(reasons.router, prefix="/api", tags=["reasons"])
app.include_router(downtime_events.router, prefix="/api", tags=["downtime-events"])
