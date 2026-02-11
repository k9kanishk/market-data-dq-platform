from sqlmodel import SQLModel, create_engine, Session
from .settings import settings

def get_engine():
    return create_engine(settings.database_url, echo=False)

def init_db():
    SQLModel.metadata.create_all(get_engine())

def session():
    # critical for Streamlit + short-lived sessions
    return Session(get_engine(), expire_on_commit=False)
