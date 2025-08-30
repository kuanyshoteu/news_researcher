# backend/main.py
import os
import json
from datetime import datetime
from typing import List, Optional, Annotated

from fastapi import FastAPI, Depends, HTTPException, Header
from pydantic import BaseModel, Field, EmailStr

from sqlalchemy import (
    create_engine, String, Text, Integer, BigInteger, DateTime,
    ForeignKey, ARRAY, select, func
)
from sqlalchemy.orm import (
    DeclarativeBase, Mapped, mapped_column,
    relationship, sessionmaker, Session
)

# Импорт твоей функции: возвращает JSON-строку со списком триплетов [text, url, date]
from src.ai_news_process import ai_news_process as external_ai_news_process

# ---------- Настройки/БД ----------
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+psycopg://app:app_password@db:5432/app")
API_SECRET = os.getenv("API_SECRET", "change_me")

engine = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)

class Base(DeclarativeBase):
    pass

def get_db() -> Session:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# ---------- Модели ----------
class User(Base):
    __tablename__ = "users"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    email: Mapped[Optional[str]] = mapped_column(String, unique=True, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=func.now())

    tags: Mapped[List["UserTag"]] = relationship(back_populates="user", cascade="all, delete-orphan")
    seen: Mapped[List["UserSeenNews"]] = relationship(back_populates="user", cascade="all, delete-orphan")

class UserTag(Base):
    __tablename__ = "user_tags"
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), primary_key=True)
    tag: Mapped[str] = mapped_column(String, primary_key=True)
    user: Mapped[User] = relationship(back_populates="tags")

class NewsItem(Base):
    __tablename__ = "news_items"
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    url: Mapped[str] = mapped_column(Text, unique=True, index=True)
    title: Mapped[Optional[str]] = mapped_column(Text)
    body: Mapped[Optional[str]] = mapped_column(Text)
    tags: Mapped[Optional[List[str]]] = mapped_column(ARRAY(String), default=[])
    published_at: Mapped[Optional[datetime]] = mapped_column(DateTime)

    seen_by: Mapped[List["UserSeenNews"]] = relationship(back_populates="news", cascade="all, delete-orphan")

class UserSeenNews(Base):
    __tablename__ = "user_seen_news"
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), primary_key=True)
    news_id: Mapped[int] = mapped_column(ForeignKey("news_items.id", ondelete="CASCADE"), primary_key=True)
    seen_at: Mapped[datetime] = mapped_column(DateTime, default=func.now())

    user: Mapped[User] = relationship(back_populates="seen")
    news: Mapped[NewsItem] = relationship(back_populates="seen_by")

# ---------- Pydantic-схемы ----------
class UserCreate(BaseModel):
    email: Optional[EmailStr] = None

class UserOut(BaseModel):
    id: int
    email: Optional[EmailStr] = None
    class Config:
        from_attributes = True

class TagsIn(BaseModel):
    tags: List[str] = Field(default_factory=list)

class TagsOut(BaseModel):
    user_id: int
    tags: List[str]

class SeenIn(BaseModel):
    news_id: int

class NewsOut(BaseModel):
    id: int
    url: str
    title: Optional[str]
    body: Optional[str]
    tags: List[str] = []
    published_at: Optional[datetime]
    class Config:
        from_attributes = True

class RunResponse(BaseModel):
    items: List[NewsOut]

# ---------- Авторизация для защищённых ручек ----------
def require_api_key(x_api_key: Annotated[Optional[str], Header()] = None):
    if x_api_key != API_SECRET:
        raise HTTPException(status_code=401, detail="Unauthorized")

# ---------- Приложение ----------
app = FastAPI(title="News API")

@app.on_event("startup")
def on_startup():
    Base.metadata.create_all(bind=engine)

# ---- Пользователи ----
@app.post("/users", response_model=UserOut)
def create_user(payload: UserCreate, db: Session = Depends(get_db)):
    user = User(email=payload.email)
    db.add(user)
    db.commit()
    db.refresh(user)
    return user

@app.get("/users/{user_id}", response_model=UserOut)
def get_user(user_id: int, db: Session = Depends(get_db)):
    user = db.get(User, user_id)
    if not user:
        raise HTTPException(404, "User not found")
    return user

# ---- Теги ----
@app.get("/users/{user_id}/tags", response_model=TagsOut)
def get_tags(user_id: int, db: Session = Depends(get_db)):
    user = db.get(User, user_id)
    if not user:
        raise HTTPException(404, "User not found")
    tags = [t.tag for t in user.tags]
    return TagsOut(user_id=user_id, tags=tags)

@app.post("/users/{user_id}/tags", response_model=TagsOut)
def set_tags(user_id: int, payload: TagsIn, db: Session = Depends(get_db)):
    user = db.get(User, user_id)
    if not user:
        raise HTTPException(404, "User not found")
    db.query(UserTag).filter(UserTag.user_id == user_id).delete()
    for tag in payload.tags:
        tag = tag.strip().lower()
        if tag:
            db.add(UserTag(user_id=user_id, tag=tag))
    db.commit()
    tags = [t.tag for t in db.query(UserTag).filter_by(user_id=user_id).all()]
    return TagsOut(user_id=user_id, tags=tags)

# ---- Отметка "просмотрено" ----
@app.post("/users/{user_id}/seen")
def mark_seen(user_id: int, payload: SeenIn, db: Session = Depends(get_db)):
    user = db.get(User, user_id)
    if not user:
        raise HTTPException(404, "User not found")
    news = db.get(NewsItem, payload.news_id)
    if not news:
        raise HTTPException(404, "News not found")
    if db.get(UserSeenNews, {"user_id": user_id, "news_id": payload.news_id}):
        return {"status": "ok"}  # уже отмечено
    db.add(UserSeenNews(user_id=user_id, news_id=payload.news_id))
    db.commit()
    return {"status": "ok"}

# ---- Фид пользователя ----
@app.get("/users/{user_id}/feed", response_model=List[NewsOut])
def user_feed(user_id: int, limit: int = 50, db: Session = Depends(get_db)):
    user = db.get(User, user_id)
    if not user:
        raise HTTPException(404, "User not found")

    user_tags = [t.tag for t in user.tags]
    seen_ids = set(
        db.scalars(select(UserSeenNews.news_id).where(UserSeenNews.user_id == user_id)).all()
    )

    stmt = (
        select(NewsItem)
        .where(~NewsItem.id.in_(seen_ids))
        .order_by(NewsItem.published_at.desc().nullslast())
        .limit(limit)
    )
    items = list(db.scalars(stmt).all())

    if user_tags:
        tagset = set(user_tags)
        items = [it for it in items if set((it.tags or [])) & tagset]
    return items

# ---- /run: вызываем твою функцию из src/ai_news_process.py и апсертим в БД ----
@app.post("/run", response_model=RunResponse, dependencies=[Depends(require_api_key)])
def run(user_id: Optional[int] = None, db: Session = Depends(get_db)):
    """
    1) external_ai_news_process() -> возвращает JSON-строку со списком триплетов [text, url, date].
    2) Апсерт в news_items по url: сохраняем body=text, published_at=date.
    3) Если указан user_id — исключаем уже просмотренные и фильтруем по его тегам.
    """
    try:
        output = external_ai_news_process()  # JSON-строка
        triplets = json.loads(output)
    except Exception as exc:
        raise HTTPException(500, f"ai_news_process error: {exc}")

    stored: List[NewsItem] = []
    for t in triplets:
        if not isinstance(t, list) or len(t) < 2:
            continue
        text = (t[0] or "").strip()
        url = t[1]
        datestr = t[2] if len(t) > 2 else None

        published_at = None
        if datestr:
            try:
                published_at = datetime.fromisoformat(str(datestr).replace("Z", "+00:00"))
            except Exception:
                published_at = None

        existing = db.execute(select(NewsItem).where(NewsItem.url == url)).scalar_one_or_none()
        if existing:
            existing.body = text
            if published_at:
                existing.published_at = published_at
            db.add(existing)
            stored.append(existing)
        else:
            item = NewsItem(
                url=url,
                title=None,
                body=text,
                tags=[],  # при желании можно заполнять в будущем
                published_at=published_at,
            )
            db.add(item)
            db.flush()
            stored.append(item)

    db.commit()

    items = stored
    if user_id is not None:
        user = db.get(User, user_id)
        if not user:
            raise HTTPException(404, "User not found")
        user_tags = [t.tag for t in user.tags]
        seen_ids = set(
            db.scalars(select(UserSeenNews.news_id).where(UserSeenNews.user_id == user_id)).all()
        )
        items = [it for it in items if it.id not in seen_ids]
        if user_tags:
            tagset = set(user_tags)
            items = [it for it in items if set((it.tags or [])) & tagset]

    return RunResponse(items=items)
