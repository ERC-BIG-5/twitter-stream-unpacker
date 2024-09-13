from datetime import datetime

from sqlalchemy import Column, String, DateTime, JSON, Integer, func, Boolean
from sqlalchemy.orm import Mapped, mapped_column

from db_config import Base


class DBPost(Base):
    __tablename__ = 'post'

    id: Mapped[int] = mapped_column(primary_key=True)
    platform: Mapped[str] = mapped_column(String(20), nullable=False)
    # platform_id: Mapped[str] = mapped_column(String(50), nullable=True)
    post_url: Mapped[str] = mapped_column(String(60), nullable=True)
    post_url_computed: Mapped[str] = mapped_column(String(60), nullable=False, unique=False)
    date_created: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    content: Mapped[dict] = mapped_column(JSON)
    text: Mapped[str] = mapped_column(String(300))
    date_collected: Mapped[datetime] = mapped_column(DateTime, nullable=False, server_default=func.now())
    language: Mapped[str] = mapped_column(String(5), nullable=False)

    year_created: Mapped[int] = mapped_column(Integer, nullable=False)
    month_created: Mapped[int] = mapped_column(Integer, nullable=False)
    day_created: Mapped[int] = mapped_column(Integer, nullable=False)


    classification_relevant: Mapped[bool] = mapped_column(Boolean, nullable=True)
    classification_note: Mapped[str] = mapped_column(String, nullable=True)
    classification_data: Mapped[dict] = mapped_column(JSON, nullable=True)

    # comments: Mapped[list[DBComment]] = relationship(back_populates="post")

    def set_date_columns(self):
        self.year_created = self.date_created.year
        self.month_created = self.date_created.month
        self.day_created = self.date_created.day
