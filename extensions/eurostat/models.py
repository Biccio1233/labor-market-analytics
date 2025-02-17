from sqlalchemy import Column, String, DateTime, Text
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime

Base = declarative_base()

class EurostatDataset(Base):
    __tablename__ = 'eurostat_datasets'
    __table_args__ = {'schema': 'eurostat'}
    
    dataset_code = Column(String(50), primary_key=True)
    view_name = Column(String(255), nullable=False)
    dataset_title = Column(String(500), nullable=False)
    description = Column(Text)
    created_at = Column(DateTime, default=datetime.utcnow)
    last_updated = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    def __repr__(self):
        return self.dataset_title
