from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from config import SnowflakeConfig
from sqlalchemy import text

# Create the declarative base
Base = declarative_base()

class Database:
    def __init__(self, config: SnowflakeConfig):
        self.config = config
        self.engine = None
        self.SessionLocal = None
        
    def init_db(self):
        """Initialize database connection and session factory."""
        connection_url = self.config.get_connection_url()
        self.engine = create_engine(
            connection_url,
            echo=False  # Set to True for SQL query logging
        )
        self.SessionLocal = sessionmaker(
            autocommit=False,
            autoflush=False,
            bind=self.engine
        )
        
    def create_tables(self):
        """Create all tables defined in models."""
        Base.metadata.create_all(bind=self.engine)
        
    def get_session(self):
        """Get a new database session."""
        if self.SessionLocal is None:
            raise RuntimeError("Database not initialized. Call init_db() first.")
        return self.SessionLocal()

# Create global database instance
db = Database(SnowflakeConfig.from_env())

def get_db():
    """Dependency to get database session."""
    session = db.get_session()
    try:
        yield session
    finally:
        session.close()

def get_engine(connection_url):
    """Create SQLAlchemy engine from connection URL."""
    # Get the config instance from the connection URL
    config = SnowflakeConfig.from_env()
    
    # Create engine with additional parameters
    engine = create_engine(
        connection_url,
        echo=False,  # Set to True for SQL query logging
        **config.get_engine_params()
    )
    
    # Ensure schema is set
    if config.schema:
        with engine.connect() as conn:
            conn.execute(text(f"USE SCHEMA {config.schema}"))
            conn.commit()
    
    return engine

def get_session(engine):
    """Create a new session factory bound to the engine."""
    Session = sessionmaker(bind=engine)
    return Session() 