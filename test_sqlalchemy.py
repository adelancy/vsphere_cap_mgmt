from sqlalchemy import create_engine

# DB URL format --> dialect+driver://username:password@host:port/database
engine = create_engine('postgresql://test1@10.12.30.30/test1')
