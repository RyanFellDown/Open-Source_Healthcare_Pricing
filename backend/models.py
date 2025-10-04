#This is the ORM that connects to our database, this is where queries occur.
from sqlalchemy import Boolean, Column, ForeignKey, Integer, String, BigInteger, Float
from sqlalchemy.types import Double
from database import Base
from sqlalchemy import Table, MetaData
from database import engine, Base

class ZipCodeProcedures(Base):
    __tablename__ = "zip_stats_df"

    Rndrng_Prvdr_Zip5 = Column("Rndrng_Prvdr_Zip5", String, index=True)
    HCPCS_Cd = Column("HCPCS_Cd", String, index=True)
    Avg_Sbmtd_Chrg = Column("Avg_Sbmtd_Chrg", Float)
    Avg_Mdcr_Alowd_Amt = Column("Avg_Mdcr_Alowd_Amt", Float)
    Avg_Mdcr_Pymt_Amt = Column("Avg_Mdcr_Pymt_Amt", Float)
    Estimated_Average_Payment = Column("Estimated_Average_Payment", Float)
    provider_count = Column("provider_count", BigInteger)
    id = Column(Integer, primary_key=True, index=True)