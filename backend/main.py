from fastapi import FastAPI, Depends, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from models import ZipCodeProcedures
from sqlalchemy.orm import Session
from database import SessionLocal
from pydantic import BaseModel
from typing import List
import uvicorn


#Gets zips and procedures.
class ZipsAndProcedures(BaseModel):
    Rndrng_Prvdr_Zip5: str
    HCPCS_Cd: str
    Avg_Sbmtd_Chrg: float
    Avg_Mdcr_Alowd_Amt: float
    Avg_Mdcr_Pymt_Amt: float
    Estimated_Average_Payment: float
    provider_count: int
    id: int

    class Config:
        orm_mode = True

#Changed this to return the charged amounts rather than everything from the DB.
class ZipProcedureCreate(BaseModel):
    Avg_Sbmtd_Chrg: float
    Avg_Mdcr_Alowd_Amt: float
    Avg_Mdcr_Pymt_Amt: float
    Estimated_Average_Payment: float

    class Config:
        orm_mode = True

#I changed this to take in only things we want to query to the DB.
class ZipProcedureRead(BaseModel):
    Rndrng_Prvdr_Zip5: str
    HCPCS_Cd: str

    class Config:
        orm_mode = True   # very important for SQLAlchemy → Pydantic

app = FastAPI()
app.state.posts = {}

#This will try to open the database and connect to it.
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@app.get("/zips", response_model = List[ZipProcedureCreate])
def get_zip_procedures(db: Session = Depends(get_db)):
    #See if anything's in the query box from user; if so, query the DB and return.
    print(app.state.posts)
    if app.state.posts:
        #Still need the original query with ZipCodeProcedures, but we filter by using
        #the two key-value pairs in the global dictionary to find ONE result.
        results = db.query(ZipCodeProcedures).filter(
            ZipCodeProcedures.Rndrng_Prvdr_Zip5 == app.state.posts["Rndrng_Prvdr_Zip5"],
            ZipCodeProcedures.HCPCS_Cd == app.state.posts["HCPCS_Cd"]
        ).all()
        return results
    return [{
        "Avg_Sbmtd_Chrg": 0,
        "Avg_Mdcr_Alowd_Amt": 0,
        "Avg_Mdcr_Pymt_Amt": 0,
        "Estimated_Average_Payment": 0,
        }]


@app.post("/zips", response_model = ZipProcedureRead)
def add_zip_and_procedures(zipCode: str, procedureCode: str, data: ZipProcedureRead):
    #We'll probably need to make something s.t. if one of the two (or neither) show
    #up, then we try/except to stop the app from breaking.

    app.state.posts = {
        "Rndrng_Prvdr_Zip5": zipCode,
        "HCPCS_Cd": procedureCode
    }

    return app.state.posts


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)