# Open-Source_Healthcare_Pricing
A tool utilizing publically available medical datasets to show patients average costs per procedures in their area.

## 🔧 Tech Stack
- Python
- PostgreSQL
- FastAPI
- React (Future Implementation)

## 🛠️ Setup
1. Clone the repository.
2. Create `.env` with the database config.
3. Run `data processing/load.py`.
4. Launch FastAPI with `uvicorn`.

## ⚙️ Current Implementations
- Preprocessing script to group dataset by Zip Codes and Procedures and load dataset
  into a PostgreSQL database.
- Backend utilizing FastAPI to pull from database, process user queries and retrieve
  nececssary information pertaining to user request (average charges, average medicare
  coverage, etc).
- Script that takes in publically available procedure code data and editing it such
  that procedure descriptions include no abbreviations and can be used to choose
  from within the lists of procedures (AKA, code and description listed).

## 📈 Future Additions
- Frontend filters by map or chart
- More specific provider locations and pricings
- User-submitted pricing data (possibly)
