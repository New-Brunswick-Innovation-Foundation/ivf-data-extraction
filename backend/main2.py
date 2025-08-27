# api.py
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Optional

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],  # frontend origin
    allow_credentials=True,
    allow_methods=["*"],  # allow all HTTP methods
    allow_headers=["*"],  # allow all headers
)

# ============ MODELS ============

class RegionChoice(BaseModel):
    city: str
    region: str

class DuplicateMatch(BaseModel):
    id: int
    name: str
    email: Optional[str]
    similarity: float

class DuplicateChoice(BaseModel):
    choice: int   # 1=Insert, 2=Skip, 3=Update, 4=Show details
    target_id: Optional[int] = None

# ============ MOCK QUEUES ============
# Later, replace with DB or pipeline queue
city_queue = ["Saint Andrews", "Minto", "Memramcook"]
person_queue = [
    {
        "name": "Bill McIver",
        "email": "bill.mciver@nbcc.ca",
        "matches": [
            {"id": 113, "name": "William McIver", "email": "Bill.McIver@nbcc.ca", "similarity": 1.0}
        ]
    },
    {
        "name": "Amber Garber",
        "email": "amber.garber@huntsmanmarine.ca",
        "matches": [
            {"id": 476, "name": "Amber Garber", "email": "amber.garber@huntsmanmarine.ca", "similarity": 1.0}
        ]
    }
]
company_queue = [
    {
        "name": "Monster House Publishing NB inc.",
        "address": "416 Northumberland St., Fredericton",
        "matches": [
            {"id": 228, "name": "Monster House Publishing NB inc.", "address": "416 Northumberland St., Fredericton", "similarity": 1.0}
        ]
    }
]

results = {"cities": [], "people": [], "companies": []}

# ============ ENDPOINTS ============

# --- City Region Assignment ---
@app.get("/next-city")
def next_city():
    if not city_queue:
        return {"done": True}
    return {"city": city_queue[0], "options": ["NE", "NW", "SE", "SW"]}

@app.post("/assign-region")
def assign_region(choice: RegionChoice):
    if city_queue and city_queue[0] == choice.city:
        city_queue.pop(0)
        results["cities"].append({"city": choice.city, "region": choice.region})
        return {"status": "ok", "assigned": choice.dict()}
    return {"status": "error", "message": "City mismatch"}

# --- Person Duplicate Resolution ---
@app.get("/next-person-duplicate")
def next_person_duplicate():
    if not person_queue:
        return {"done": True}
    return person_queue[0] | {"options": ["Insert new", "Skip", "Update", "Show details"]}

@app.post("/resolve-person-duplicate")
def resolve_person_duplicate(choice: DuplicateChoice):
    if not person_queue:
        return {"status": "error", "message": "No duplicates pending"}
    current = person_queue.pop(0)
    results["people"].append({"person": current, "resolution": choice.dict()})
    return {"status": "resolved", "action": choice.choice, "target": choice.target_id}

# --- Company Duplicate Resolution ---
@app.get("/next-company-duplicate")
def next_company_duplicate():
    if not company_queue:
        return {"done": True}
    return company_queue[0] | {"options": ["Insert new", "Skip", "Update", "Show details"]}

@app.post("/resolve-company-duplicate")
def resolve_company_duplicate(choice: DuplicateChoice):
    if not company_queue:
        return {"status": "error", "message": "No duplicates pending"}
    current = company_queue.pop(0)
    results["companies"].append({"company": current, "resolution": choice.dict()})
    return {"status": "resolved", "action": choice.choice, "target": choice.target_id}

# --- Final Summary ---
@app.get("/summary")
def summary():
    return results
