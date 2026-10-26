from models.tariff import TariffBlock
from models.plant_state import PlantState
from services.rtm_strategy import generate_rtm_decision
from services.bid_generator import generate_bid
from services.csv_writer import save_bids_to_csv
from statistics import mean

### SAMPLE INPUT DATA (manual for now)
tariffs = [
    TariffBlock(45, "11:15", "11:30", 3.25),
    TariffBlock(46, "11:30", "11:45", 3.50),
    TariffBlock(90, "22:15", "22:30", 6.00)
]

plant_state = PlantState(soc=70, available_capacity_mw=5)

avg_price = mean([t.price_rs_per_kwh for t in tariffs])

### PIPELINE
bids = []

for row in tariffs:
    decision = generate_rtm_decision(row, plant_state, avg_price)
    bid_obj = generate_bid(row, decision, "2025-10-30")
    bids.append(bid_obj)

### SAVE OUTPUT
file_path = save_bids_to_csv(bids)

print(f"RTM Bids saved: {file_path}")
