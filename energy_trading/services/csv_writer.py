import csv

def save_bids_to_csv(bids, filename="rtm_bids.csv"):
    with open(filename, "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow([
            "Bid_ID","Market","Entity","Date","Block_No","Start_Time","End_Time",
            "Quantity_MW","Price_Rs_per_kWh","Direction","Asset_ID","Remarks"
        ])

        for b in bids:
            writer.writerow([
                b.bid_id, b.market, b.entity, b.date, b.block_no, b.start_time,
                b.end_time, b.quantity_mw, b.price_rs_per_kwh,
                b.direction, b.asset_id, b.remarks
            ])

    return filename
