from ...models.dam import DAMBid

class DAMBidValidator:

    @staticmethod
    def enforce_soc_limit(bids: list[DAMBid], soc_available_kwh: float):

        max_energy = soc_available_kwh
        used = 0
        valid_bids = []

        for bid in bids:
            energy = bid.quantity_kw * 0.25

            if used + energy <= max_energy:
                valid_bids.append(bid)
                used += energy
            else:
                # Trim only highest price steps
                if bid.step == 1:
                    continue
                break

        return valid_bids
