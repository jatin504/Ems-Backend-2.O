from ...models.dam import DAMBlockConstraint

class DAMConstraintEngine:

    def evaluate(blocks, bess):

        constraints = {}

        remaining_cycles = (
            bess["max_cycles_per_day"] - bess["cycles_used_today"]
        )

        for block in blocks:

            constraints[block.block_no] = DAMBlockConstraint(
                can_charge=bess["backup_soc"] < bess["max_soc"],
                can_discharge=bess["backup_soc"] > bess["min_soc"],
                max_charge_kw=bess["max_discharge_kw"],
                max_discharge_kw=bess["max_discharge_kw"]
            )

        return constraints
