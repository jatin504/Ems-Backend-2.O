from ...models.dam import DAMRawData, DAMBlock

class DAMBlockBuilder:

    @staticmethod
    def build(raw) -> list[DAMBlock]:

        blocks = []

        for pf in raw.plant_forecast:
            blocks.append(
                DAMBlock(
                    block_no=pf.block_no,
                    start_time=pf.start_time,
                    end_time=pf.end_time,
                    plant_power_kw=pf.plant_power_kw
                )
            )

        return blocks

