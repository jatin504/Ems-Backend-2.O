class RTMBlockResolver:

    @staticmethod
    def get_active_blocks(now):
        """
        Returns list of RTM blocks that are in scope for planning / freezing / submitting
        """

        blocks = []

        for block in TimeBlockGenerator.future_blocks(hours=3):
            delivery_start = block.start_dt

            minutes_to_delivery = (delivery_start - now).total_seconds() / 60

            if minutes_to_delivery <= 120:
                blocks.append(block)

        return blocks
