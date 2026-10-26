class DAMPowerDistributor:

    @staticmethod
    def split(total_kw: float, confidence: str = "MEDIUM"):
        """
        Returns list of (step, kw_fraction)
        """

        if confidence == "HIGH":
            ratios = [0.5, 0.3, 0.2]
        elif confidence == "LOW":
            ratios = [0.6, 0.3, 0.1]
        else:  # MEDIUM
            ratios = [0.4, 0.4, 0.2]

        return [
            (1, total_kw * ratios[0]),
            (2, total_kw * ratios[1]),
            (3, total_kw * ratios[2]),
        ]
 