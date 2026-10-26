from typing import List, Dict, Any, Optional
from datetime import datetime


class BESScheduler:
    """
    Look-ahead BESS scheduler with safe grid charging.

    ENERGY FLOW PRIORITY
    --------------------
    1. PV → LOAD
    2. BESS → LOAD
    3. GRID → LOAD

    PEAK DETECTION & SKIP LOGIC
    ---------------------------
    - Active peaks ('peak', 'on_peak'): BESS prepares to serve the FULL net load.
    - Mid-peaks / Off-peaks: BESS skips discharging to preserve energy for active peaks,
      UNLESS discharging is strictly required to prevent a grid limit violation.

    BESS BEHAVIOR
    -------------
    - During active peak:
      BESS discharges to serve load. Grid import NEVER exceeds grid_limit.

    - During normal operation (off_peak / mid_peak):
      BESS discharges ONLY energy above the required reserve.
      If grid limit is at risk, it forces discharge to prevent violation.

    - Tariff-Aware Grid Charging:
      Prefers charging during low-tariff periods.
      Breaches tariff preference if peak is urgent and reserve is unmet.

    - Cycle Counting:
      Uses throughput method: (Total Charge + Total Discharge) / (2 * Capacity).

    EVENT ID FORMAT
    ---------------
    SPE{YYMMDDHHMM}-{seq:02d}.{attempt:02d}
      S   = Schedule
      P   = Power Dispatch
      E   = EMS
      YYMMDDHHMM = timestamp of the interval
      seq = event sequence (increments on decision change)
      attempt = retry attempt (00 = first attempt)
    """

    def __init__(
        self,
        capacity_mwh: float = 16.716,
        max_power_mw: float = 4.0,
        hard_min_soc_percent: float = 20.0,
        hard_max_soc_percent: float = 80.0,
        target_min_soc_percent: float = 20.0,
        target_max_soc_percent: float = 80.0,
        initial_soc_percent: float = 50.0,
        charge_efficiency: float = 0.97,
        discharge_efficiency: float = 0.97,
        interval_hours: float = 0.25,
        lookahead_intervals: int = 96,
        peak_reserve_margin_percent: float = 5.0,
        risk_reserve_percent: float = 5.0,
        pv_rated_power_kw: float =2000.0,
        allow_grid_charging: bool = True,
        max_grid_charge_power_kw: Optional[float] = None,
        charge_grid_to_target: bool = False,
    ):
        self.capacity_mwh = capacity_mwh
        self.max_power_mw = max_power_mw
        self.hard_min_soc_percent = hard_min_soc_percent
        self.hard_max_soc_percent = hard_max_soc_percent
        self.target_min_soc_percent = target_min_soc_percent
        self.target_max_soc_percent = target_max_soc_percent
        self.initial_soc_percent = initial_soc_percent
        self.charge_efficiency = charge_efficiency
        self.discharge_efficiency = discharge_efficiency
        self.interval_hours = interval_hours
        self.lookahead_intervals = lookahead_intervals
        self.peak_reserve_margin_percent = peak_reserve_margin_percent
        self.risk_reserve_percent = risk_reserve_percent
        self.allow_grid_charging = allow_grid_charging
        self.max_grid_charge_power_kw = max_grid_charge_power_kw
        self.charge_grid_to_target = charge_grid_to_target

        self.hard_min_energy_mwh = capacity_mwh * hard_min_soc_percent / 100.0
        self.hard_max_energy_mwh = capacity_mwh * hard_max_soc_percent / 100.0
        self.target_min_energy_mwh = capacity_mwh * target_min_soc_percent / 100.0
        self.target_max_energy_mwh = capacity_mwh * target_max_soc_percent / 100.0

        self.pv_rated_power_kw = pv_rated_power_kw

        self.nominal_usable_capacity_mwh = 16.716 - 0.43 # 26 - 0.7

        self.cumulative_charge_mwh = 0.0
        self.cumulative_discharge_mwh = 0.0

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    @staticmethod
    def aggregate_to_15min(sample_window: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Aggregates 1-minute interval data into 15-minute intervals.

        Behavior
        --------
        - Groups rows by 15-minute bucket (00, 15, 30, 45).
        - Averages numeric fields: load_kw, pv_kw, tariff_per_kw,
        co_relation_percent, grid_import_limit_kw.
        - For categorical field load_state: uses the most frequent value
        (mode) in the window.
        - Preserves the raw 1-minute load_kw values in `window_load`.
        - Output timestamp is the start of the 15-min window.

        Parameters
        ----------
        sample_window : list of dict
            1-minute resolution rows. Each row must contain at least:
            time_stamp, load_kw, pv_kw, co_relation_percent, load_state,
            grid_import_limit_kw, tariff_per_kw.

        Returns
        -------
        list of dict
            15-minute resolution rows with an additional `window_load` key.
        """
        if not sample_window:
            return []

        ts_format = "%Y-%m-%d %H:%M:%S"

        unique_rows = {}
        for row in sample_window:
            unique_rows[row["time_stamp"]] = row
            sample_window = list(unique_rows.values())

        # Group rows by 15-minute window
        windows: Dict[str, List[Dict[str, Any]]] = {}
        for row in sample_window:
            ts = datetime.strptime(row["time_stamp"], ts_format)
            minute = (ts.minute // 15) * 15
            window_start = ts.replace(minute=minute, second=0, microsecond=0)
            window_key = window_start.strftime(ts_format)

            if window_key not in windows:
                windows[window_key] = []
            windows[window_key].append(row)

        aggregated: List[Dict[str, Any]] = []
        for window_key in sorted(windows.keys()):
            rows = windows[window_key]
            n = len(rows)

            # Raw 1-minute load values (preserve for event object)
            window_load = [float(r["load_kw"]) for r in rows]
            pv_window = [BESScheduler.calculate_pv_from_poa(2000.0, float(r["poa"])) for r in rows]
            poa_window = [float(r["poa"]) for r in rows]

            # Averages for numeric fields
            avg_load = sum(window_load) / n
            avg_pv = sum(float(r["pv_kw"]) for r in rows) / n
            avg_poa = sum(float(r["poa"]) for r in rows) / n
            avg_co = sum(float(r["co_relation_percent"]) for r in rows) / n
            avg_tariff = sum(float(r["tariff_per_kw"]) for r in rows) / n
            avg_grid_limit = sum(float(r["grid_import_limit_kw"]) for r in rows) / n

            # Mode for categorical field
            load_states = [r["load_state"] for r in rows]
            load_state = max(set(load_states), key=load_states.count)

            aggregated.append({
                "time_stamp": window_key,
                "load_kw": avg_load,
                "pv_kw": avg_pv,
                "poa": avg_poa,
                "co_relation_percent": avg_co,
                "load_state": load_state,
                "grid_import_limit_kw": avg_grid_limit,
                "tariff_per_kw": avg_tariff,
                "window_load": window_load,
                "pv_window": pv_window,
                "poa_window": poa_window,
            })

        return aggregated

    def _clamp(self, value: float, minimum: float, maximum: float) -> float:
        return max(minimum, min(value, maximum))

    def _soc(self, energy_mwh: float) -> float:
        return energy_mwh / self.capacity_mwh * 100.0

    def _is_active_peak(self, load_state: str) -> bool:
        state_lower = load_state.lower().strip()
        return state_lower in ["peak"]

    def _max_charge_power(self, energy_mwh: float) -> float:
        available_energy_mwh = max(0.0, self.hard_max_energy_mwh - energy_mwh)
        max_power_from_soc = (
            available_energy_mwh * 1000.0
            / (self.charge_efficiency * self.interval_hours)
        )
        return min(self.max_power_mw * 1000.0, max_power_from_soc)

    def _max_discharge_power(self, energy_mwh: float) -> float:
        available_energy_mwh = max(0.0, energy_mwh - self.hard_min_energy_mwh)
        max_power_from_soc = (
            available_energy_mwh * self.discharge_efficiency
            / self.interval_hours * 1000.0
        )
        return min(self.max_power_mw * 1000.0, max_power_from_soc)

    @staticmethod
    def _format_event_id(timestamp: str, seq: int, attempt: int = 0) -> str:
        """
        Build event id: SPE{YYMMDDHHMM}-{seq:02d}.{attempt:02d}
        """
        ts = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
        ts_str = ts.strftime("%y%m%d%H%M")
        return f"SPE{ts_str}-{seq:02d}.{attempt:02d}"

    # ------------------------------------------------------------------
    # Lookahead helpers
    # ------------------------------------------------------------------
    def _get_upcoming_peaks_info(
        self,
        sample_window: List[Dict[str, Any]],
        current_index: int,
    ) -> Dict[str, Any]:
        end_index = min(
            len(sample_window),
            current_index + self.lookahead_intervals + 1,
        )

        active_peak_energy_mwh = 0.0
        violation_energy_mwh = 0.0
        next_peak_ts = None
        max_required_kw = 0.0

        for i in range(current_index + 1, end_index):
            row = sample_window[i]
            load_state = row.get("load_state", "")

            load_kw = float(row["load_kw"])
            pv_kw = float(row["pv_kw"])
            net_load_kw = max(0.0, load_kw - pv_kw)
            grid_limit_kw = float(row["grid_import_limit_kw"])

            is_active = self._is_active_peak(load_state)

            if is_active:
                if next_peak_ts is None:
                    next_peak_ts = row["time_stamp"]

                req_kw = min(net_load_kw, self.max_power_mw * 1000.0)
                max_required_kw = max(max_required_kw, req_kw)

                active_peak_energy_mwh += (
                    req_kw * self.interval_hours
                    / 1000.0 / self.discharge_efficiency
                )
            else:
                if net_load_kw > grid_limit_kw:
                    violation_kw = min(
                        net_load_kw - grid_limit_kw,
                        self.max_power_mw * 1000.0,
                    )
                    violation_energy_mwh += (
                        violation_kw * self.interval_hours
                        / 1000.0 / self.discharge_efficiency
                    )

        return {
            "next_peak_ts": next_peak_ts,
            "active_peak_energy_mwh": active_peak_energy_mwh,
            "violation_energy_mwh": violation_energy_mwh,
            "max_required_kw": max_required_kw,
        }

    def _calculate_required_reserve(
        self,
        active_peak_energy_mwh: float,
        violation_energy_mwh: float,
    ) -> Dict[str, float]:
        total_required_energy = active_peak_energy_mwh + violation_energy_mwh

        if total_required_energy == 0.0:
            return {
                "active_peak_energy_mwh": 0.0,
                "violation_energy_mwh": 0.0,
                "peak_margin_mwh": 0.0,
                "risk_reserve_mwh": 0.0,
                "required_reserve_mwh": self.target_min_energy_mwh,
            }

        peak_margin_mwh = self.capacity_mwh * self.peak_reserve_margin_percent / 100.0
        risk_reserve_mwh = self.capacity_mwh * self.risk_reserve_percent / 100.0

        required_reserve_mwh = (
            self.hard_min_energy_mwh
            + total_required_energy
            + peak_margin_mwh
            + risk_reserve_mwh
        )

        return {
            "active_peak_energy_mwh": active_peak_energy_mwh,
            "violation_energy_mwh": violation_energy_mwh,
            "peak_margin_mwh": peak_margin_mwh,
            "risk_reserve_mwh": risk_reserve_mwh,
            "required_reserve_mwh": min(required_reserve_mwh, self.target_max_energy_mwh),
        }

    @staticmethod
    def calculate_pv_from_poa(pv_rated_power_kw, poa: float) -> float:
        return pv_rated_power_kw * poa / 1000

    # ------------------------------------------------------------------
    # Main scheduler
    # ------------------------------------------------------------------
    def schedule(
        self,
        sample_window: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        if not sample_window:
            return []

        self.cumulative_charge_mwh = 0.0
        self.cumulative_discharge_mwh = 0.0

        energy_mwh = self.capacity_mwh * self.initial_soc_percent / 100.0
        energy_mwh = self._clamp(
            energy_mwh,
            self.hard_min_energy_mwh,
            self.hard_max_energy_mwh,
        )

        results: List[Dict[str, Any]] = []

        # Event tracking
        event_seq = 0
        last_decision: Optional[str] = None
        # current_event_id = self._format_event_id(sample_window[0]["time_stamp"], 0, 0)
        current_target_charge_mwh = 0.0
        current_target_discharge_mwh = 0.0

        for idx, row in enumerate(sample_window):
            load_kw = float(row["load_kw"])
            pv_kw = BESScheduler.calculate_pv_from_poa(self.pv_rated_power_kw, float(row["poa"])) #float(row["pv_kw"])
            grid_limit_kw = float(row["grid_import_limit_kw"])
            timestamp = row["time_stamp"]
            current_event_id = self._format_event_id(timestamp, event_seq, 0)
            event_number = idx + 1
            load_state = row.get("load_state", "")
            tariff_per_kw = float(row.get("tariff_per_kw", 0.0))
            co_relation_percent = float(row.get("co_relation_percent", 0.0))
            window_load = row.get("window_load", [])
            pv_window = row.get("pv_window", [])
            poa_window = row.get("poa_window", [])

            net_load_kw = load_kw - pv_kw

            peaks_info = self._get_upcoming_peaks_info(sample_window, idx)
            next_peak_ts = peaks_info["next_peak_ts"]
            active_peak_energy_mwh = peaks_info["active_peak_energy_mwh"]
            violation_energy_mwh = peaks_info["violation_energy_mwh"]
            max_required_kw = peaks_info["max_required_kw"]

            reserve_info = self._calculate_required_reserve(
                active_peak_energy_mwh, violation_energy_mwh
            )
            required_reserve_mwh = reserve_info["required_reserve_mwh"]

            active_peak = self._is_active_peak(load_state)

            max_charge_kw = self._max_charge_power(energy_mwh)
            max_discharge_kw = self._max_discharge_power(energy_mwh)

            if self.max_grid_charge_power_kw is None:
                max_grid_charge_kw = self.max_power_mw * 1000.0
            else:
                max_grid_charge_kw = min(
                    self.max_grid_charge_power_kw,
                    self.max_power_mw * 1000.0,
                )
            max_grid_charge_kw = min(max_grid_charge_kw, max_charge_kw)

            decision = "idle"
            charge_kw = 0.0
            grid_charge_kw = 0.0
            discharge_kw = 0.0
            description = ""

            # ======================================================
            # ACTIVE PEAK: PV → BESS → GRID
            # ======================================================
            if active_peak:
                target_discharge = min(net_load_kw, max_discharge_kw)

                grid_import_preview = max(0.0, net_load_kw - target_discharge)
                if grid_import_preview > grid_limit_kw:
                    additional_needed = grid_import_preview - grid_limit_kw
                    target_discharge = min(
                        target_discharge + additional_needed,
                        max_discharge_kw,
                    )

                discharge_kw = target_discharge
                grid_import_kw = max(0.0, net_load_kw - discharge_kw)

                if discharge_kw > 0:
                    decision = "discharge"
                    description = (
                        f"Active peak. PV supplies {pv_kw:.0f} kW. "
                        f"BESS discharges {discharge_kw:.0f} kW to serve load. "
                        f"Grid imports {grid_import_kw:.0f} kW."
                    )
                else:
                    decision = "idle"
                    description = (
                        f"Active peak. PV supplies {pv_kw:.0f} kW. "
                        f"BESS cannot discharge (hard min reached). "
                        f"Grid imports {grid_import_kw:.0f} kW."
                    )

            # ======================================================
            # NORMAL OPERATION (off_peak / mid_peak)
            # ======================================================
            else:
                if net_load_kw <= 0:
                    pv_surplus_kw = abs(net_load_kw)
                    energy_deficit_mwh = max(0.0, required_reserve_mwh - energy_mwh)

                    if energy_deficit_mwh > 0:
                        target_charge_mwh = energy_deficit_mwh
                        charging_for_peak = True
                    else:
                        target_charge_mwh = max(0.0, self.target_max_energy_mwh - energy_mwh)
                        charging_for_peak = False

                    required_charge_kw = (
                        target_charge_mwh * 1000.0
                        / (self.charge_efficiency * self.interval_hours)
                    )

                    charge_kw = min(pv_surplus_kw, max_charge_kw, required_charge_kw)

                    if charge_kw > 0:
                        decision = "charge"
                        if charging_for_peak:
                            description = (
                                f"PV surplus {pv_surplus_kw:.0f} kW. "
                                f"BESS charges {charge_kw:.0f} kW for upcoming peak preparation."
                            )
                        else:
                            description = (
                                f"PV surplus {pv_surplus_kw:.0f} kW. "
                                f"BESS charges {charge_kw:.0f} kW toward target SOC."
                            )

                else:
                    available_energy_mwh = max(0.0, energy_mwh - required_reserve_mwh)
                    max_discharge_from_reserve = (
                        available_energy_mwh * self.discharge_efficiency
                        / self.interval_hours * 1000.0
                    )
                    max_discharge_from_reserve = min(max_discharge_from_reserve, max_discharge_kw)

                    required_to_prevent_violation = max(0.0, net_load_kw - grid_limit_kw)

                    discharge_kw = min(
                        net_load_kw,
                        max(max_discharge_from_reserve, required_to_prevent_violation),
                    )
                    discharge_kw = min(discharge_kw, max_discharge_kw)

                    grid_import_kw = max(0.0, net_load_kw - discharge_kw)

                    if discharge_kw > 0:
                        decision = "discharge"
                        if required_to_prevent_violation > 0 and max_discharge_from_reserve < required_to_prevent_violation:
                            description = (
                                f"Grid limit risk. BESS discharges {discharge_kw:.0f} kW "
                                f"to prevent grid violation. Grid imports {grid_import_kw:.0f} kW."
                            )
                        else:
                            description = (
                                f"PV supplies {pv_kw:.0f} kW. "
                                f"BESS discharges {discharge_kw:.0f} kW from energy above reserve. "
                                f"Grid imports {grid_import_kw:.0f} kW."
                            )
                    else:
                        decision = "idle"
                        description = (
                            f"PV supplies {pv_kw:.0f} kW. "
                            f"BESS idle to protect peak reserve (skipping mid-peak). "
                            f"Grid imports {grid_import_kw:.0f} kW."
                        )

            # ======================================================
            # TARIFF-AWARE GRID CHARGING FOR PEAK PREPARATION
            # ======================================================
            if (
                next_peak_ts is not None
                and not active_peak
                and self.allow_grid_charging
            ):
                min_pre_peak_tariff = float('inf')
                intervals_to_peak = 0
                for i in range(idx + 1, len(sample_window)):
                    if sample_window[i].get("time_stamp") == next_peak_ts:
                        break
                    t = float(sample_window[i].get("tariff_per_kw", 0.0))
                    if t < min_pre_peak_tariff:
                        min_pre_peak_tariff = t
                    intervals_to_peak += 1

                is_low_tariff = True
                if min_pre_peak_tariff != float('inf'):
                    is_low_tariff = (tariff_per_kw <= min_pre_peak_tariff * 1.1)

                urgent_to_charge = (intervals_to_peak <= 4 and energy_mwh < required_reserve_mwh)

                if is_low_tariff or urgent_to_charge:
                    energy_deficit_mwh = max(0.0, required_reserve_mwh - energy_mwh)

                    if self.charge_grid_to_target and energy_deficit_mwh <= 0:
                        energy_deficit_mwh = max(0.0, self.target_max_energy_mwh - energy_mwh)

                    if energy_deficit_mwh > 0:
                        pv_energy_added_mwh = (
                            charge_kw * self.interval_hours / 1000.0 * self.charge_efficiency
                        )
                        remaining_deficit_mwh = max(0.0, energy_deficit_mwh - pv_energy_added_mwh)

                        if remaining_deficit_mwh > 0:
                            required_grid_charge_kw = (
                                remaining_deficit_mwh * 1000.0
                                / (self.charge_efficiency * self.interval_hours)
                            )

                            remaining_bess_capacity_kw = max(0.0, max_charge_kw - charge_kw)

                            grid_charge_kw = min(
                                required_grid_charge_kw,
                                max_grid_charge_kw,
                                remaining_bess_capacity_kw,
                            )

                            available_grid_capacity_kw = max(0.0, grid_limit_kw - max(0.0, net_load_kw))
                            grid_charge_kw = min(grid_charge_kw, available_grid_capacity_kw)

                            if grid_charge_kw > 0:
                                decision = "grid_charge"
                                if not is_low_tariff and urgent_to_charge:
                                    description = (
                                        f"URGENT: Breaching low tariff to suffice peak reserve. "
                                        f"Grid charges BESS {grid_charge_kw:.0f} kW."
                                    )
                                else:
                                    description = (
                                        f"Low tariff period. Grid charges BESS {grid_charge_kw:.0f} kW "
                                        f"for upcoming peak preparation."
                                    )

            # ======================================================
            # EVENT ID & TARGETS
            # ======================================================
            if decision != last_decision:
                event_seq += 1
                last_decision = decision
                # current_event_id = self._format_event_id(timestamp, event_seq, 0)

                if decision in ("charge", "grid_charge"):
                    planned_charge = max(0.0, required_reserve_mwh - energy_mwh)
                    if self.charge_grid_to_target and planned_charge <= 0.0:
                        planned_charge = max(0.0, self.target_max_energy_mwh - energy_mwh)
                    current_target_charge_mwh = planned_charge
                    current_target_discharge_mwh = 0.0
                elif decision == "discharge":
                    planned_discharge = active_peak_energy_mwh + violation_energy_mwh
                    if planned_discharge == 0.0:
                        planned_discharge = max(0.0, energy_mwh - required_reserve_mwh)
                    current_target_discharge_mwh = planned_discharge
                    current_target_charge_mwh = 0.0
                else:
                    current_target_charge_mwh = 0.0
                    current_target_discharge_mwh = 0.0

            # ======================================================
            # UPDATE BATTERY ENERGY & CYCLE COUNT
            # ======================================================
            total_charge_kw = charge_kw + grid_charge_kw

            step_charge_mwh = 0.0
            step_discharge_mwh = 0.0

            if total_charge_kw > 0:
                step_charge_mwh = (
                    total_charge_kw * self.interval_hours / 1000.0 * self.charge_efficiency
                )
                energy_mwh += step_charge_mwh

            if discharge_kw > 0:
                step_discharge_mwh = (
                    discharge_kw * self.interval_hours / 1000.0 / self.discharge_efficiency
                )
                energy_mwh -= step_discharge_mwh

            self.cumulative_charge_mwh += step_charge_mwh
            self.cumulative_discharge_mwh += step_discharge_mwh

            energy_mwh = self._clamp(
                energy_mwh,
                self.hard_min_energy_mwh,
                self.hard_max_energy_mwh,
            )
            soc_percent = self._soc(energy_mwh)

            throughput_cycles = (
                (self.cumulative_charge_mwh + self.cumulative_discharge_mwh)
                / (2 * self.nominal_usable_capacity_mwh)
            )

            grid_import_kw = max(0.0, net_load_kw + grid_charge_kw + charge_kw - discharge_kw)
            grid_limit_violation_kw = max(0.0, grid_import_kw - grid_limit_kw)

            results.append({
                "time_stamp": timestamp,
                "load_kw": round(load_kw, 2),
                "pv_kw": round(pv_kw, 2),
                "net_load_kw": round(net_load_kw, 2),
                "grid_import_limit_kw": round(grid_limit_kw, 2),
                "tariff_per_kw": round(tariff_per_kw, 4),
                "co_relation_percent": round(co_relation_percent, 2),
                "load_state": load_state,
                "decision": decision,
                "charge_kw": round(charge_kw, 2),
                "grid_charge_kw": round(grid_charge_kw, 2),
                "discharge_kw": round(discharge_kw, 2),
                "grid_import_kw": round(grid_import_kw, 2),
                "grid_limit_violation_kw": round(grid_limit_violation_kw, 2),
                "soc_percent": round(soc_percent, 2),
                "battery_energy_mwh": round(energy_mwh, 3),
                "throughput_cycles": round(throughput_cycles, 4),
                "upcoming_peak_timestamp": next_peak_ts,
                "upcoming_peak_required_kw": round(max_required_kw, 2),
                "active_peak_energy_mwh": round(active_peak_energy_mwh, 3),
                "violation_energy_mwh": round(violation_energy_mwh, 3),
                "risk_reserve_mwh": round(reserve_info["risk_reserve_mwh"], 3),
                "required_reserve_mwh": round(required_reserve_mwh, 3),
                "description": description,
                "event_id": current_event_id,
                "event_number": event_number,
                "target_charge_energy_mwh": round(current_target_charge_mwh, 3),
                "target_discharge_energy_mwh": round(current_target_discharge_mwh, 3),
                "window_load": [round(v, 2) for v in window_load],
                "pv_window": [round(v, 2) for v in pv_window],
                "poa_window": [round(v, 2) for v in poa_window],
            })

        return results

# from typing import List, Dict, Any, Optional

# class BESScheduler:
#     """
#     Look-ahead BESS scheduler with safe grid charging.

#     ENERGY FLOW PRIORITY
#     --------------------
#     1. PV → LOAD
#     2. BESS → LOAD
#     3. GRID → LOAD

#     PEAK DETECTION & SKIP LOGIC
#     ---------------------------
#     - Active peaks ('peak', 'on_peak'): BESS prepares to serve the FULL net load.
#     - Mid-peaks / Off-peaks: BESS skips discharging to preserve energy for active peaks,
#       UNLESS discharging is strictly required to prevent a grid limit violation.

#     BESS BEHAVIOR
#     -------------
#     - During active peak:
#       BESS discharges to serve load. Grid import NEVER exceeds grid_limit.
    
#     - During normal operation (off_peak / mid_peak):
#       BESS discharges ONLY energy above the required reserve.
#       If grid limit is at risk, it forces discharge to prevent violation.
    
#     - Tariff-Aware Grid Charging:
#       Prefers charging during low-tariff periods. 
#       Breaches tariff preference if peak is urgent and reserve is unmet.

#     - Cycle Counting:
#       Uses throughput method: (Total Charge + Total Discharge) / (2 * Capacity).
#     """

#     def __init__(
#         self,
#         capacity_mwh: float = 26.95,
#         max_power_mw: float = 5.39,
#         max_charge_power_kw: float = 5390,
#         max_discharge_power_kw: float = 5390,
#         hard_min_soc_percent: float = 20.0,
#         hard_max_soc_percent: float = 80.0,
#         target_min_soc_percent: float = 20.0,
#         target_max_soc_percent: float = 80.0,
#         initial_soc_percent: float = 50.0,
#         charge_efficiency: float = 1.0,
#         discharge_efficiency: float = 1.0,
#         interval_hours: float = 0.25,
#         lookahead_intervals: int = 96,
#         peak_reserve_margin_percent: float = 5.0,
#         risk_reserve_percent: float = 5.0,
#         allow_grid_charging: bool = True,
#         max_grid_charge_power_kw: Optional[float] = None,
#         charge_grid_to_target: bool = False,
#     ):
#         self.capacity_mwh = capacity_mwh
#         self.max_power_mw = max_power_mw
#         self.hard_min_soc_percent = hard_min_soc_percent
#         self.hard_max_soc_percent = hard_max_soc_percent
#         self.target_min_soc_percent = target_min_soc_percent
#         self.target_max_soc_percent = target_max_soc_percent
#         self.initial_soc_percent = initial_soc_percent
#         self.charge_efficiency = charge_efficiency
#         self.discharge_efficiency = discharge_efficiency
#         self.interval_hours = interval_hours
#         self.lookahead_intervals = lookahead_intervals
#         self.peak_reserve_margin_percent = peak_reserve_margin_percent
#         self.risk_reserve_percent = risk_reserve_percent
#         self.allow_grid_charging = allow_grid_charging
#         self.max_grid_charge_power_kw = max_grid_charge_power_kw
#         self.charge_grid_to_target = charge_grid_to_target

#         self.hard_min_energy_mwh = capacity_mwh * hard_min_soc_percent / 100.0
#         self.hard_max_energy_mwh = capacity_mwh * hard_max_soc_percent / 100.0
#         self.target_min_energy_mwh = capacity_mwh * target_min_soc_percent / 100.0
#         self.target_max_energy_mwh = capacity_mwh * target_max_soc_percent / 100.0

#         self.nominal_usable_capacity_mwh = 26 - 0.7

#         self.cumulative_charge_mwh = 0.0
#         self.cumulative_discharge_mwh = 0.0

#     def _clamp(self, value: float, minimum: float, maximum: float) -> float:
#         return max(minimum, min(value, maximum))

#     def _soc(self, energy_mwh: float) -> float:
#         return energy_mwh / self.capacity_mwh * 100.0

#     def _is_active_peak(self, load_state: str) -> bool:
#         """
#         Strictly identifies main peaks. 
#         Returns True for 'peak', 'on_peak'.
#         Returns False for 'mid_peak', 'off_peak' (allows skipping them).
#         """
#         state_lower = load_state.lower().strip()
#         return state_lower in ["peak"]

#     def _max_charge_power(self, energy_mwh: float) -> float:
#         available_energy_mwh = max(0.0, self.hard_max_energy_mwh - energy_mwh)
#         max_power_from_soc = (
#             available_energy_mwh * 1000.0 
#             / (self.charge_efficiency * self.interval_hours)
#         )
#         return min(self.max_power_mw * 1000.0, max_power_from_soc)

#     def _max_discharge_power(self, energy_mwh: float) -> float:
#         available_energy_mwh = max(0.0, energy_mwh - self.hard_min_energy_mwh)
#         max_power_from_soc = (
#             available_energy_mwh * self.discharge_efficiency 
#             / self.interval_hours * 1000.0
#         )
#         return min(self.max_power_mw * 1000.0, max_power_from_soc)

#     def _get_upcoming_peaks_info(
#         self,
#         sample_window: List[Dict[str, Any]],
#         current_index: int,
#     ) -> Dict[str, Any]:
#         """
#         Calculates energy required for upcoming intervals.
#         - Active peaks: Reserves full net load.
#         - Non-active peaks: Reserves ONLY enough to prevent grid violations.
#         """
#         end_index = min(
#             len(sample_window),
#             current_index + self.lookahead_intervals + 1,
#         )

#         active_peak_energy_mwh = 0.0
#         violation_energy_mwh = 0.0
#         next_peak_ts = None
#         max_required_kw = 0.0

#         for i in range(current_index + 1, end_index):
#             row = sample_window[i]
#             load_state = row.get("load_state", "")
            
#             load_kw = float(row["load_kw"])
#             pv_kw = float(row["pv_kw"])
#             net_load_kw = max(0.0, load_kw - pv_kw)
#             grid_limit_kw = float(row["grid_import_limit_kw"])

#             is_active = self._is_active_peak(load_state)

#             if is_active:
#                 if next_peak_ts is None:
#                     next_peak_ts = row["time_stamp"]

#                 req_kw = min(net_load_kw, self.max_power_mw * 1000.0)
#                 max_required_kw = max(max_required_kw, req_kw)

#                 # Reserve full net load for active peaks
#                 active_peak_energy_mwh += (
#                     req_kw * self.interval_hours 
#                     / 1000.0 / self.discharge_efficiency
#                 )
#             else:
#                 # For non-active peaks, only reserve enough to prevent grid violation
#                 if net_load_kw > grid_limit_kw:
#                     violation_kw = min(
#                         net_load_kw - grid_limit_kw, 
#                         self.max_power_mw * 1000.0
#                     )
#                     violation_energy_mwh += (
#                         violation_kw * self.interval_hours 
#                         / 1000.0 / self.discharge_efficiency
#                     )

#         return {
#             "next_peak_ts": next_peak_ts,
#             "active_peak_energy_mwh": active_peak_energy_mwh,
#             "violation_energy_mwh": violation_energy_mwh,
#             "max_required_kw": max_required_kw,
#         }

#     def _calculate_required_reserve(
#         self,
#         active_peak_energy_mwh: float,
#         violation_energy_mwh: float,
#     ) -> Dict[str, float]:
#         total_required_energy = active_peak_energy_mwh + violation_energy_mwh

#         if total_required_energy == 0.0:
#             return {
#                 "active_peak_energy_mwh": 0.0,
#                 "violation_energy_mwh": 0.0,
#                 "peak_margin_mwh": 0.0,
#                 "risk_reserve_mwh": 0.0,
#                 "required_reserve_mwh": self.target_min_energy_mwh,
#             }

#         peak_margin_mwh = self.capacity_mwh * self.peak_reserve_margin_percent / 100.0
#         risk_reserve_mwh = self.capacity_mwh * self.risk_reserve_percent / 100.0

#         required_reserve_mwh = (
#             self.hard_min_energy_mwh 
#             + total_required_energy 
#             + peak_margin_mwh 
#             + risk_reserve_mwh
#         )

#         return {
#             "active_peak_energy_mwh": active_peak_energy_mwh,
#             "violation_energy_mwh": violation_energy_mwh,
#             "peak_margin_mwh": peak_margin_mwh,
#             "risk_reserve_mwh": risk_reserve_mwh,
#             "required_reserve_mwh": min(required_reserve_mwh, self.target_max_energy_mwh),
#         }

#     def schedule(
#         self,
#         sample_window: List[Dict[str, Any]],
#     ) -> List[Dict[str, Any]]:
#         if not sample_window:
#             return []

#         self.cumulative_charge_mwh = 0.0
#         self.cumulative_discharge_mwh = 0.0

#         energy_mwh = self.capacity_mwh * self.initial_soc_percent / 100.0
#         energy_mwh = self._clamp(
#             energy_mwh, 
#             self.hard_min_energy_mwh, 
#             self.hard_max_energy_mwh
#         )

#         results = []

#         for idx, row in enumerate(sample_window):
#             load_kw = float(row["load_kw"])
#             pv_kw = float(row["pv_kw"])
#             grid_limit_kw = float(row["grid_import_limit_kw"])
#             timestamp = row["time_stamp"]
#             load_state = row.get("load_state", "")
#             tariff_per_kw = float(row.get("tariff_per_kw", 0.0))
#             co_relation_percent = float(row.get("co_relation_percent", 0.0))

#             net_load_kw = load_kw - pv_kw

#             peaks_info = self._get_upcoming_peaks_info(sample_window, idx)
#             next_peak_ts = peaks_info["next_peak_ts"]
#             active_peak_energy_mwh = peaks_info["active_peak_energy_mwh"]
#             violation_energy_mwh = peaks_info["violation_energy_mwh"]
#             max_required_kw = peaks_info["max_required_kw"]

#             reserve_info = self._calculate_required_reserve(
#                 active_peak_energy_mwh, violation_energy_mwh
#             )
#             required_reserve_mwh = reserve_info["required_reserve_mwh"]

#             active_peak = self._is_active_peak(load_state)

#             max_charge_kw = self._max_charge_power(energy_mwh)
#             max_discharge_kw = self._max_discharge_power(energy_mwh)

#             if self.max_grid_charge_power_kw is None:
#                 max_grid_charge_kw = self.max_power_mw * 1000.0
#             else:
#                 max_grid_charge_kw = min(
#                     self.max_grid_charge_power_kw,
#                     self.max_power_mw * 1000.0,
#                 )
#             max_grid_charge_kw = min(max_grid_charge_kw, max_charge_kw)

#             decision = "idle"
#             charge_kw = 0.0
#             grid_charge_kw = 0.0
#             discharge_kw = 0.0
#             description = ""

#             # ======================================================
#             # ACTIVE PEAK: PV → BESS → GRID
#             # ======================================================
#             if active_peak:
#                 # Serve as much load as possible
#                 target_discharge = min(net_load_kw, max_discharge_kw)
                
#                 # Ensure grid import doesn't exceed limit
#                 grid_import_preview = max(0.0, net_load_kw - target_discharge)
#                 if grid_import_preview > grid_limit_kw:
#                     additional_needed = grid_import_preview - grid_limit_kw
#                     target_discharge = min(
#                         target_discharge + additional_needed, 
#                         max_discharge_kw
#                     )
                    
#                 discharge_kw = target_discharge
#                 grid_import_kw = max(0.0, net_load_kw - discharge_kw)

#                 if discharge_kw > 0:
#                     decision = "discharge"
#                     description = (
#                         f"Active peak. PV supplies {pv_kw:.0f} kW. "
#                         f"BESS discharges {discharge_kw:.0f} kW to serve load. "
#                         f"Grid imports {grid_import_kw:.0f} kW."
#                     )
#                 else:
#                     decision = "idle"
#                     description = (
#                         f"Active peak. PV supplies {pv_kw:.0f} kW. "
#                         f"BESS cannot discharge (hard min reached). "
#                         f"Grid imports {grid_import_kw:.0f} kW."
#                     )

#             # ======================================================
#             # NORMAL OPERATION (off_peak / mid_peak)
#             # ======================================================
#             else:
#                 if net_load_kw <= 0:
#                     # PV SURPLUS: PV → LOAD, PV → BESS
#                     pv_surplus_kw = abs(net_load_kw)
#                     energy_deficit_mwh = max(0.0, required_reserve_mwh - energy_mwh)

#                     if energy_deficit_mwh > 0:
#                         target_charge_mwh = energy_deficit_mwh
#                         charging_for_peak = True
#                     else:
#                         target_charge_mwh = max(0.0, self.target_max_energy_mwh - energy_mwh)
#                         charging_for_peak = False

#                     required_charge_kw = (
#                         target_charge_mwh * 1000.0 
#                         / (self.charge_efficiency * self.interval_hours)
#                     )

#                     charge_kw = min(pv_surplus_kw, max_charge_kw, required_charge_kw)

#                     if charge_kw > 0:
#                         decision = "charge"
#                         if charging_for_peak:
#                             description = (
#                                 f"PV surplus {pv_surplus_kw:.0f} kW. "
#                                 f"BESS charges {charge_kw:.0f} kW for upcoming peak preparation."
#                             )
#                         else:
#                             description = (
#                                 f"PV surplus {pv_surplus_kw:.0f} kW. "
#                                 f"BESS charges {charge_kw:.0f} kW toward target SOC."
#                             )

#                 else:
#                     # PV DEFICIT: PV → LOAD, BESS → LOAD, GRID → LOAD
#                     available_energy_mwh = max(0.0, energy_mwh - required_reserve_mwh)
#                     max_discharge_from_reserve = (
#                         available_energy_mwh * self.discharge_efficiency 
#                         / self.interval_hours * 1000.0
#                     )
#                     max_discharge_from_reserve = min(max_discharge_from_reserve, max_discharge_kw)

#                     # MUST discharge at least this much to prevent grid violation
#                     required_to_prevent_violation = max(0.0, net_load_kw - grid_limit_kw)

#                     # Discharge max of (reserve excess) and (violation prevention)
#                     discharge_kw = min(
#                         net_load_kw, 
#                         max(max_discharge_from_reserve, required_to_prevent_violation)
#                     )
#                     discharge_kw = min(discharge_kw, max_discharge_kw)
                    
#                     grid_import_kw = max(0.0, net_load_kw - discharge_kw)

#                     if discharge_kw > 0:
#                         decision = "discharge"
#                         if required_to_prevent_violation > 0 and max_discharge_from_reserve < required_to_prevent_violation:
#                              description = (
#                                 f"Grid limit risk. BESS discharges {discharge_kw:.0f} kW "
#                                 f"to prevent grid violation. Grid imports {grid_import_kw:.0f} kW."
#                             )
#                         else:
#                             description = (
#                                 f"PV supplies {pv_kw:.0f} kW. "
#                                 f"BESS discharges {discharge_kw:.0f} kW from energy above reserve. "
#                                 f"Grid imports {grid_import_kw:.0f} kW."
#                             )
#                     else:
#                         decision = "idle"
#                         description = (
#                             f"PV supplies {pv_kw:.0f} kW. "
#                             f"BESS idle to protect peak reserve (skipping mid-peak). "
#                             f"Grid imports {grid_import_kw:.0f} kW."
#                         )

#             # ======================================================
#             # TARIFF-AWARE GRID CHARGING FOR PEAK PREPARATION
#             # ======================================================
#             if (
#                 next_peak_ts is not None 
#                 and not active_peak 
#                 and self.allow_grid_charging
#             ):
#                 min_pre_peak_tariff = float('inf')
#                 intervals_to_peak = 0
#                 for i in range(idx + 1, len(sample_window)):
#                     if sample_window[i].get("time_stamp") == next_peak_ts:
#                         break
#                     t = float(sample_window[i].get("tariff_per_kw", 0.0))
#                     if t < min_pre_peak_tariff:
#                         min_pre_peak_tariff = t
#                     intervals_to_peak += 1

#                 is_low_tariff = True
#                 if min_pre_peak_tariff != float('inf'):
#                     is_low_tariff = (tariff_per_kw <= min_pre_peak_tariff * 1.1)

#                 urgent_to_charge = (intervals_to_peak <= 4 and energy_mwh < required_reserve_mwh)

#                 if is_low_tariff or urgent_to_charge:
#                     energy_deficit_mwh = max(0.0, required_reserve_mwh - energy_mwh)

#                     if self.charge_grid_to_target and energy_deficit_mwh <= 0:
#                         energy_deficit_mwh = max(0.0, self.target_max_energy_mwh - energy_mwh)

#                     if energy_deficit_mwh > 0:
#                         pv_energy_added_mwh = (
#                             charge_kw * self.interval_hours / 1000.0 * self.charge_efficiency
#                         )
#                         remaining_deficit_mwh = max(0.0, energy_deficit_mwh - pv_energy_added_mwh)

#                         if remaining_deficit_mwh > 0:
#                             required_grid_charge_kw = (
#                                 remaining_deficit_mwh * 1000.0 
#                                 / (self.charge_efficiency * self.interval_hours)
#                             )

#                             remaining_bess_capacity_kw = max(0.0, max_charge_kw - charge_kw)

#                             grid_charge_kw = min(
#                                 required_grid_charge_kw,
#                                 max_grid_charge_kw,
#                                 remaining_bess_capacity_kw,
#                             )

#                             available_grid_capacity_kw = max(0.0, grid_limit_kw - max(0.0, net_load_kw))
#                             grid_charge_kw = min(grid_charge_kw, available_grid_capacity_kw)

#                             if grid_charge_kw > 0:
#                                 decision = "grid_charge"
#                                 if not is_low_tariff and urgent_to_charge:
#                                     description = (
#                                         f"URGENT: Breaching low tariff to suffice peak reserve. "
#                                         f"Grid charges BESS {grid_charge_kw:.0f} kW."
#                                     )
#                                 else:
#                                     description = (
#                                         f"Low tariff period. Grid charges BESS {grid_charge_kw:.0f} kW "
#                                         f"for upcoming peak preparation."
#                                     )

#             # ======================================================
#             # UPDATE BATTERY ENERGY & CYCLE COUNT
#             # ======================================================
#             total_charge_kw = charge_kw + grid_charge_kw
            
#             step_charge_mwh = 0.0
#             step_discharge_mwh = 0.0

#             if total_charge_kw > 0:
#                 step_charge_mwh = (
#                     total_charge_kw * self.interval_hours / 1000.0 * self.charge_efficiency
#                 )
#                 energy_mwh += step_charge_mwh

#             if discharge_kw > 0:
#                 step_discharge_mwh = (
#                     discharge_kw * self.interval_hours / 1000.0 / self.discharge_efficiency
#                 )
#                 energy_mwh -= step_discharge_mwh

#             self.cumulative_charge_mwh += step_charge_mwh
#             self.cumulative_discharge_mwh += step_discharge_mwh

#             energy_mwh = self._clamp(
#                 energy_mwh, 
#                 self.hard_min_energy_mwh, 
#                 self.hard_max_energy_mwh
#             )
#             soc_percent = self._soc(energy_mwh)

#             throughput_cycles = (
#                 (self.cumulative_charge_mwh + self.cumulative_discharge_mwh) 
#                 / (2 * self.nominal_usable_capacity_mwh)
#             )

#             grid_import_kw = max(0.0, net_load_kw + grid_charge_kw + charge_kw - discharge_kw)
#             grid_limit_violation_kw = max(0.0, grid_import_kw - grid_limit_kw)

#             results.append({
#                 "time_stamp": timestamp,
#                 "load_kw": round(load_kw, 2),
#                 "pv_kw": round(pv_kw, 2),
#                 "net_load_kw": round(net_load_kw, 2),
#                 "grid_import_limit_kw": round(grid_limit_kw, 2),
#                 "tariff_per_kw": round(tariff_per_kw, 4),
#                 "co_relation_percent": round(co_relation_percent, 2),
#                 "load_state": load_state,
#                 "decision": decision,
#                 "charge_kw": round(charge_kw, 2),
#                 "grid_charge_kw": round(grid_charge_kw, 2),
#                 "discharge_kw": round(discharge_kw, 2),
#                 "grid_import_kw": round(grid_import_kw, 2),
#                 "grid_limit_violation_kw": round(grid_limit_violation_kw, 2),
#                 "soc_percent": round(soc_percent, 2),
#                 "battery_energy_mwh": round(energy_mwh, 3),
#                 "throughput_cycles": round(throughput_cycles, 4),
#                 "upcoming_peak_timestamp": next_peak_ts,
#                 "upcoming_peak_required_kw": round(max_required_kw, 2),
#                 "active_peak_energy_mwh": round(active_peak_energy_mwh, 3),
#                 "violation_energy_mwh": round(violation_energy_mwh, 3),
#                 "risk_reserve_mwh": round(reserve_info["risk_reserve_mwh"], 3),
#                 "required_reserve_mwh": round(required_reserve_mwh, 3),
#                 "description": description,
#             })

#         return results