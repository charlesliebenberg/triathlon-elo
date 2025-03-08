"""
Glicko-2 Rating System

Implementation based on the work by Ryan Kirkman and Mark Glickman's
paper "Glicko-2: A Rating System for Chess and Similar Games".

Original implementation (c) 2009 Ryan Kirkman under MIT License
Modified for the triathlon database project.

This module contains both the original Player class and additional functions
adapted for use with the triathlon database project's rating system.
"""

import math

# Constants for the Glicko-2 system
SCALE_FACTOR = 173.7178
INITIAL_RATING = 1500
INITIAL_RD = 350
INITIAL_VOLATILITY = 0.06
TAU = 0.5  # System constant, constraining volatility changes
EPSILON = 0.000001  # Convergence tolerance for volatility iteration

class Player:
    # Class attribute
    # The system constant, which constrains
    # the change in volatility over time.
    _tau = 0.5

    def getRating(self):
        return (self.__rating * 173.7178) + 1500 

    def setRating(self, rating):
        self.__rating = (rating - 1500) / 173.7178

    rating = property(getRating, setRating)

    def getRd(self):
        return self.__rd * 173.7178

    def setRd(self, rd):
        self.__rd = rd / 173.7178

    rd = property(getRd, setRd)
     
    def __init__(self, rating = 1500, rd = 350, vol = 0.06):
        # For testing purposes, preload the values
        # assigned to an unrated player.
        self.setRating(rating)
        self.setRd(rd)
        self.vol = vol
            
    def _preRatingRD(self):
        """ Calculates and updates the player's rating deviation for the
        beginning of a rating period.
        
        preRatingRD() -> None
        
        """
        self.__rd = math.sqrt(math.pow(self.__rd, 2) + math.pow(self.vol, 2))
        
    def update_player(self, rating_list, RD_list, outcome_list):
        """ Calculates the new rating and rating deviation of the player.
        
        update_player(list[int], list[int], list[bool]) -> None
        
        """
        # Convert the rating and rating deviation values for internal use.
        rating_list = [(x - 1500) / 173.7178 for x in rating_list]
        RD_list = [x / 173.7178 for x in RD_list]

        v = self._v(rating_list, RD_list)
        self.vol = self._newVol(rating_list, RD_list, outcome_list, v)
        self._preRatingRD()
        
        self.__rd = 1 / math.sqrt((1 / math.pow(self.__rd, 2)) + (1 / v))
        
        tempSum = 0
        for i in range(len(rating_list)):
            tempSum += self._g(RD_list[i]) * \
                       (outcome_list[i] - self._E(rating_list[i], RD_list[i]))
        self.__rating += math.pow(self.__rd, 2) * tempSum
        
    #step 5        
    def _newVol(self, rating_list, RD_list, outcome_list, v):
        """ Calculating the new volatility as per the Glicko2 system. 
        
        Updated for Feb 22, 2012 revision. -Leo
        
        _newVol(list, list, list, float) -> float
        
        """
        #step 1
        a = math.log(self.vol**2)
        eps = 0.000001
        A = a
        
        #step 2
        B = None
        delta = self._delta(rating_list, RD_list, outcome_list, v)
        tau = self._tau
        if (delta ** 2)  > ((self.__rd**2) + v):
          B = math.log(delta**2 - self.__rd**2 - v)
        else:        
          k = 1
          while self._f(a - k * math.sqrt(tau**2), delta, v, a) < 0:
            k = k + 1
          B = a - k * math.sqrt(tau **2)
        
        #step 3
        fA = self._f(A, delta, v, a)
        fB = self._f(B, delta, v, a)
        
        #step 4
        while math.fabs(B - A) > eps:
          #a
          C = A + ((A - B) * fA)/(fB - fA)
          fC = self._f(C, delta, v, a)
          #b
          if fC * fB <= 0:
            A = B
            fA = fB
          else:
            fA = fA/2.0
          #c
          B = C
          fB = fC
        
        #step 5
        return math.exp(A / 2)
        
    def _f(self, x, delta, v, a):
      ex = math.exp(x)
      num1 = ex * (delta**2 - self.__rating**2 - v - ex)
      denom1 = 2 * ((self.__rating**2 + v + ex)**2)
      return  (num1 / denom1) - ((x - a) / (self._tau**2))
        
    def _delta(self, rating_list, RD_list, outcome_list, v):
        """ The delta function of the Glicko2 system.
        
        _delta(list, list, list) -> float
        
        """
        tempSum = 0
        for i in range(len(rating_list)):
            tempSum += self._g(RD_list[i]) * (outcome_list[i] - self._E(rating_list[i], RD_list[i]))
        return v * tempSum
        
    def _v(self, rating_list, RD_list):
        """ The v function of the Glicko2 system.
        
        _v(list[int], list[int]) -> float
        
        """
        tempSum = 0
        for i in range(len(rating_list)):
            tempE = self._E(rating_list[i], RD_list[i])
            tempSum += math.pow(self._g(RD_list[i]), 2) * tempE * (1 - tempE)
        return 1 / tempSum
        
    def _E(self, p2rating, p2RD):
        """ The Glicko E function.
        
        _E(int) -> float
        
        """
        return 1 / (1 + math.exp(-1 * self._g(p2RD) * \
                                 (self.__rating - p2rating)))
        
    def _g(self, RD):
        """ The Glicko2 g(RD) function.
        
        _g() -> float
        
        """
        return 1 / math.sqrt(1 + 3 * math.pow(RD, 2) / math.pow(math.pi, 2))
        
    def did_not_compete(self):
        """ Applies Step 6 of the algorithm. Use this for
        players who did not compete in the rating period.

        did_not_compete() -> None
        
        """
        self._preRatingRD()

# Helper Functions for integrating with data_analyzer.py

def g(phi):
    """
    Compute the g function based on opponent's rating deviation.
    
    Args:
        phi (float): Rating deviation in Glicko-2 internal scale
        
    Returns:
        float: The g value
    """
    # Ensure phi is not too small to prevent numerical issues
    phi = max(phi, 0.0001)
    return 1 / math.sqrt(1 + 3 * phi**2 / math.pi**2)

def E(mu, mu_j, phi_j):
    """
    Compute the expected score against an opponent.
    
    Args:
        mu (float): Player's rating in Glicko-2 internal scale
        mu_j (float): Opponent's rating in Glicko-2 internal scale
        phi_j (float): Opponent's rating deviation in Glicko-2 internal scale
        
    Returns:
        float: Expected score (between 0 and 1)
    """
    # Calculate the exponent value with safety check to prevent overflow
    exponent = -g(phi_j) * (mu - mu_j)
    
    # Handle extreme cases to prevent overflow
    if exponent > 700:  # exp(700) is close to the limit for float
        return 0.0      # For very large negative differences, expected score approaches 0
    elif exponent < -700:
        return 1.0      # For very large positive differences, expected score approaches 1
    
    # Normal case - calculate normally
    return 1 / (1 + math.exp(exponent))

def update_volatility(phi, v, delta, sigma, tau=TAU, epsilon=EPSILON):
    """
    Update volatility using the Illinois algorithm.
    
    Args:
        phi (float): Rating deviation in Glicko-2 internal scale
        v (float): Variance of the change in rating
        delta (float): Expected change in rating
        sigma (float): Current volatility
        tau (float): System constant
        epsilon (float): Convergence tolerance
        
    Returns:
        float: Updated volatility
    """
    # Handle special cases to prevent numerical issues
    if v == float('inf') or abs(delta) < 0.0001:
        return sigma  # No change in volatility when v is infinite or delta is negligible
    
    a = math.log(sigma**2)
    
    # Define the optimization function
    def f(x):
        exp_x = math.exp(x)
        # Prevent division by zero or extreme values
        denom = max(2 * (phi**2 + v + exp_x)**2, 1e-10)
        return (exp_x * (delta**2 - phi**2 - v - exp_x)) / denom - (x - a) / tau**2
    
    # Set initial bounds
    A = a
    
    try:
        if delta**2 > phi**2 + v:
            B = math.log(delta**2 - phi**2 - v)
        else:
            k = 1
            while f(a - k * tau) < 0 and k < 100:  # Add limit to prevent infinite loop
                k += 1
            B = a - k * tau
    except (ValueError, OverflowError):
        # Fallback if we encounter numerical issues
        return sigma
    
    # Limit iteration count to prevent excessive computation
    iteration_count = 0
    max_iterations = 100
    
    f_A = f(A)
    f_B = f(B)
    
    # Check if we can continue with the algorithm
    if not (math.isfinite(f_A) and math.isfinite(f_B)):
        return sigma
    
    while abs(B - A) > epsilon and iteration_count < max_iterations:
        # Prevent division by zero
        if abs(f_B - f_A) < 1e-10:
            C = (A + B) / 2  # Fallback to bisection if slope is too small
        else:
            C = A + (A - B) * f_A / (f_B - f_A)
        
        f_C = f(C)
        
        # Check for numerical stability
        if not math.isfinite(f_C):
            # Use bisection as fallback
            C = (A + B) / 2
            f_C = f(C)
            if not math.isfinite(f_C):
                # If still unstable, return original volatility
                return sigma
        
        if f_C * f_B <= 0:
            A = B
            f_A = f_B
        else:
            f_A = f_A / 2
        
        B = C
        f_B = f_C
        
        iteration_count += 1
    
    # If we hit max iterations, use the midpoint as a fallback
    if iteration_count >= max_iterations:
        result = math.exp((A + B) / 4)  # Taking sqrt of exp((A+B)/2)
    else:
        result = math.exp(A / 2)
    
    # Make sure the result is within reasonable bounds
    if not (0.0001 <= result <= 1.0):
        return sigma  # Return original if result is outside reasonable range
    
    return result

def calculate_elo_ratings(results_data, athletes_data):
    """
    Calculate Glicko-2 ratings for athletes based on race results (with MONTHLY rating periods).
    All events/results in the same calendar month are grouped into a single rating period and
    updated simultaneously, i.e. each athlete sees only the *old* ratings of that month's opponents.
    
    Args:
        results_data (dict): Results data with events and results
        athletes_data (dict): Athletes data with details
        
    Returns:
        dict: Elo ratings data for all athletes
    """
    from datetime import datetime
    from collections import defaultdict

    # --------------------------------------------------------------------
    # 1) Initialize ratings
    # --------------------------------------------------------------------
    elo_ratings = {}
    for athlete_id in athletes_data.get("athletes", {}):
        elo_ratings[athlete_id] = {
            "initial": INITIAL_RATING,
            "current": INITIAL_RATING,
            "current_rd": INITIAL_RD,
            "current_volatility": INITIAL_VOLATILITY,
            "history": [],
            "races_completed": 0
        }
    
    # Build an event lookup so we can fetch event date/name
    event_lookup = {}
    for event_id, event_data in results_data.get("events", {}).items():
        event_date = event_data.get("date")
        event_name = event_data.get("title", "")
        if event_date:
            event_lookup[event_id] = {
                "date": event_date,
                "name": event_name
            }
            try:
                event_lookup[int(event_id)] = {
                    "date": event_date,
                    "name": event_name
                }
            except:
                pass

    # Pull out all results with date info
    # Store as (datetime_obj, result_dict)
    dated_results = []
    for r in results_data.get("results", []):
        e_id = r.get("event_id")
        if not e_id or e_id not in event_lookup:
            continue
        dt_str = event_lookup[e_id]["date"]
        try:
            dt_obj = datetime.fromisoformat(dt_str[:10])  # e.g. "YYYY-MM-DD"
        except ValueError:
            # If format is not standard, skip or parse differently
            continue
        r_copy = r.copy()
        r_copy["event_date"] = dt_obj
        r_copy["event_name"] = event_lookup[e_id]["name"]
        dated_results.append((dt_obj, r_copy))

    # Sort results chronologically
    dated_results.sort(key=lambda x: x[0])

    # --------------------------------------------------------------------
    # 2) Group results by month
    # --------------------------------------------------------------------
    monthly_groups = defaultdict(list)

    for dt_obj, r_item in dated_results:
        # month_key as (year, month)
        month_key = (dt_obj.year, dt_obj.month)
        monthly_groups[month_key].append(r_item)

    # Sort the month_keys in ascending time
    sorted_month_keys = sorted(monthly_groups.keys())

    # --------------------------------------------------------------------
    # Iterate month by month, applying a single Glicko-2 update
    # --------------------------------------------------------------------
    for month_key in sorted_month_keys:
        # We pick the date for rating-history logging (e.g. last day of that month's results)
        # so we know which date to store in the rating "history."
        these_results = monthly_groups[month_key]
        # If multiple events/days in the same month, we'll store the rating update date
        # as the *latest* date in that month. (You can also choose the earliest if you prefer.)
        rating_period_date = max(r["event_date"] for r in these_results)

        # 2A) Build a dictionary listing every athlete who raced this month.
        #     We'll accumulate partial Glicko sums from each event for each athlete,
        #     then do one final rating update at the end of the month.
        month_participants = {}
        # month_participants[a_id] = {
        #   "old_r": ...
        #   "old_rd": ...
        #   "old_vol": ...
        #   "old_mu": ...
        #   "old_phi": ...
        #   "matches": [ {opponent_phi, s_i, E_i, g_i}, ... ]   # We'll store raw data and sum later
        # }
        
        # Gather participants from all events in this month
        # For each event, we do the usual "sort by finishing position" logic
        # to derive pairwise results, but we won't do a rating update right away.
        # Instead, we'll store them and sum at the end.

        # We'll first group results by (event_id, prog_id) to handle each event's finishing order.
        event_prog_map = defaultdict(list)
        for rr in these_results:
            e_id = rr.get("event_id")
            p_id = rr.get("prog_id")
            if e_id and p_id:
                event_prog_map[(e_id, p_id)].append(rr)

        # For each (event, prog), figure out the finishing order, gather pairwise matches
        for (e_id, pr_id), ev_results in event_prog_map.items():
            # Sort participants by position, with DNF => bottom
            valid_racers = []
            for item in ev_results:
                a_id = item.get("athlete_id")
                if a_id and str(a_id) in elo_ratings:
                    valid_racers.append(item)
            if len(valid_racers) < 2:
                continue

            sorted_racers = sorted(
                valid_racers,
                key=lambda x: (
                    0 if x.get("status", "").upper() != "DNF" else 1,
                    int(x["position"]) if x.get("position") else float("inf")
                )
            )
            
            # Go through each athlete in this event to ensure we load "old" rating info
            for racer in sorted_racers:
                a_id_str = str(racer["athlete_id"])
                if a_id_str not in month_participants:
                    # Pull the current rating from the global dictionary
                    old_r = elo_ratings[a_id_str]["current"]
                    old_rd = elo_ratings[a_id_str]["current_rd"]
                    old_vol = elo_ratings[a_id_str]["current_volatility"]

                    old_mu = (old_r - 1500) / SCALE_FACTOR
                    old_phi = old_rd / SCALE_FACTOR

                    month_participants[a_id_str] = {
                        "old_r": old_r,
                        "old_rd": old_rd,
                        "old_vol": old_vol,
                        "old_mu": old_mu,
                        "old_phi": old_phi,
                        "matches": []
                    }

            # Now create pairwise results
            for i in range(len(sorted_racers)):
                r_i = sorted_racers[i]
                a_i = str(r_i["athlete_id"])
                pos_i = r_i.get("position")
                status_i = r_i.get("status", "").upper()

                for j in range(i+1, len(sorted_racers)):
                    r_j = sorted_racers[j]
                    a_j = str(r_j["athlete_id"])
                    pos_j = r_j.get("position")
                    status_j = r_j.get("status", "").upper()

                    # Score from i's perspective
                    if status_i == "DNF" and status_j == "DNF":
                        s_ij = 0.5
                    elif status_i == "DNF":
                        s_ij = 0.0
                    elif status_j == "DNF":
                        s_ij = 1.0
                    else:
                        s_ij = 1.0 if pos_i < pos_j else 0.0

                    # We also want i's perspective of j, and j's perspective of i
                    mu_j = month_participants[a_j]["old_mu"]
                    phi_j = month_participants[a_j]["old_phi"]
                    g_j_val = g(phi_j)
                    E_ij = E(month_participants[a_i]["old_mu"], mu_j, phi_j)

                    # For j's perspective:
                    # s_ji = 1 - s_ij if we assume no ties beyond DNF logic
                    s_ji = 1.0 - s_ij
                    mu_i = month_participants[a_i]["old_mu"]
                    phi_i = month_participants[a_i]["old_phi"]
                    g_i_val = g(phi_i)
                    E_ji = E(mu_j, mu_i, phi_i)

                    # Store these "match records" so we can sum them up later
                    month_participants[a_i]["matches"].append({
                        "g_op": g_j_val,
                        "E_op": E_ij,
                        "s": s_ij
                    })
                    month_participants[a_j]["matches"].append({
                        "g_op": g_i_val,
                        "E_op": E_ji,
                        "s": s_ji
                    })
        
        # 2B) Now that we have all matchups for the entire month, do a single Glicko update
        new_ratings = {}

        for a_id_str, p_info in month_participants.items():
            mu_i = p_info["old_mu"]
            phi_i = p_info["old_phi"]
            sigma_i = p_info["old_vol"]
            matches_i = p_info["matches"]

            if not matches_i:
                # No matches for this athlete (e.g. only one participant or got filtered out)
                # No change, but we'll do the inactivity step after.
                new_ratings[a_id_str] = (p_info["old_r"], p_info["old_rd"], sigma_i, 0.0)
                continue

            # Step 3: v = 1 / sum( g_op^2 * E_op * (1 - E_op) )
            v_inv = 0.0
            for m in matches_i:
                g_j = m["g_op"]
                E_j = m["E_op"]
                v_inv += (g_j**2) * E_j * (1 - E_j)
            if abs(v_inv) < 1e-12:
                v = float('inf')
            else:
                v = 1 / v_inv
            # Bound v if you still want the clamp from original
            v = min(v, 100.0)

            # Step 4: Delta = v * sum( g_op * (s - E_op) )
            delta_sum = 0.0
            for m in matches_i:
                delta_sum += m["g_op"] * (m["s"] - m["E_op"])
            Delta = v * delta_sum

            # Step 5: New volatility
            sigma_new = update_volatility(phi_i, v, Delta, sigma_i, TAU)
            # If you want to replicate the original clamp, do so:
            sigma_new = max(0.0001, min(sigma_new, 0.15))

            # Step 6: phi_star = sqrt(phi_i^2 + sigma_new^2)
            phi_star = math.sqrt(phi_i**2 + sigma_new**2)

            # Step 7: new phi
            if math.isinf(v):
                phi_new = phi_star
            else:
                phi_new = 1 / math.sqrt((1 / phi_star**2) + (1 / v))

            # new mu
            mu_new = mu_i + (phi_new**2) * delta_sum

            # Convert back to original scale
            r_new = (mu_new * SCALE_FACTOR) + 1500
            rd_new = phi_new * SCALE_FACTOR

            # Keep bounding logic if desired
            rating_change = r_new - p_info["old_r"]
            if rating_change > 100:
                r_new = p_info["old_r"] + 100
            elif rating_change < -100:
                r_new = p_info["old_r"] - 100

            r_new = max(100, min(r_new, 5000))
            rd_new = max(10, min(rd_new, 500))

            new_ratings[a_id_str] = (r_new, rd_new, sigma_new, delta_sum)

        # 2C) Commit the new rating for each participant in this month & log the history
        for a_id_str, p_info in month_participants.items():
            old_r = p_info["old_r"]
            old_rd = p_info["old_rd"]
            old_vol = p_info["old_vol"]
            (r_new, rd_new, sigma_new, delta_sum) = new_ratings[a_id_str]

            # Log a single "monthly update" history entry
            elo_ratings[a_id_str]["history"].append({
                "date": rating_period_date.date().isoformat(),
                "event_id": None,  # Because it's a combined month. You can store any ID or info you prefer.
                "event_name": f"Monthly rating period {month_key[0]}-{month_key[1]:02d}",
                "event_importance": "monthly",
                "prog_id": None,
                "position": None,
                "status": "MONTHLY_UPDATE",
                "old_elo": old_r,
                "old_rd": old_rd,
                "old_volatility": old_vol,
                "new_elo": r_new,
                "new_rd": rd_new,
                "new_volatility": sigma_new,
                "change": r_new - old_r,
                "opponents_faced": len(p_info["matches"]),  # total matches recorded
            })

            # Update global rating info
            elo_ratings[a_id_str]["current"] = r_new
            elo_ratings[a_id_str]["current_rd"] = rd_new
            elo_ratings[a_id_str]["current_volatility"] = sigma_new
            elo_ratings[a_id_str]["races_completed"] += len(p_info["matches"])

        # 2D) For all athletes who did NOT participate this month, increase RD for inactivity
        #     (the normal Glicko-2 step for a rating period in which a player does not compete).
        participants_this_month = set(month_participants.keys())
        all_athlete_ids = set(elo_ratings.keys())
        non_participants = all_athlete_ids - participants_this_month
        for nid in non_participants:
            rd = elo_ratings[nid]["current_rd"]
            sigma = elo_ratings[nid]["current_volatility"]
            # The typical "inactivity" formula: phi* = sqrt(phi^2 + sigma^2)
            # On the original scale => new_rd = sqrt(old_rd^2 + (SCALE_FACTOR*sigma)^2)
            rd_new = math.sqrt(rd**2 + (SCALE_FACTOR * sigma)**2)
            rd_new = max(10, min(rd_new, 500))
            elo_ratings[nid]["current_rd"] = rd_new

    return elo_ratings