import csv
import os
import json
import sys


class QuotaChangeModelRunner(object):
    def __init__(self):
        self.trips = {}
        self.acls = {}
        self.low_buffer = .15
        self.valid_stocks = [str(i) for i in range(1, 17+1)]

        self.ingest_inputs()
        self.process_trips()
        self.calculate_p_scores()

    def ingest_inputs(self):
        self.ingest_catch()
        self.ingest_cost()
        self.ingest_acl()

    def ingest_catch(self):
        #"MULT_YEAR","mri","TRIP_ID","spec","stock","stock_id1","_TYPE_","_FREQ_","land","disc"
        #"2010","2192","2010_100409_0001","non_gfish","all","23","0","1","0","."
        # Read catch records from file, sum on stock and
        # gfish/non_gfish spec.
        catch_file = "/home/adorsk/projects/noaa/qcm/test_data/catch.csv"
        with open(catch_file, "rb") as f:
            reader = csv.DictReader(f)
            for row in reader:
                trip_id = row.get('TRIP_ID')
                
                # Ignore records lacking id or spec.
                if trip_id is None: continue
                if row.get('spec') is None: continue

                # Get landed/discard (default=0 for empty or '.')
                for field in ['land', 'disc']:
                    try: row[field] = float(row[field])
                    except: row[field] = 0

                # Ignore records with no catch.
                catch = row['land'] + row['disc']
                if not catch: continue

                # Get or initialize trip.
                trip = self.trips.setdefault(trip_id, {
                    'trip_id': trip_id,
                    'mri': row['mri'],
                    'stock_catch': {},
                    'spec_totals': {},
                    'stock_p_scores': {},
                })

                # Add catch to stock catch lists.
                stock_id = row['stock_id1']
                trip['stock_catch'][stock_id] = catch

                # Add to spec catch totals.
                spec = row['spec']
                if spec != 'non_gfish':
                    spec = 'gfish'
                trip['spec_totals'][spec] = trip['spec_totals'].setdefault(
                    spec, 0) + catch

    def ingest_cost(self):
        # costs.
        #"TRIP_ID","trip_revenue","trip_cost","_TYPE_","_FREQ_","quota_cost","sector_cost","variable_cost"
        #"2010_100409_0001","0",".","0","1","0","0","."
        cost_file = "/home/adorsk/projects/noaa/qcm/test_data/costs.csv"
        with open(cost_file, "rb") as f:
            reader = csv.DictReader(f)
            for row in reader:
                trip_id = row.get('TRIP_ID')
                trip = self.trips.get(trip_id)
                if not trip:
                    continue
                for field in ['trip_revenue', 'variable_cost']:
                    try: trip[field] = float(row[field])
                    except: trip[field] = 0

    def ingest_acl(self):
        # acl.
        #"spec","stock","stock_id1","limit_1","limit_2","limit_3"
        #"am_plaice","all","7","6058240.8","0","0"
        acl_file = "/home/adorsk/projects/noaa/qcm/test_data/acl.csv"
        with open(acl_file, "rb") as f:
            reader = csv.DictReader(f)
            for row in reader:
                stock_id = row.get('stock_id1')
                if stock_id is None: continue
                if stock_id not in self.valid_stocks: 
                    continue
                stock = self.acls.setdefault(stock_id, {})
                stock.update(row)

    def process_trips(self):
        for trip_id, trip in self.trips.items():
            # Filter out trips that don't have any of the stocks we're 
            # interested in.
            has_valid_stocks = False
            for stock_id in trip['stock_catch']:
                if stock_id in self.valid_stocks:
                    has_valid_stocks = True
                    break
            if not has_valid_stocks:
                del self.trips[trip_id]
                continue

            # Filter out trips that have low gfish catch, 
            # or low groundfish ratios.
            spec_totals = trip['spec_totals']
            gfish = spec_totals.get('gfish', 0)
            non_gfish = spec_totals.get('non_gfish', .1)
            if gfish <= 15 or 1.0 * gfish/non_gfish <= .0075:
                del self.trips[trip_id]
                continue

            # Calculate net revenue.
            trip['netrev'] = trip['trip_revenue'] - trip['variable_cost']

            # Calculate ACE efficiency per stock.
            trip['stock_effics'] = {}
            for stock_id, catch in trip['stock_catch'].items():
                if catch > 0:
                    ace_effic = trip['netrev']/catch
                else:
                    ace_effic = 0
                trip['stock_effics'][stock_id] = ace_effic

    def calculate_p_scores(self):
        """ Calculate probability scores for each trip.
        A p_score represent a trip's probability of occuring.
        A trip's stock p_score is the ratio of the trip's efficiency for 
        that stock to the minimum efficiency for that stock (relative
        efficiency), modified by the distance between the max and min 
        efficiency for that stock.
        A trip's final p_score is the min of the trip's stock p_scores.
        """

        for stock_id, acl in self.acls.items():
            limit = float(acl['limit_1'])

            # Get min, max ace efficiencies for the stock, 
            # from the set of most efficient trips whose combined
            # catch does not exceed the limit.
            def get_ace_effic(trip):
                return trip['stock_effics'].get(stock_id, 0)
            sorted_trips = sorted(self.trips.values(), key=get_ace_effic,
                                  reverse=True)
            if not sorted_trips:
                continue
            max_ace_effic = sorted_trips[0]['stock_effics'].get(stock_id)
            min_ace_effic = None
            cumulative_catch = 0
            for trip in sorted_trips:
                ace_effic = trip['stock_effics'].get(stock_id)
                if ace_effic is None: 
                    continue
                if ace_effic <= 0: 
                    break
                cumulative_catch += trip['stock_catch'].get(stock_id)
                if cumulative_catch >= limit:
                    break
                min_ace_effic = ace_effic
            
            # If invalid min or max, set 0 for p_score for trips that
            # had catches for the stock.
            if max_ace_effic <= 0 or min_ace_effic <= 0:
                for trip in self.trips.values():
                    if trip['stock_catch'].get(stock_id) is not None:
                        trip['stock_p_scores'][stock_id] = 0
                continue

            # Buffer the minimum efficiency.
            buffered_min_ace_effic = min_ace_effic * (1.0 - self.low_buffer)
            # Set the stock p_score for each trip.
            for trip in self.trips.values():
                stock_p_scores = trip['stock_p_scores']
                ace_effic = trip['stock_effics'].get(stock_id)
                if ace_effic is None: 
                    continue
                if ace_effic <= 0:
                    stock_p_score = 0
                else:
                    relative_effic = 1.0 - buffered_min_ace_effic/ace_effic
                    range_modifier = 1.0 - buffered_min_ace_effic/max_ace_effic
                    stock_p_score = max(0, relative_effic/range_modifier)
                stock_p_scores[stock_id] = stock_p_score

        # Set each trip's overall p_score as the min of its stock p_scores.
        for trip in self.trips.values():
            trip['p_score'] = min(trip.get('stock_p_scores').values() or [0])

    def run_simulations(self):
        pass


def main():
    qcmr = QuotaChangeModelRunner()

if __name__ == '__main__':
    main()
