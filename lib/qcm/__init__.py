import csv
import os
import json


class QuotaChangeModelRunner(object):
    def __init__(self):
        self.trips = {}
        self.acl = {}

        self.ingest_inputs()
        self.process_trips()

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
                    'mri': row['mri'],
                    'stock_catches': {},
                    'spec_totals': {},
                })

                # Add catch to stock catch lists.
                stock_id = row['stock_id1']
                trip['stock_catches'].setdefault(stock_id, []).append(catch)

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
                stock = self.acl.setdefault(stock_id, {})
                stock.update(row)

    def process_trips(self):
        for trip_id, trip in self.trips.items():
            # Filter out trips that have low gfish catch, 
            # or low groundfish ratios.
            spec_totals = trip['spec_totals']
            gfish = spec_totals.get('gfish', 0)
            non_gfish = spec_totals.get('non_gfish', .1)
            if gfish < 15 or 1.0 * gfish/non_gfish <= .0075:
                del self.trips[trip_id]
                continue

            # Calculate net revenue.
            trip['netrev'] = trip['trip_revenue'] - trip['variable_cost']

            # Calculate ACE efficiency per stock.
            # Note: each catch list in stock_catches should normally
            # just have one item, but in case there are multiple
            # items, the mean of the valus is used, as per chad's
            # SAS code.
            trip['stock_efficiencies'] = {}
            for stock_id, catches in trip['stock_catches'].items():
                mean_catch = float(sum(catches))/len(catches)
                if mean_catch > 0:
                    ace_effic = trip['netrev']/mean_catch
                    trip['stock_efficiencies'][stock_id] = ace_effic

    def calculate_p_scores(self):
        """ Calculate probability scores for each trip."""
        pass

    def run_simulations(self):
        pass


def main():
    qcmr = QuotaChangeModelRunner()

if __name__ == '__main__':
    main()
