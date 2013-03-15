import csv
import os


class QuotaChangeModelRunner(object):
    def __init__(self):
        self.trips = {}
        self.acl = {}

        self.ingest_inputs()

        # process trips to throw out trips w/ low gfish rats.

    def ingest_inputs(self):
        # catch.
        #"MULT_YEAR","mri","TRIP_ID","spec","stock","stock_id1","_TYPE_","_FREQ_","land","disc"
        #"2010","2192","2010_100409_0001","non_gfish","all","23","0","1","0","."
        catch_file = "/home/adorsk/projects/noaa/qcm/test_data/catch.csv"
        with open(catch_file, "rb") as f:
            reader = csv.DictReader(f)
            for row in reader:
                trip_id = row.get('TRIP_ID')
                if trip_id is None: continue

                # Get or initialize trip.
                trip = self.trips.setdefault(trip_id, {
                    'mri': row['mri'],
                    'stock_catches': {},
                    'spec_catches': {},
                })

                # Ignore trips that don't have 'spec.'
                if row.get('spec') is None: continue

                # Get landed/discard (default=0 for empty or '.')
                for field in ['land', 'disc']:
                    try: row[field] = float(row['field'])
                    except: row[field] = 0

                # Calculate catch and ignore records with no catch.
                catch = row['land'] + row['disc']
                if not catch: continue

                # Add catch to stock catches
                stock_id = row['stock_id1']
                stock_catches = trip['stock_catches']
                stock_catches[stock_id] = stock_catches.setdefault(
                    stock_id, 0) + catch

                # Add to spec catches.
                spec = row['spec']
                if spec != 'non_gfish':
                    spec = 'gfish'
                spec_catches[spec] = spec_catches.setdefault(spec, 0) + catch

        # costs.
        #"TRIP_ID","trip_revenue","trip_cost","_TYPE_","_FREQ_","quota_cost","sector_cost","variable_cost"
        #"2010_100409_0001","0",".","0","1","0","0","."
        cost_file = "/home/adorsk/projects/noaa/qcm/test_data/costs.csv"
        with open(catch_file, "rb") as f:
            reader = csv.DictReader(f)
            for row in reader:
                trip_id = row.get('TRIP_ID')
                if trip_id is None: continue
                trip = self.trips.setdefault(trip_id, {})
                trip.update(row)

        # acl.
        #"spec","stock","stock_id1","limit_1","limit_2","limit_3"
        #"am_plaice","all","7","6058240.8","0","0"
        acl_file = "/home/adorsk/projects/noaa/qcm/test_data/acl.csv"
        with open(acl_file, "rb") as f:
            reader = csv.DictReader(f)
            for row in reader:
                stock_id = row.get('stock_id1')
                if stock_id is None: continue
                stock = self.acl.setdefault(trip_id, {})
                stock.update(row)

    def calculate_p_scores(self):
        pass

    def run_simulations(self):
        pass


def main():
    qcmr = QuotaChangeModelRunner()

if __name__ == '__main__':
    main()
