from concurrent.futures import ThreadPoolExecutor
import pandas as pd

import bigquery
import event
import data_from_cloud


statement = (f"SELECT * FROM `i-azimuth-215919.dataloggers.events` "
             f"WHERE `avgCurrent` < 0 "
             f"ORDER BY `timestamp` DESC")
events = bigquery.df_from_query(statement)

print(len(events.index), "# events")

worker = ThreadPoolExecutor(1)
n_fixed = 0
futures = list()

for i, row in enumerate(events.itertuples()):
    print(i, 'of', len(events.index))
    local_filename = data_from_cloud.download_blob_by_name(row.filename)
    try:
        df = pd.read_csv(local_filename)
    except (pd.errors.EmptyDataError, ValueError) as e:
        print(local_filename, 'error', e)
        continue
    df = event.harmonize_columns(df)
    df = event.fix_energy_values(df)

    try:
        old_energy = round(float(events.iloc[row.Index]['energy']), 2)
    except KeyError as e:
        print(row.filename, e)
        continue

    fixed_energy = round(float(df['energy (Wh)'].iloc[-1]), 2)
    if fixed_energy != old_energy:
        print(f'Energy fixed from {old_energy} to {fixed_energy} in {row.filename}')

        # https://stackoverflow.com/a/76113054/2666454
        future = worker.submit(
            bigquery.update,
            'events',
            f"SET `energy` = {fixed_energy} WHERE `filename` = '{row.filename}' "
        )
        print(future.result())
        futures.append(future)
        n_fixed = n_fixed + 1

print(n_fixed, 'n_fixed')
worker.shutdown(wait = False, cancel_futures = False)
for future in futures:
    print(future.result())