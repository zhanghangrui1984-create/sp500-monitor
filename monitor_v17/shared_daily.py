"""Package only dated public market observations from this run's verified responses."""
from hashlib import sha256
from pathlib import Path
from zipfile import ZipFile, ZIP_DEFLATED
import json
import pandas as pd

ETFS = ('XLF', 'SPY', 'XLY', 'XLI', 'XLB', 'XLP', 'XLV', 'XLU')
MACRO = ('NFCI', 'CFNAIDIFF', 'CPIAUCSL', 'DCPF3M', 'DTB3')

def publish(data_dir, sources, errors, asof):
    manifest = {'market_date': str(asof.date()), 'series': {}}
    temp = data_dir/'shared_daily.pending'
    with ZipFile(temp, 'w', ZIP_DEFLATED) as out:
        for code in (*ETFS, '^NDX', '^GSPC', *MACRO):
            key = code if code in MACRO else 'price_'+code
            if key in errors or key not in sources:
                manifest['series'][code] = {'status': 'unavailable'}
                continue
            values, meta = sources[key]
            raw = (data_dir.parents[1]/meta['raw_file']).read_bytes()
            if sha256(raw).hexdigest() != meta['sha256']:
                raise ValueError('Shared input hash mismatch '+code)
            source = json.loads(raw)
            if code in MACRO:
                # Before model-specific release alignment, same market-date FRED vintage.
                rows = source['observations']
                values = pd.Series({r['date']: float(r['value']) for r in rows if r['value'] != '.'})
            elif code.startswith('^'):
                # Preserve native precision for consumers. SP uses its own rounding.
                chart = source['chart']['result'][0]
                if chart['meta']['symbol'] != code:
                    raise ValueError('Shared symbol mismatch')
                idx = pd.to_datetime(chart['timestamp'], unit='s', utc=True).tz_convert('America/New_York').strftime('%Y-%m-%d')
                values = pd.Series(chart['indicators']['quote'][0]['close'], index=idx).loc[:str(asof.date())]
                if pd.isna(values.get(str(asof.date()))) and meta.get('close_completion'):
                    values.loc[str(asof.date())] = chart['meta']['regularMarketPrice']
            body = values.rename('value').to_csv(index_label='date').encode()
            out.writestr(code+'.csv', body)
            manifest['series'][code] = {'status':'available','sha256':sha256(body).hexdigest(),
                                        'source_sha256':meta['sha256'],'retrieved_at':meta['retrieved_at']}
        out.writestr('manifest.json', json.dumps(manifest))
    temp.replace(data_dir/'shared_daily.zip')

