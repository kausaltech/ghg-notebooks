import csv
import pandas as pd
from collections import OrderedDict
import quilt


# get_pcaxis_file('E21_kaup_rakennus_tyypeittain', '../DATABASE/02_ENERGIA/4_KAUPUNGIN_OMISTAMAT/')
def _parse_csv(path):
    all_rows = []
    with open(path, 'r', encoding='iso8859-1') as csvf:
        reader = csv.reader(csvf, delimiter=';', quotechar='"')
        title = next(reader)[0]
        # Empty row
        next(reader)
        field_names = next(reader)
        field_names[0] = 'Rakennustyyppi'
        field_names[1] = 'Vuosi'
        current_type = None
        for row in reader:
            if not len(row):
                break
            data = OrderedDict(list(zip(field_names, row)))
            title = row[0].strip()
            if title:
                current_type = title
                continue

            data['Rakennustyyppi'] = current_type
            for key, val in data.items():
                if key == 'Rakennustyyppi':
                    continue
                if key == 'Vuosi':
                    data[key] = int(val)
                    continue
                if val in ('..', '-'):
                    data[key] = pd.np.nan
                else:
                    data[key] = float(data[key])
            all_rows.append(data)

    df = pd.DataFrame(all_rows).set_index('Rakennustyyppi')
    return df


if __name__ == '__main__':
    df = _parse_csv('ytdata/E21_kaup_rakennus_tyypeittain18121617144011544976023865s.csv')
    quilt.build('jyrjola/ymparistotilastot/e21_kaup_rakennus_tyypeittain', df)
    quilt.push('jyrjola/ymparistotilastot/e21_kaup_rakennus_tyypeittain', is_public=True)
