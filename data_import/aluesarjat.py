import os
import re
import requests
import quilt
from datetime import datetime, timedelta
from pandas_pcaxis import PxParser
from pandas_pcaxis.pxweb_api import PXWebAPI

from utils.quilt import update_node_from_pcaxis



def update_quilt(quilt_path):
    import os
    import glob
    import settings

    def upload_px_dataset(root_node, file):
        fname = os.path.splitext(os.path.basename(file))[0]
        if 'hginseutu' not in fname.lower() and 'UM' not in fname and 'hki' not in fname.lower():
            return

        print(fname)
        if re.match('^[0-9]', fname):
            # If the name begins with a number, prefix it with an letter
            # to make it a legal Python identifier.
            fname = 'z' + fname

        fname = fname.replace('-', '_').lower()

        content = open(file, 'r', encoding='windows-1252').read()
        parser = PxParser()
        try:
            file = parser.parse(content)
        except Exception as e:
            print(e)
            return

        now = datetime.now()

        parser = PxParser()
        file = parser.parse(content)
        now = datetime.now()
        from pprint import pprint
        #if 'last_updated' not in file.meta or (now - file.meta['last_updated']) > timedelta(days=2 * 365):
        #    return

        print("\t%s" % file.meta['contents'])

        if root_node:
            quilt_target = root_node
        else:
            quilt_target = quilt_path

        node = update_node_from_pcaxis(quilt_target, fname, file)
        return node

    SKIP_FILES = []

    data_dir = os.path.join(settings.DATA_DIR, 'aluesarjat')
    files = glob.glob('%s/*.px' % data_dir)
    skip_until = None

    root_node = None
    for file in files:
        if 'A01S_HKI_Rak' not in file:
            continue
        if skip_until:
            if skip_until not in file:
                continue
            skip_until = None

        skip = False
        for sf in SKIP_FILES:
            if sf in file:
                skip = True
                break
        if skip:
            continue

        ret = upload_px_dataset(root_node, file)
        if ret:
            root_node = ret

    assert root_node
    quilt.build(quilt_path, root_node)
    quilt.push(quilt_path, is_public=True)


if __name__ == '__main__':
    import settings
    import requests_cache
    requests_cache.install_cache('aluesarjat')

    from utils.dvc import update_dataset
    api = PXWebAPI('http://api.aluesarjat.fi', 'fi')
    p = 'Helsingin seudun tilastot/Helsingin seutu/Väestö/Väestöennusteet/Hginseutu_VA_VE01_Vaestoennuste_PKS.px'
    pxf = api.get_table(p)
    path = 'jyrjola/aluesarjat'
    fname = 'hginseutu_va_ve01_vaestoennuste_pks'
    update_dataset('helsinki/aluesarjat/hginseutu_va_ve01_vaestoennuste_pks', pxf)
    #p = 'Ympäristötilastot/12_Ymparistotalous/1_Taloudelliset%20tunnusluvut/T1_talousluvut.px'
    #p = 'Helsingin%20seudun%20tilastot/Helsingin%20seutu/V%C3%A4est%C3%B6/V%C3%A4est%C3%B6ennusteet/Hginseutu_VA_VE01_Vaestoennuste_PKS.px'
    #download_all_datasets()
    # update_quilt('jyrjola/aluesarjat')
