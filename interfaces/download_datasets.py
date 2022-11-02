import sys
import logging
from optparse import OptionParser

from data_us_census import download_base as dbase

parser = OptionParser()
parser.add_option('--params', action='store', dest ='param', help='params file', default='interfaces.params_base')
parser.add_option('--savecsv', action='store_true', dest ='save_csv', help='save downloaded file as csv', default=False)
parser.add_option('--savetable', action='store_true', dest ='save_table', help='save downloaded file into the table', default=False)
(options, args) = parser.parse_args()


def main():
    logging.basicConfig(level=logging.DEBUG)
    logging.info('Importing {}'.format(options.param))
    __import__(options.param)
    myparam = sys.modules[options.param]
    params = myparam.Params()

    download_base = dbase.DownloadBase(params)
    filled = download_base.fill(save_csv=options.save_csv, save_table=options.save_table)
    if not filled:
        download_base.fetch()
        download_base.fill(save_csv=options.save_csv, save_table=options.save_table)


 

if __name__=='__main__':
    main()
