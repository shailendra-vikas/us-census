
from optparse import OptionParser

parser = OptionParser()
(options, args) = parser.parse_args()


def main():
    pass

def test_download():
    import params_base
    params = params_base.Params()
    download_base = DownloadBase(params)
    download_base.fill()
    pass
 

if __name__=='__main__':
    main()
