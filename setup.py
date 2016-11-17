from distutils.core import setup

setup(
    name = 'r1-dw-arte-app',
    version='0.0.1',
    description='Programmable bidder app',
    author='Basanth Roy',
    author_email='broy@radiumone.com',
    packages = [
        '.',
        'python.arte.util',
        'python.programmable_bidder',
        'python.programmable_bidder.config',
        'python.programmable_bidder.nqdq'
        ],
    url = 'ssh://git@git.dev.dw.sc.gwallet.com:7999/scm/dw/r1-dw-arte-app.git',
    #download_url = 'ssh://git@git.dev.dw.sc.gwallet.comi:7999/scm/dw/r1-dw-arte-app.git/tarball/0.1',
    keywords = ['arte', 'enqueue', 'dequeue', 'programmable_bidder'],
    classifiers = [],
    scripts=
        ["python/programmable_bidder/nqdq/start_pb_dequeue_for_prod.sh",
         "python/programmable_bidder/nqdq/start_pb_enqueue_for_prod.sh"
         ]
)