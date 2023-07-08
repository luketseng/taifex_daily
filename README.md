taifex_daily
-------
This is project can help for backup history of taifex.
http://zane.myftp.org/~luke/taifex_web/

How to install
--------------
1. You can install PyDrive with regular ``pip`` command.

    $ pip install PyDrive

    reference url:
    https://github.com/gsuitedevs/PyDrive/blob/master/README.rst

2. Go to APIs Console and make your own project and create certificate.

    https://console.cloud.google.com/apis/credentials

3. Download JSON certificate and rename to "client_secret.json" in ~/taifex_daily/device/

4. FCT_DB.db store history of taifex in ~/taifex_daily/ (share FCT_DB.db later)

PS. maybe you need to install "wget" with ``pip``

    $ pip install wget

Example
--------------

    $ ./mining_rpt.py -d 20190101-20190102
    $ ./mining_rpt.py -e TX 300 -d 20190101
