#!/usr/bin/python
# -*- coding: utf-8 -*-
import sys, os
import logging
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive

'''
Homepage: https://pypi.python.org/pypi/PyDrive
Documentation: Official documentation on GitHub pages
Github: https://github.com/googledrive/PyDrive
Quickstart: https://pythonhosted.org/PyDrive/quickstart.html
'''

def get_logging_moduel():
    global logger
    logger=logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    console=logging.StreamHandler(sys.stdout)
    console.setLevel(logging.INFO)
    formatter=logging.Formatter('%(asctime)s | %(name)s - %(levelname)s - %(message)s')
    console.setFormatter(formatter)
    logger.addHandler(console)

class gdrive():
    '''init logging'''
    get_logging_moduel()

    path=os.path.dirname(__file__)
    item_obj={'rpt': None, 'fut_rpt': None, 'opt_rpt': None}
    drive=None

    def __init__(self):
        creds_file_path=os.path.join(self.path, "mycreds.txt")
        gauth=GoogleAuth()
        # Try to load saved client credentials
        gauth.LoadCredentialsFile(creds_file_path)
        if gauth.credentials is None:
            # Authenticate if they're not there
            gauth.LocalWebserverAuth()
        elif gauth.access_token_expired:
            # Refresh them if expired
            gauth.Refresh()
        else:
            # Initialize the saved creds
            gauth.Authorize()
        # Save the current credentials to a file
        gauth.SaveCredentialsFile(creds_file_path)
        self.drive=GoogleDrive(gauth)

        for key in self.item_obj.keys():
            self.item_obj[key]=self.getObjByName(key)
            logger.info("id of '{}' dir: {}".format(self.item_obj[key]['title'], self.item_obj[key]['id']))

    def getObjByName(self, name):
        ## get obj id by file name
        query="title='{}' and trashed=false".format(name)
        query_list=self.drive.ListFile({'q': query}).GetList()
        if len(query_list)==1:
            logger.debug("getObjByName: id({}) of '{}' item".format(query_list[0]['id'], name))
            return query_list[0]
        elif len(query_list)==0:
            logger.warning("'{}' item not found item in gdrive".format(name))
        else:
            logger.warning("'{}' item not only in gdrive".format(name))
        return None

    def GetContentFile(self, name, path, _mimetype='application/zip'):
        obj=self.getObjByName(name)
        if obj!=None:
            ## GetContentFile(): download file(filepath) from gdrive(target_id)
            file_obj=self.drive.CreateFile({'id': obj['id']})
            logger.debug('Downloading file: {} from gdrive'.format(file_obj['title']))
            ## Save Drive file as a local file
            file_obj.GetContentFile(path, mimetype=_mimetype)
            logger.info('Download done: path={}'.format(path))
        else:
            logger.error("'{}' item not found in gdrive".format(name))

    def UploadFile(self, path, name, _mimetype='application/zip', recover=True):
        ## Upload(): upload file(file_path) to grive(name)
        ext_obj=self.item_obj.get(name, self.getObjByName(name))

        if ext_obj!=None:
            # delete file if file exist in gdrive
            fname=os.path.basename(path)
            obj=self.getObjByName(fname)
            if obj!=None and recover:
                logger.debug('file({}) exist in gdrive: id={}'.format(obj['title'], obj['id']))
                obj.Delete()
                logger.info('Delete file({}) in gdrive'.format(obj['title']))

            # Create GoogleDriveFile instance
            file_obj=self.drive.CreateFile({"title": fname, "parents": [{"id": ext_obj['id']}]})
            file_obj.SetContentFile(path)
            file_obj.Upload()
            logger.info("upload local file({}) with mimeType {} to '{}' item of gdrive".format(file_obj['title'], file_obj['mimeType'], ext_obj['title']))
        else:
            logger.error("Unexpected error:", sys.exc_info()[0])

if __name__ == '__main__':

    '''funtion library
    #gdrive().getIdByName(name, target_id):
    #gdrive().GetContentFile(file_path, target_id, _mimetype='application/zip'):
    #gdrive().UploadFile(file_path, target_id, _mimetype='application/zip'):
    '''
    # Auto-iterate through all files in the root folder.
    gdrive=gdrive()
    gdrive.getObjByName('Daily_2019_11_06.zip')
    sys.exit()
    drive=gdrive.drive
    #query="'{!s}' in parents and trashed=false".format(self.fut_rpt_id)
    query="'{!s}' in parents and title='Daily_2019_11_06.zip' and trashed=false".format(gdrive.fut_rpt_id)
    #query="title='MaxDrawDown_TXF1.xlsm' and trashed=false"
    query2="'root' in parents and trashed=false"#, 'maxResults': 10
    file_list = drive.ListFile({'q': query, 'maxResults': 10}).GetList()
    for f_obj in file_list:
        #if file1['title']=='fut_rpt':
        print('title: %s, id: %s' % (f_obj['title'], f_obj['id']))
    # Create GoogleDriveFile instance
    # file2 = drive.CreateFile( {'title':'Daily_2018_08_10.zip', 'mimeType':'application/zip',
    #    V    "parents": [{"id": tgt_folder_id}]})
