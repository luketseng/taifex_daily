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
    path=os.path.dirname(__file__)
    item_obj={'rpt': None, 'fut_rpt': None, 'opt_rpt': None}
    drive=None

    def __init__(self):
        '''init logging'''
        get_logging_moduel()

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

        for key, obj in self.item_obj.items():
            if not obj:
                obj_req=self.getObjByName(key)
                if obj_req!=None and len(obj_req)<2:
                    self.item_obj[key]=obj_req[0]
                else:
                    assert Fasle, 'obj_req not found or item not only in gdrive'
            logger.info("id of '{}' dir: {}".format(self.item_obj[key]['title'], self.item_obj[key]['id']))

    def getObjByName(self, name):
        ## get obj id by file name
        query="title='{}' and trashed=false".format(name)
        query_list=self.drive.ListFile({'q': query}).GetList()
        if len(query_list)>0:
            if len(query_list)>1:
                logger.warning("'{}' item not only in gdrive".format(name))
            return query_list
        else:
            logger.warning("'{}' item not found item in gdrive".format(name))
        return None

    def GetContentFile(self, name, path, _mimetype='application/zip'):
        obj_req=self.getObjByName(name)
        if obj_req!=None and len(obj_req)<2:
            ## GetContentFile(): download file(filepath) from gdrive(target_id)
            file_obj=self.drive.CreateFile({'id': obj_req[0]['id']})
            logger.info("Downloading '{}' from gdrive".format(file_obj['title']))
            ## Save Drive file as a local file
            file_obj.GetContentFile(path, mimetype=_mimetype)
            logger.info("Finish to Download and save to '{}'".format(path))
        else:
            logger.error("'{}' item not found or not only one in gdrive".format(name))
            assert False, "'{}' item not found or not only one in gdrive".format(name)

    def UploadFile(self, path, name, _mimetype='application/zip', recover=True):
        ## Upload(): upload file(file_path) to grive(name)
        dst_obj=self.item_obj.get(name, None)

        if dst_obj!=None:
            # delete file if file exist in gdrive
            fname=os.path.basename(path)
            obj=self.getObjByName(fname)
            if obj!=None and recover:
                for i in range(len(obj)):
                    logger.info('file({}) exist in gdrive: id={}'.format(obj[i]['title'], obj[i]['id']))
                    obj[i].Delete()
                logger.info('Delete all file({}) in gdrive'.format(fname))

            # Create GoogleDriveFile instance
            file_obj=self.drive.CreateFile({"title": fname, "parents": [{"id": dst_obj['id']}]})
            file_obj.SetContentFile(path)
            file_obj.Upload()
            logger.info("upload local file({}) with mimeType {} to '{}' item of gdrive".format(file_obj['title'], file_obj['mimeType'], dst_obj['title']))
        else:
            logger.error("Unexpected error:", sys.exc_info()[0])
            assert Fasle, "'{}' not found gdrive().item_obj".format(name)


if __name__ == '__main__':
    ## funtion library
    #device=gdrive()
    #device.getObjByName(name)
    #device.GetContentFile(src_name, dst_path, _mimetype='application/zip'):
    #device.UploadFile(src_path, dst_name, _mimetype='application/zip'):
    pass
