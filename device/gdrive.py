#!/usr/bin/python
# -*- coding: utf-8 -*-
import sys, os
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive

'''
Homepage: https://pypi.python.org/pypi/PyDrive
Documentation: Official documentation on GitHub pages
Github: https://github.com/googledrive/PyDrive
Quickstart: https://pythonhosted.org/PyDrive/quickstart.html
'''
class gdrive():

    path=os.path.dirname(__file__)
    rpt_id='1kPoq8OgxJ-FDPweFleGpPt_D6jA8Ndm-'
    fut_rpt_id='12UAi5f_XU6oaYbhIzU4iRohsd2V5i3V2'
    opt_rpt_id='1GYaukGZrR3Kj3ubexwXDTwkJWkeyMEPE'
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

    def getIdByName(self, name, target_id):
        # Paginate file lists by specifying number of max results
        query="'%s' in parents and trashed=false" %target_id
        i=1
        for file_list in self.drive.ListFile({'q': query, 'maxResults': 10}):
            print('Received %s files from Files.list(%s)' %(len(file_list), i)) # <= 10
            i+=1
            for file_obj in file_list:
                print(file_obj['title'])
                if file_obj['title']==name:
                    print('title: %s, id: %s' % (file_obj['title'], file_obj['id']))
                    return file_obj['id']
        return None

    def GetContentFile(self, file_path, target_id, _mimetype='application/zip'):
        if target_id==None:
            return target_id
        # GetContentFile(): download file(filepath) from gdrive(target_id)
        file_obj=self.drive.CreateFile({'id': target_id})
        print('Downloading file %s from Google Drive' % file_obj['title'])
        # Save Drive file as a local file
        file_obj.GetContentFile(file_path, mimetype=_mimetype)
        print('file is on local path: %s' %file_path)

    def UploadFile(self, file_path, target_id, _mimetype='application/zip'):
        # Upload(): upload file(file_path) to grive(target_id)
        try:
            #dir_path=os.path.split(localpath)[0]
            file_name=os.path.split(file_path)[1]
            #print(file_name, target_id)
            #query="'%s' in parents and trashed=false" %target_id
            #file_list=self.drive.ListFile({'q': query}).GetList();

            #for tmp in file_list:
            #    if tmp['title']==file_name:
            #        print(tmp['title'], tmp['id'])
            #        # Create GoogleDriveFile instance
            #        oid=gdrive().getIdByName(file_name, target_id)
            #        file_obj=self.drive.CreateFile({"title": file_name, "id": oid,})
            #        break
            #    else:
            #        # Create GoogleDriveFile instance
            #        file_obj=self.drive.CreateFile({"title": file_name, "parents": [{"id": target_id}]})

            file_obj=self.drive.CreateFile({"title": file_name, "parents": [{"id": target_id}]})
            file_obj.SetContentFile(file_path)
            file_obj.Upload()
            print('Upload file %s with mimeType %s' % (file_obj['title'], file_obj['mimeType']))
        except :
            print("Unexpected error:", sys.exc_info()[0])

if __name__ == '__main__':

    '''funtion library
    #gdrive().getIdByName(name, target_id):
    #gdrive().GetContentFile(file_path, target_id, _mimetype='application/zip'):
    #gdrive().UploadFile(file_path, target_id, _mimetype='application/zip'):
    '''
    oid=gdrive().getIdByName('Daily_2018_10_04.zip', gdrive().fut_rpt_id)
    #gdrive().GetContentFile('/home/luke/fex_daily/trash/aa.zip', oid)
    #gdrive().UploadFile('/home/luke/fex_daily/trash/aa.zip', gdrive().fut_rpt_id)
    #gdrive().fut_rpt_id
    sys.exit()


    # Auto-iterate through all files in the root folder.
    file_list = drive.ListFile({'q': "'root' in parents and trashed=false", 'maxResults': 10}).GetList()
    for file1 in file_list:
        #if file1['title']=='fut_rpt':
        print('title: %s, id: %s' % (file1['title'], file1['id']))
    sys.exit()
    # Create GoogleDriveFile instance
    # file2 = drive.CreateFile( {'title':'Daily_2018_08_10.zip', 'mimeType':'application/zip',
    #        "parents": [{"id": tgt_folder_id}]})
