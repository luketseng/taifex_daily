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
    fut_dir_list=None
    opt_dir_list=None
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

        if self.fut_dir_list==None or self.opt_dir_list==None:
            try:
                print('Loading file list from gdrive...')
                query="'%s' in parents and trashed=false" %self.fut_rpt_id
                self.fut_dir_list=self.drive.ListFile({'q': query}).GetList()
                query="'%s' in parents and trashed=false" %self.opt_rpt_id
                self.opt_dir_list=self.drive.ListFile({'q': query}).GetList()
            except:
                print('Except: file list error')

    def getIdByName(self, name, target_id):
        # Paginate file lists by specifying number of max results
        if target_id==self.fut_rpt_id:
            file_list=self.fut_dir_list
        elif target_id==self.opt_rpt_id:
            file_list=self.opt_dir_list
        for file_obj in file_list:
            #print(file_obj['title'])
            if file_obj['title']==name:
                print('getIdByName(): title=%s, id=%s' % (file_obj['title'], file_obj['id']))
                return file_obj['id']
        return None

    def GetContentFile(self, file_path, target_id, _mimetype='application/zip'):
        if target_id==None:
            return target_id
        # GetContentFile(): download file(filepath) from gdrive(target_id)
        file_obj=self.drive.CreateFile({'id': target_id})
        print('Downloading file: %s from gdrive' % file_obj['title'])
        # Save Drive file as a local file
        file_obj.GetContentFile(file_path, mimetype=_mimetype)
        print('Download done: path=%s' %file_path)

    def UploadFile(self, file_path, target_id, _mimetype='application/zip', recover=True):
        # Upload(): upload file(file_path) to grive(target_id)
        file_name=os.path.split(file_path)[1]
        try:
            if target_id==self.fut_rpt_id:
                file_list=self.fut_dir_list
            elif target_id==self.opt_rpt_id:
                file_list=self.opt_dir_list

            # delete file if file exist in gdrive
            for obj in file_list:
                if obj['title']==file_name and recover==True:
                    print('File exist on gdrive: title=%s, id=%s' %(obj['title'], obj['id']))
                    obj.Delete()
                    print('Delete file in gdrive')

            # Create GoogleDriveFile instance
            file_obj=self.drive.CreateFile({"title": file_name, "parents": [{"id": target_id}]})
            file_obj.SetContentFile(file_path)
            file_obj.Upload()
            print('Upload file: %s with mimeType %s' % (file_obj['title'], file_obj['mimeType']))
        except :
            print("Unexpected error:", sys.exc_info()[0])

if __name__ == '__main__':

    '''funtion library
    #gdrive().getIdByName(name, target_id):
    #gdrive().GetContentFile(file_path, target_id, _mimetype='application/zip'):
    #gdrive().UploadFile(file_path, target_id, _mimetype='application/zip'):
    '''
    #print(type(gdrive().fut_dir_list[0]))
    #oid=gdrive().getIdByName('Daily_2018_10_04.zip', gdrive().fut_rpt_id)
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
