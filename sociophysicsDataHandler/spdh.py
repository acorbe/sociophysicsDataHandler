"""specs etc.
"""

import owncloud
import pyarrow.parquet as pq
import pyarrow


TARGET_WEBDAV = "https://tue.data.surfsara.nl"
DEFAULT_FNAME = "auth.txt"
BASE_PATH = "/ProRail_USE_LL_data"

class SociophysicsDataHandler(object):

    def __init__(self
                 , target_webdav=TARGET_WEBDAV
                 , auth_fname=DEFAULT_FNAME):
        
        self.__target_webdav=target_webdav
        self.load_credentials(auth_fname)
        if self.__have_credentials:
            self.__login()

    def load_credentials(self
                         , auth_fname=DEFAULT_FNAME):
        try:
            with open(auth_fname, 'r') as f:
                content = [x.replace('\n','').replace(' ','') for x in f.readlines()]
                

            self.__credentials_usr = content[0]
            self.__credentials_token = content[1]

            self.__have_credentials = True
            
        except:
            print("ERROR. Fill the file '{}'".format(auth_fname))
            print("An empty one has been created for you")
            print("Content: (2 lines)")
            print("<access email>")
            print("<access token>")

            with open(auth_fname,'w') as f:
                f.write('')
            
            # TODO: rewrite as a proper FnF exception
            self.__have_credentials = False
            ## raise Exception("File not found")
        

    def __login(self):
        self.__oc_client = owncloud.Client(self.__target_webdav)
        oc = self.__oc_client
        
        usr = self.__credentials_usr
        tok = self.__credentials_token
        

        try:
            ## print(usr,tok)
            oc.login(usr, tok)
        except Exception as e:
            print("Login error. ")
            print(e)

    def __decode_parquet(self,fpath):        
        return pq.ParquetDataset(fpath).read_pandas().to_pandas()

    def __decode_parquet_in_memory(self, fpath):

        to_obj_f = pyarrow.BufferReader(fpath)
        return pq.read_pandas(to_obj_f).to_pandas()

    def fetch_data_from_path(self
                             , path
                             , basepath=BASE_PATH):
        
        if not path.startswith('/'):
            path = '/' + path

        final_path = basepath + path
        print('trying to fetch:', final_path)

        dump_data_in_memory_only = True

        if dump_data_in_memory_only:
            df = self.__oc_client.get_file_contents(final_path)
            self.df = self.__decode_parquet_in_memory(df)
        else:
            ## not the preferred way. disabled by default.
            temp_file = 'temp.parquet'
            self.__oc_client.get_file(final_path,temp_file)
            self.df = self.__decode_parquet(temp_file)

        print("data fetched. Accessible as <this-object>.df")
        

        
        
