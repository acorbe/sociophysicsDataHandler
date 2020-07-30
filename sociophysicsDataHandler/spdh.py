"""specs etc.
"""

import owncloud

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

    def fetch_data_from_path(self
                             , path
                             , basepath=BASE_PATH):
        if not path.startswith('/'):
            path = '/' + path

        final_path = basepath + path
        print('trying to fetch:', final_path)

        df = self.__oc_client.get_file_contents(final_path)

        self.df = df

        print("data fetched. Accessible as <this-object>.df")
        

        
        
