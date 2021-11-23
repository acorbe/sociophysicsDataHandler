"""Pedestrian dynamics data retriever for the  Sociophysics
courses at Eindhoven University of Technology.

Author: Alessandro Corbetta
"""

import owncloud
import pyarrow.parquet as pq
import pyarrow
import PIL.Image as Image
import io
import os
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
TARGET_WEBDAV = "https://crowdflow3.phys.tue.nl/owncloud"
DEFAULT_FNAME = "auth.txt"
BASE_PATH = "/storage/surfsara/ProRail_USE_LL_data"
# TARGET_WEBDAV = "https://fnfcloud.phys.tue.nl/owncloud"
# DEFAULT_FNAME = "auth.txt"
# BASE_PATH = "/storage12/surfsara/ProRail_USE_LL_data"


class RedditComments(object):
    def __init__(self,tgz_archive):
        self.tgz_archive = tgz_archive
        self.df = None

    def print_file_list(self):
        print(self.tgz_archive.list())

    def get_file_names(self):
        return self.tgz_archive.getnames()

    def get_comment_matching_id(self,idstr):
        import pandas as pd
        from io import BytesIO
        flist = self.get_file_names()

        matching_fnames = filter(lambda x : idstr in x
                                 , flist)

        out = []
        for final_fname in matching_fnames:
            print(final_fname)
            data = self.tgz_archive.extractfile(final_fname)

            out.append(pd.read_json(data))

        print("data fetched. Accessible as <this-object>.reddit_comments.df")
        self.df = out
            


class SociophysicsDataHandler(object):
    """Pedestrian dynamics data retriever for the
    Sociophysics courses at Eindhoven University of Technology.
    """

    def __init__(self
                 , target_webdav=TARGET_WEBDAV
                 , auth_fname=DEFAULT_FNAME
                 , basepath = BASE_PATH):
        """Constructor method
        """
        self.__target_webdav = target_webdav
        self.load_credentials(auth_fname)
        if self.__have_credentials:
            self.__login()

        self.__basepath = basepath

    def load_credentials(self, auth_fname=DEFAULT_FNAME):
        """Load credentials from auth.txt file. A typical file looks
        as follows:

        s123456@student.tue.nl
        XXXX-XXXX-XXXX-XXXX

        :param auth_fname: path + filename to auth.txt file
        """

        try:
            with open(auth_fname, 'r') as f:
                content = [x.replace('\n', '').replace(' ', '')
                           for x in f.readlines()]

            self.__credentials_usr = content[0]
            self.__credentials_token = content[1]

            self.__have_credentials = True

        except:
            print("ERROR. Fill the file '{}'".format(auth_fname))
            print("An empty one has been created for you")
            print("Content: (2 lines)")
            print("<access email>")
            print("<access token>")

            with open(auth_fname, 'w') as f:
                f.write('')

            # TODO: rewrite as a proper FnF exception
            self.__have_credentials = False
            # raise Exception("File not found")

    def __login(self):
        self.__oc_client = owncloud.Client(self.__target_webdav, verify_certs = True)
        oc = self.__oc_client

        usr = self.__credentials_usr
        tok = self.__credentials_token

        try:
            # print(usr,tok)
            oc.login(usr, tok)
        except Exception as e:
            print("Login error. ")
            print(e)

    def __decode_parquet(self, fpath):
        return pq.ParquetDataset(fpath).read_pandas().to_pandas()

    def __decode_targz(self, fpath):
        from .ddut import get_depth_maps
        return get_depth_maps([fpath], verbose=True)

    def __decode_parquet_in_memory(self, fpath):
        to_obj_f = pyarrow.BufferReader(fpath)
        return pq.read_pandas(to_obj_f).to_pandas()

    def __decode_targz_in_memory(self, fpath):
        to_obj_f = pyarrow.BufferReader(fpath)
        return self.__decode_targz(to_obj_f)

    def __cast_dtypes(self, df):
        dtypes = {
            'date_time_utc': 'float64',
            'tracked_object': 'int32',
            'x_pos': 'float32',
            'y_pos': 'float32'
        }
        df = df.assign(**{c: df[c].astype(d)
                          for c, d in dtypes.items()})
        return df
    
    def __rename_columns(self, df):
        del df['h_pos']
        df.columns = ['date_time_utc', 'tracked_object', 'x_pos', 'y_pos']
        return df

    def fetch_prorail_data_from_path(self, path, basepath=BASE_PATH):
        """
        Fetch trajectory data from tue research drive.

        :param path: path + filename if the file that will be fetched
        :param basepath: enables changing the basepath.
                         Only for advanced usage.
        """

        if basepath is None:
            basepath = self.__basepath

        if not path.startswith('/'):
            path = '/' + path

        final_path = basepath + path
        print('trying to fetch:', final_path)

        dump_data_in_memory_only = True

        if dump_data_in_memory_only:
            df = self.__oc_client.get_file_contents(final_path)
            self.df = self.__decode_parquet_in_memory(df)
        else:
            # not the preferred way. disabled by default.
            temp_file = 'temp.parquet'
            self.__oc_client.get_file(final_path, temp_file)
            self.df = self.__decode_parquet(temp_file)

        if self.df.shape[1] > 4:
            self.df = self.__rename_columns(self.df)

        self.df = self.__cast_dtypes(self.df)

        print("data fetched. Accessible as <this-object>.df")

    def fetch_depth_data_from_path(self, path, basepath=BASE_PATH):
        """
        Fetch image depth data from tue research drive.

        :param path: path + filename if the file that will be fetched
        :param basepath: enables changing the basepath. Only for advanced usage.
        """

        if basepath is None:
            basepath = self.__basepath

        final_path = os.path.join(basepath, path)
        print('trying to fetch:', final_path)

        dump_data_in_memory_only = True

        if dump_data_in_memory_only:
            targz = self.__oc_client.get_file_contents(final_path)
            self.t, self.dd = self.__decode_targz_in_memory(targz)
        else:
            # not the preferred way. disabled by default.
            temp_file = 'temp.tar.gz'
            self.__oc_client.get_file(final_path, temp_file)
            self.t, self.dd = self.__decode_targz(temp_file)

        print("depth data fetched. Accessible as <this-object>"
              ".dd and associated timestamps accesible as <this-object>.t")

    def fetch_econophysics_data_from_path(self,path, basepath=BASE_PATH):

        import pandas as pd
        import tarfile
        from io import BytesIO
        
        if basepath is None:
            basepath = self.__basepath

        final_path = os.path.join(basepath, path)
        print('trying to fetch:', final_path)

        dump_data_in_memory_only = True

        formats_to_decompress = ["tar.gz"]
        formats_to_open_onthefly = ["json","csv"]

        if dump_data_in_memory_only:
            data = self.__oc_client.get_file_contents(final_path)
            if True:
                if final_path.endswith(".json"):
                    # reddit case, no sub 
                    self.df = pd.read_json(data, orient="index")
                    print("data fetched. Accessible as <this-object>.df")

                elif final_path.endswith(".tar.gz"):
                    #reddit case, with subcomments
                    
                    buffer = BytesIO()


                    buffer.write(data)
                    buffer.seek(0)
                    
                    self.reddit_comments = RedditComments(tarfile.open(fileobj=buffer,mode = "r:gz"))
                    print("data fetched. Accessible as <this-object>.reddit_comments")

                elif final_path.endswith(".csv"):
                    # stock case
                    buffer = BytesIO()
                    buffer.write(data)
                    buffer.seek(0)
                    self.df = pd.read_csv(buffer, index_col=0, parse_dates=[0])
                    print("data fetched. Accessible as <this-object>.df")

                else:
                    print("file extension is not recognized!")
                    raise NotImplementedError
            # except:
            #     return data
            
            # self.t, self.dd = self.__decode_targz_in_memory(targz)
        else:
            raise NotImplementedError
        
            # # not the preferred way. disabled by default.
            # temp_file = 'temp.tar.gz'
            # self.__oc_client.get_file(final_path, temp_file)
            # self.t, self.dd = self.__decode_targz(temp_file)

        

    def fetch_background_image_from_path(self, path, basepath=BASE_PATH):
        """
        Fetch overhead black/white image, as was observed by station tracking
        sensors. This enables plotting trajectories on top of the platform.

        :param path: path + filename if the file that will be fetched
        :param basepath: enables changing the basepath.
                         Only for advanced usage.
        """
        if basepath is None:
            basepath = self.__basepath
        
        if not path.startswith('/'):
            path = '/' + path

        final_path = basepath + path
        print('trying to fetch:', final_path)

        bg = self.__oc_client.get_file_contents(final_path)
        self.bg = Image.open(io.BytesIO(bg))

        print("background fetched. Accessible as <this-object>.bg")

    def list_files(self, path, basepath=BASE_PATH):
        """
        List all files within a folder of the tue research drive.
        For instance: dh.list_files("") returns a pandas dataframe
        with the paths available from the root folder.

        :param path: path + filename if the file that will be fetched
        :param basepath: enables changing the basepath.
                         Only for advanced usage.
        """
        if basepath is None:
            basepath = self.__basepath
        
        from pandas import DataFrame

        final_path = os.path.join("", basepath, path, "")
        print("targeting path", final_path)

        oc_files = self.__oc_client.list(final_path)

        self.filelist = DataFrame(
            [x.__dict__ for x in oc_files]).drop('file_type', axis=1)

        print("Files listed. Accessible as <this-object>.filelist")

        return self.filelist

    def print_files(self, path, basepath=BASE_PATH):
        """
        Print all files within a folder of the tue research drive.

        :param path: path + filename if the file that will be fetched
        :param basepath: enables changing the basepath.
                         Only for advanced usage.
        """
        if basepath is None:
            basepath = self.__basepath

        final_path = os.path.join("", basepath, path, "")

        entries = self.__oc_client.list(final_path)

        print(f"Folder {path} contains the following files and/or folders:")
        for file in entries:
            if file.file_type == 'dir':
                print(f'Folder: {file.name}\n')
                self.print_files(os.path.join(path, file.name))
            else:
                print(f'  File: {file.name}')
                if file == entries[-1]:
                    print('\n')
