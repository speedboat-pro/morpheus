from bs4 import BeautifulSoup
import requests

class Souper():
    
    def __init__(self, url, bsParser='html.parser'):
        self.url = url
        self.bsParser = bsParser
    
    def scoop(self):  
        def status_check(r):
            rtn = str()
            if r.status_code==200:
                rtn = "Success!"
            else:
                rtn = "Failed!"
            return rtn
        def encoding_check(r):
            return (r.encoding)
        def decode_content(r,encoding):
            return (r.content.decode(encoding)) 
        response = requests.get(self.url)
        enc = encoding_check(response)
        contents = decode_content(response,enc)        
        stat = status_check(response)
        #print(response)
        try:
            print("Scooping {0} with encoding: {1}:{2}".format(
                self.url
                ,enc
                ,stat
                ))
            
            soup = BeautifulSoup(contents,self.bsParser)
        except Exception as err:
            print(err)
        return soup
    