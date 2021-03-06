import pip
pip.main(['install', '--disable-pip-version-check', '--no-cache-dir', 'pydrive'])

from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from io import BytesIO
import os 
import zipfile 
import pandas as pd
import numpy as np
import xml.etree.ElementTree as ET
import glob
from keboola import docker

""" =========================== """
"""     PARSER DEFINITON        """
""" =========================== """
## UNIQUE ID GENERATOR (NOT USED)
#def uniqueIdGenerator():
#    seed = 1
#    while True:
#       yield seed
#       seed += 1

# MAIN PARSER CLASSES
class Node:
    """ 
    Building block for the XMLParser Class.
    """
    def __init__(self, element, parentNode=None):
        self.element = element
        self.parentNode = parentNode
        if self.parentNode:
            self.parentTag = parentNode.tag
        self.tag = element.tag
        self.value = element.text
        self.childrenElementList = element.getchildren()
        self.childrenNodes = []
        if not self.childrenElementList:
            self.isLeaf = True
        else:
            self.isLeaf = False
        if self.isLeaf:
            self.dataFrame = pd.DataFrame({'parentTag':[self.parentTag],'{}.{}'.format(self.parentTag,self.tag):self.value})
        else:
            self.dataFrame = None
    
    def __str__(self):
        return str(self.tag)
    
    def __repr__(self):
        return str(self.tag)
    
    def feedforwardInit(self, recursive=False, level=0, treeDict={}):
        """ 
        Create children Nodes if there are any according to the XML tree structure. 
        If `recursive` is true, than all `childrenNodes` in the tree will be initialized.
        
        """
        self.childrenNodes = [Node(childElement,self) for childElement in self.childrenElementList]
        if recursive:
            level += 1
            for childNode in self.childrenNodes:
                if level in treeDict:
                    pass
                else:
                    treeDict[level]=[]
                treeDict[level].append(childNode)                
                childNode.feedforwardInit(True,level,treeDict)
            return False
            
    def childMerge(self):
        """ 
        Merge DataFrames, carried by the children of the nodes, into one DataFrame, which 
        will be then stored in `self.DataFrame`.
        """ 
        if self.isLeaf:
            pass       
        else:
            nodeList = []
            banNodesList = []
            childrenNodesList = self.childrenNodes.copy()
            for node in childrenNodesList:
                if node in banNodesList:
                    pass
                else:
                    subnodeList = []
                    for subnode in self.childrenNodes:
                        if node.tag == subnode.tag:
                            subnodeList.append(subnode)
                            banNodesList.append(subnode)
                        else:
                            pass
                    if len(subnodeList) > 1:
                        nodeList.append(subnodeList)
                    else:
                        nodeList.append(subnodeList[0])
            if isinstance(nodeList[0],list):
                resultingDataFrame = pd.concat([df.dataFrame for df in nodeList[0]],ignore_index=True)
            else:
                resultingDataFrame = nodeList[0].dataFrame
            # forcycle the rest
            for item in nodeList[1:]:
                if isinstance(item,list):
                    resultingDataFrame = resultingDataFrame.merge(pd.concat([df.dataFrame for df in item],ignore_index=True),how='inner',on='parentTag')
                else:
                    resultingDataFrame = resultingDataFrame.merge(item.dataFrame,how='inner',on='parentTag')
            self.dataFrame = resultingDataFrame
            if self.parentNode:
                self.dataFrame['parentTag'] = self.parentTag

class XMLParser:
    """
    XML Parser for Liftago. Takes either `path_to_xml` string (mandatory, set to None if unusued) or `extTree` ElementTree
    object. Method `parseToDataFrame` returns flattened xml as pandas.DataFrame, which can be then exported to csv via *.to_csv method
    """
    def __init__(self,path_to_xml,extTree=None):
        if extTree:
            root = extTree
        else:
            tree = ET.parse(path_to_xml)
            root = tree.getroot()
        self.rootNode = Node(root)
        self.treeDict = {0:[self.rootNode]}
        self.xmlDataFrame = None
    
    def parseToDataFrame(self,returnDataFrame=True):
        self.rootNode.feedforwardInit(recursive=True,level=0,treeDict=self.treeDict)
        for i in reversed(range(len(self.treeDict))):
            for node in self.treeDict[i]:
                node.childMerge()
        self.xmlDataFrame = self.rootNode.dataFrame
        if returnDataFrame:
            return self.xmlDataFrame
        
""" =========================== """
"""    GOOGLE API CONNECTION    """
""" =========================== """

if __name__ == '__main__': 
    
    """ =========================== """    
    """ LOAD ALREADY PROCESSED DATA """
    """ =========================== """

    batchTable = pd.read_csv('in/tables/csobBatch.csv')
    alreadyProcessedZipfiles = pd.read_csv('in/tables/CSOB_alreadyProcessedZipFiles.csv')
    
    """ =========================== """
    """ KEBOOLA STUFF """
    """ =========================== """
    
    cfg = docker.Config()
    parameters = cfg.get_parameters()
    folderNames = parameters.get('folderNames')
    gauth = GoogleAuth(settings_file=parameters.get('pathToFile'))
    drive = GoogleDrive(gauth)
    
    """ =========================== """
    """      FILL THE DATAFRAME     """
    """ =========================== """
        
    finalDataFrame = None
        
    if folderNames:
        FOLDERS_TO_LOOKAT = list(folderNames)
    else:
        FOLDERS_TO_LOOKAT = ['CSOB AM 2016','CSOB AM 2017'] 
       
    for folderToLookAt in FOLDERS_TO_LOOKAT:
        driveFilesList = drive.ListFile({'q':"mimeType='application/vnd.google-apps.folder' and title='{}' and trashed=false".format(folderToLookAt)}).GetList()                        
        folderId = driveFilesList[0]['id']
        zipfilesInFolder = drive.ListFile({'q':"'{}' in parents".format(folderId)}).GetList()
        for zf in zipfilesInFolder:
            if ('zip' in zf['title'].lower()) & (zf['title'] not in alreadyProcessedZipfiles['name'].tolist()) :
                print('title: {}'.format(zf['title']))
                toUnzip = drive.CreateFile({'id':zf['id']})
                toUnzipStringContent = toUnzip.GetContentString(encoding='cp862')
                toUnzipBytesContent = BytesIO(toUnzipStringContent.encode('cp862'))
                readZipfile = zipfile.ZipFile(toUnzipBytesContent, "r")
                for fileInZipfileName in readZipfile.namelist():
                    if '.xml' in fileInZipfileName.lower():
                        openedXml = readZipfile.open(fileInZipfileName).read()
                        loadedXml = ET.fromstring(openedXml.decode())
                        toBeParsed = XMLParser(None,loadedXml.find('merchants'))  
                        parsedXmlDataFrame = toBeParsed.parseToDataFrame()
                        if finalDataFrame is not None:
                            finalDataFrame = pd.concat([finalDataFrame.copy(),parsedXmlDataFrame])
                        else:
                            finalDataFrame = parsedXmlDataFrame.copy()
                    
                    else:
                        pass              
            else:
                pass
            
    try:
        """ =========================== """
        """      CONCAT NEW WITH OLD    """
        """ =========================== """
        
        outputFrame = pd.concat([batchTable,finalDataFrame])
        outputFrame.drop_duplicates(subset=['merchant_header.type', 'parentTag', 'merchant_header.merchant_id',
       'merchant_header.merchant_name', 'merchant_header.firm_identificator',
       'merchant_header.bank_account', 'merchant_header.bank_code',
       'merchant_header.transaction_currency',
       'merchant_header.account_currency', 'transaction.type',
       'transaction.terminal_id', 'transaction.auth_code', 'transaction.date',
       'transaction.time', 'transaction.invoice_number',
       'transaction.variable_symbol', 'transaction.card_number',
       'transaction.brutto_transaction_currency',
       'transaction.brutto_account_currency', 'transaction.brutto_CRDB',
       'transaction.IF', 'transaction.AF', 'transaction.fee',
       'transaction.netto', 'transaction.netto_CRDB', 'transaction.cashback',
       'transaction.cashback_CRDB'],inplace=True)
    
        outputFrame.to_csv('out/tables/parsedBatch.csv',index=None)
    except AttributeError:
        pass
