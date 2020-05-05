# Ler e Importar XML, e exportar para base MSSQL 
#
# Importando os pacotes
import pandas as pd
import sqlalchemy
import pyodbc
import xml.etree.ElementTree as et
import tkinter as tk
from tkinter import ttk
from tkinter.filedialog import askdirectory 
from tkinter import messagebox
import os
import time
import glob
import math as m
#import re
#from concurrent.futures import ThreadPoolExecutor

class Leitor:
    def __init__(self,master):
        self.master = master
        master.title('Leitor XML v0.7')

        # Variáveis auxiliares
        self.ns = {'aux' : 'http://www.portalfiscal.inf.br/nfe'}
        self.tnome = 'NFe_Base'
        self.listad = ['SQL Server Native Client 11.0','SQL Server Native Client 10.0','ODBC Driver 13 for SQL Server']
        self.arquivos = 0

        self.driver = tk.StringVar(master)
        self.driver.set(self.listad[0])

        self.statbar = tk.StringVar(master)
        self.statbar.set('')

        self.cb1 = tk.IntVar()
        self.cb1.set(1)

        self.cb2 = tk.IntVar()
        self.cb2.set(0)

        self.lcaminho = tk.Label(text='Insira o caminho dos arquivos XML:')
        self.lcaminho.grid(row=0)

        self.pathinsert = tk.Entry(root, width=80, borderwidth=2)
        self.pathinsert.grid(row=1) 

        self.bpath = tk.Button(master, text='Pesquisar', command=self.get_cam, padx=5)
        self.bpath.grid(row=1,column=1)

        self.lserv = tk.Label(text='Insira o Servidor SQL:')
        self.lserv.grid(row=3)

        self.servinsert = tk.Entry(master, width=80, borderwidth=2)
        self.servinsert.grid(row=4)
        self.servinsert.insert(0,os.environ['COMPUTERNAME']) 

        self.checkbox2= tk.Checkbutton(master, text='SQL Express', variable=self.cb2,command=self.set_express)
        self.checkbox2.grid(row=4,column=1)

        self.lbase = tk.Label(text='Insira a base para inserir os dados:')
        self.lbase.grid(row=5)

        self.baseinsert = tk.Entry(master, width=80, borderwidth=2)
        self.baseinsert.grid(row=6)
        self.baseinsert.insert(0,'DB_XML') 

        self.checkbox1= tk.Checkbutton(master, text='Substituir?', variable=self.cb1)
        self.checkbox1.grid(row=6,column=1)

        self.dpadrao = tk.Label(text='Driver:')
        self.dpadrao.grid(row=9)

        self.Lista = ttk.Combobox(master,values=self.listad,textvariable=self.driver)
        self.Lista.config(width=78)
        self.Lista.grid(row=10)

        self.barprog = ttk.Progressbar(master,orient='horizontal',length=490,mode='determinate')
        self.barprog.grid(row=12,pady=10)

        self.status = tk.Label(textvariable=self.statbar)
        self.status.grid(row=13)

        self.bconnect = tk.Button(master, text='Importar XML', command=self.sql_connect)
        self.bconnect.grid(row=15,pady=10)

    def get_cam(self):
        global directory
        global path
        global arquivos 
        path = askdirectory()
        directory = os.fsencode(path)
        arquivos = len(glob.glob1(path,'*.xml'))
        self.pathinsert.delete(0,tk.END)
        self.pathinsert.insert(0,path)

    def set_express(self):
        if self.cb2.get() == 1:
            servidorexp = self.servinsert.get()
            self.servinsert.delete(0,tk.END)
            self.servinsert.insert(0,servidorexp+str('\\sqlexpress'))
            self.servinsert.update()
        elif self.cb2.get() == 0:
            servidornexp = self.servinsert.get()
            servidornexp = servidornexp.replace('\\sqlexpress','')
            self.servinsert.delete(0,tk.END)
            self.servinsert.insert(0,servidornexp)
            self.servinsert.update()

    def remover_canc(self):
        global all_xml
        global canc_xml
        global df_all_xml
        global df_canc_xml
        df_all_xml = pd.DataFrame(all_xml)
        del all_xml
        df_canc_xml = pd.DataFrame(canc_xml)
        del canc_xml
        if df_canc_xml.empty == False:
            df_all_xml =  pd.merge(df_all_xml,df_canc_xml, indicator=True,left_on='chNFe',right_on='canc_chNFe', how='left').query('_merge=="left_only"').drop(['_merge','canc_chNFe'], axis = 1)
            df_canc_xml.to_csv(os.path.join(path,'Cancelados.txt'),header = True,index = False,sep = '\t')

    def ponderar_venc(self):
        global df_all_xml
        global venc_xml
        df_venc_xml = pd.DataFrame(venc_xml)
        del venc_xml
        if df_canc_xml.empty == False:    
            df_venc_xml['dhEmi'] = pd.to_datetime(df_venc_xml['dhEmi'])
            df_venc_xml['dVenc'] = pd.to_datetime(df_venc_xml['dVenc'])
            df_venc_xml['vDup'] = pd.to_numeric(df_venc_xml['vDup'])
            df_venc_xml['dias'] = (df_venc_xml['dVenc'] - df_venc_xml['dhEmi']).dt.days
            df_venc_xml['vDup_x_Dias'] = (df_venc_xml['vDup'] * df_venc_xml['dias'])
            df_venc_pond = df_venc_xml.filter(['chNFe_venc','dhEmi','vDup','vDup_x_Dias'])
            del df_venc_xml
            df_venc_pond = df_venc_pond.groupby(['chNFe_venc','dhEmi']).sum()
            df_venc_pond = df_venc_pond.reset_index()      
            df_venc_pond['dPond'] = (df_venc_pond['vDup_x_Dias'] / df_venc_pond['vDup']).astype('int')           
            df_venc_pond['dVenc'] = df_venc_pond['dhEmi'] + pd.to_timedelta(df_venc_pond['dPond'], unit='days')
            df_all_xml = pd.merge(df_all_xml,df_venc_pond[['chNFe_venc','dVenc']], left_on='chNFe', right_on = 'chNFe_venc', how='left').drop('chNFe_venc', axis = 1)



    def sql_connect(self):
        global engine
        global df_all_xml
        global df_canc_xml
        try:
            start = time.time()
            servidor = self.servinsert.get()
            sqldriver = ''
            if self.Lista.get() == 'SQL Server Native Client 11.0':
                sqldriver = 'SQL+Server+Native+Client+11.0'
            elif self.Lista.get() == 'SQL Server Native Client 10.0':
                sqldriver = 'SQL+Server+Native+Client+10.0'
            else:
                sqldriver = 'ODBC+Driver+13+for+SQL+Server'
            engine = sqlalchemy.create_engine('mssql://'+servidor+'/'+self.baseinsert.get()+'?driver='+sqldriver+'&trusted_connection=yes',fast_executemany=True)
            engine.connect()
            if arquivos == 0:
                raise Exception('Nenhum arquivo .xml foi encontrado na pasta')
            else:
                self.criar_tabela()
                self.ler(directory,arquivos)
                self.remover_canc()
                self.ponderar_venc()
                self.export()
            end = time.time()
            horas = m.floor(((end-start)/3600))
            minutos = m.floor(((end-start)%3600)/60)
            segundos = m.floor((end-start)%60)
            self.statbar.set(str(len(df_all_xml))+' linhas inseridas\n'+'Tempo total: '+str(horas)+' hora(s) '+str(minutos)+' minuto(s) '+str(segundos)+ ' segundos.')
            messagebox.showinfo('Fim!','Importação finalizada!')
            del df_all_xml
            del df_canc_xml
        except Exception as e:
            #messagebox.showerror('Erro',e)
            raise

    def criar_tabela(self):
        if self.cb1.get() == 1:
            with engine.connect() as con:
                con.execute('DROP TABLE IF EXISTS '+self.tnome)
                con.execute('''
                            CREATE TABLE [dbo].['''+self.tnome+'''](
                                [index] [bigint] NOT NULL IDENTITY(1,1) PRIMARY KEY,
                                [Arq_ID] [bigint] NULL,
                                [Itm_ID] [bigint] NULL,
                                [chNFe] [varchar](44) NULL,
                                [cUF] [varchar](2) NULL,
                                [natOP] [varchar](max) NULL,
                                [serie] [int] NULL,
                                [nNF] [int] NULL,
                                [dhEmi] [date] NULL,
                                [dhSaiEnt] [date] NULL,
                                [dVenc] [date] NULL,
                                [tpNF] [int] NULL,
                                [refNFe] [varchar](44) NULL,
                                [finNFe] [int] NULL,
                                [Emit_CNPJ] [varchar](14) NULL,
                                [Emit_xNome] [varchar](200) NULL,
                                [Emit_xFant] [varchar](200) NULL,
                                [Emit_UF] [varchar](2) NULL,
                                [Emit_cPais] [varchar](4) NULL,
                                [Emit_xPais] [varchar](100) NULL,
                                [Emit_IE] [varchar](14) NULL,
                                [Dest_CNPJ] [varchar](14) NULL,
                                [Dest_CPF] [varchar](11) NULL,
                                [Dest_xNome] [varchar](200) NULL,
                                [Dest_xFant] [varchar](200) NULL,
                                [Dest_UF] [varchar](2) NULL,
                                [Dest_cPais] [varchar](4) NULL,
                                [Dest_xPais] [varchar](100) NULL,
                                [Dest_IE] [varchar](14) NULL,
                                [nItem] [bigint] NULL,
                                [cProd] [varchar](200) NULL,
                                [xProd] [varchar](500) NULL,
                                [NCM] [varchar](8) NULL,
                                [CFOP] [varchar](4) NULL,
                                [qCom] [numeric](24, 12) NULL,
                                [uCom] [varchar](6) NULL,
                                [vProd] [numeric](24, 12) NULL,
                                [vFrete] [numeric](24, 12) NULL,
                                [vSeg] [numeric](24, 12) NULL,
                                [vDesc] [numeric](24, 12) NULL,
                                [vOutro] [numeric](24, 12) NULL,
                                [nDI] [varchar](12) NULL,
                                [dDI] [date] NULL,
                                [xPed] [varchar](15) NULL,
                                [orig] [varchar](1) NULL,
                                [CST_ICMS] [varchar](2) NULL,
                                [vICMS] [numeric](24, 12) NULL,
                                [vICMSST] [numeric](24, 12) NULL,
                                [vICMSDeson] [numeric](24, 12) NULL,   
                                [CST_IPI] [varchar](2) NULL,
                                [vIPI] [numeric](24, 12) NULL,
                                [vBC_II] [numeric](24, 12) NULL,
                                [vDespAdu] [numeric](24,12) NULL,
                                [vII] [numeric](24, 12) NULL,
                                [CST_PIS] [varchar](2) NULL,
                                [vBC_PIS] [numeric](24, 12) NULL,
                                [pPIS] [numeric](24, 12) NULL,
                                [vPIS] [numeric](24, 12) NULL,
                                [CST_COFINS] [varchar](2) NULL,
                                [vBC_COFINS] [numeric](24, 12) NULL,
                                [pCOFINS] [numeric](24, 12) NULL,
                                [vCOFINS] [numeric](24, 12) NULL,
                                [vFCPUFDest] [numeric](24, 12) NULL,
                                [vICMSUFDest] [numeric](24, 12) NULL,
                                [vICMSUFRemet] [numeric](24, 12) NULL,
                                [vISSQN] [numeric](24, 12) NULL,
                                [infCpl] [varchar](5000) NULL,
                                [infAdFisco] [varchar](5000) NULL
                                )
                            ''')
        else:
            with engine.connect() as con2:
                if not engine.dialect.has_table(engine,self.tnome):
                    con2.execute('''
                                CREATE TABLE [dbo].['''+self.tnome+'''](
                                    [index] [bigint] NOT NULL IDENTITY(1,1) PRIMARY KEY,
                                    [Arq_ID] [bigint] NULL,
                                    [Itm_ID] [bigint] NULL,
                                    [chNFe] [varchar](44) NULL,
                                    [cUF] [varchar](2) NULL,
                                    [natOP] [varchar](max) NULL,
                                    [serie] [int] NULL,
                                    [nNF] [int] NULL,
                                    [dhEmi] [date] NULL,
                                    [dhSaiEnt] [date] NULL,
                                    [dVenc] [date] NULL,
                                    [tpNF] [int] NULL,
                                    [refNFe] [varchar](44) NULL,
                                    [finNFe] [int] NULL,
                                    [Emit_CNPJ] [varchar](14) NULL,
                                    [Emit_xNome] [varchar](200) NULL,
                                    [Emit_xFant] [varchar](200) NULL,
                                    [Emit_UF] [varchar](2) NULL,
                                    [Emit_cPais] [varchar](4) NULL,
                                    [Emit_xPais] [varchar](100) NULL,
                                    [Emit_IE] [varchar](14) NULL,
                                    [Dest_CNPJ] [varchar](14) NULL,
                                    [Dest_CPF] [varchar](11) NULL,
                                    [Dest_xNome] [varchar](200) NULL,
                                    [Dest_xFant] [varchar](200) NULL,
                                    [Dest_UF] [varchar](2) NULL,
                                    [Dest_cPais] [varchar](4) NULL,
                                    [Dest_xPais] [varchar](100) NULL,
                                    [Dest_IE] [varchar](14) NULL,
                                    [nItem] [bigint] NULL,
                                    [cProd] [varchar](200) NULL,
                                    [xProd] [varchar](500) NULL,
                                    [NCM] [varchar](8) NULL,
                                    [CFOP] [varchar](4) NULL,
                                    [qCom] [numeric](24, 12) NULL,
                                    [uCom] [varchar](6) NULL,
                                    [vProd] [numeric](24, 12) NULL,
                                    [vFrete] [numeric](24, 12) NULL,
                                    [vSeg] [numeric](24, 12) NULL,
                                    [vDesc] [numeric](24, 12) NULL,
                                    [vOutro] [numeric](24, 12) NULL,
                                    [nDI] [varchar](12) NULL,
                                    [dDI] [date] NULL,
                                    [xPed] [varchar](15) NULL,
                                    [orig] [varchar](1) NULL,
                                    [CST_ICMS] [varchar](2) NULL,
                                    [vICMS] [numeric](24, 12) NULL,
                                    [vICMSST] [numeric](24, 12) NULL,
                                    [vICMSDeson] [numeric](24, 12) NULL,   
                                    [CST_IPI] [varchar](2) NULL,
                                    [vIPI] [numeric](24, 12) NULL,
                                    [vBC_II] [numeric](24, 12) NULL,
                                    [vDespAdu] [numeric](24,12) NULL,
                                    [vII] [numeric](24, 12) NULL,
                                    [CST_PIS] [varchar](2) NULL,
                                    [vBC_PIS] [numeric](24, 12) NULL,
                                    [pPIS] [numeric](24, 12) NULL,
                                    [vPIS] [numeric](24, 12) NULL,
                                    [CST_COFINS] [varchar](2) NULL,
                                    [vBC_COFINS] [numeric](24, 12) NULL,
                                    [pCOFINS] [numeric](24, 12) NULL,
                                    [vCOFINS] [numeric](24, 12) NULL,
                                    [vFCPUFDest] [numeric](24, 12) NULL,
                                    [vICMSUFDest] [numeric](24, 12) NULL,
                                    [vICMSUFRemet] [numeric](24, 12) NULL,
                                    [vISSQN] [numeric](24, 12) NULL,
                                    [infCpl] [varchar](5000) NULL,
                                    [infAdFisco] [varchar](5000) NULL
                                    )
                                ''')

    def ler(self,diretorio,narquivos):
        global all_xml
        global canc_xml
        global venc_xml
        all_xml = []
        canc_xml = []
        venc_xml = []
        lidos = 0
        self.barprog['value']=0
        self.barprog.update()
        self.barprog['maximum'] = arquivos
        for file in os.listdir(diretorio):
            if file.endswith(b'.xml') or file.endswith(b'.XML'):
                lidos = lidos + 1 
                self.barprog['value']=lidos
                self.barprog.update()
                #print(file)
                xroot = et.parse(os.path.join(diretorio, file),parser=et.XMLParser(encoding="iso-8859-5"))
                xtree = xroot.getroot()
                self.statbar.set(str(lidos)+' / '+str(narquivos)+' arquivos lidos')
                if xtree.tag == '{http://www.portalfiscal.inf.br/nfe}nfeProc':
                    nItem = 0
                    chNFe = xtree.find('aux:NFe/aux:infNFe',self.ns).attrib['Id'][3:]
                    for ides in xroot.findall('aux:NFe/aux:infNFe/aux:ide',self.ns):
                        cUF = ides.find('aux:cUF',self.ns).text
                        natOp = ides.find('aux:natOp',self.ns).text
                        serie = ides.find('aux:serie',self.ns).text
                        nNF = ides.find('aux:nNF',self.ns).text
                        dhEmi_t = ides.find('aux:dhEmi',self.ns)
                        if dhEmi_t is not None:
                            dhEmi = ides.find('aux:dhEmi',self.ns).text[0:10]
                        else: 
                            dhEmi = ides.find('aux:dEmi',self.ns).text[0:10]
                        dhSaiEnt_t = ides.find('aux:dhSaiEnt',self.ns)
                        dSaiEnt_t = ides.find('aux:dSaiEnt',self.ns)    
                        if dhSaiEnt_t is not None:
                            dhSaiEnt = ides.find('aux:dhSaiEnt',self.ns).text[0:10]
                        elif dSaiEnt_t is not None: 
                            dhSaiEnt = ides.find('aux:dSaiEnt',self.ns).text[0:10]
                        else:
                            dhSaiEnt = None                        
                        tpNF = ides.find('aux:tpNF',self.ns).text
                        try:
                            refNFe = ides.find('aux:refNFe',self.ns).text
                        except: 
                            refNFe = None               
                        finNFe = ides.find('aux:finNFe',self.ns).text
                    for emits in xroot.findall('aux:NFe/aux:infNFe/aux:emit',self.ns):
                        try:
                            Emit_CNPJ = emits.find('aux:CNPJ',self.ns).text
                        except: 
                            Emit_CNPJ = None
                        Emit_xNome = emits.find('aux:xNome',self.ns).text 
                        try:
                            Emit_xFant = emits.find('aux:xFant',self.ns).text
                        except: 
                            Emit_xFant = None
                        try:
                            Emit_xPais = emits.find('.*/aux:xPais',self.ns).text
                        except:
                            Emit_xPais = None    
                        try:
                            Emit_cPais = emits.find('.*/aux:cPais',self.ns).text
                        except:
                            Emit_cPais = None    
                        Emit_UF = emits.find('.*/aux:UF',self.ns).text
                        try:
                            Emit_IE = emits.find('aux:IE',self.ns).text
                        except: 
                            Emit_IE = None   
                    for dests in xroot.findall('aux:NFe/aux:infNFe/aux:dest',self.ns):
                        try:
                            Dest_CNPJ = dests.find('aux:CNPJ',self.ns).text
                        except: 
                            Dest_CNPJ = None
                        try:
                            Dest_CPF = dests.find('aux:CPF',self.ns).text
                        except: 
                            Dest_CPF = None
                        Dest_xNome = dests.find('aux:xNome',self.ns).text 
                        try:
                            Dest_xFant = dests.find('aux:xFant',self.ns).text
                        except: 
                            Dest_xFant = None
                        try:
                            Dest_xPais = dests.find('.*/aux:xPais',self.ns).text
                        except:
                            Dest_xPais = None    
                        try:
                            Dest_cPais = dests.find('.*/aux:cPais',self.ns).text
                        except:
                            Dest_cPais = None    
                        Dest_UF = dests.find('.*/aux:UF',self.ns).text
                        try:
                            Dest_IE = dests.find('aux:IE',self.ns).text
                        except: 
                            Dest_IE = None
                    for infadics in xroot.findall('aux:NFe/aux:infNFe/aux:infAdic',self.ns):
                        try:
                            infCpl = infadics.find('aux:infCpl',self.ns).text
                        except: 
                            infCpl = None
                        try:
                            infAdFisco = infadics.find('aux:infAdFisco',self.ns).text
                        except: 
                            infAdFisco = None
                    for itens in xroot.findall('aux:NFe/aux:infNFe/aux:det',self.ns): 
                        nItem = nItem + 1    
                        cProd = itens.find('.*/aux:cProd',self.ns).text 
                        xProd = itens.find('.*/aux:xProd',self.ns).text    
                        NCM = itens.find('.*/aux:NCM',self.ns).text
                        CFOP = itens.find('.*/aux:CFOP',self.ns).text
                        qCom = itens.find('.*/aux:qCom',self.ns).text
                        uCom = itens.find('.*/aux:uCom',self.ns).text
                        vProd = itens.find('.*/aux:vProd',self.ns).text
                        try:
                            vFrete = itens.find('.*/aux:vFrete',self.ns).text
                        except: 
                            vFrete = 0.00
                        try:
                            vSeg = itens.find('.*/aux:vSeg',self.ns).text
                        except: 
                            vSeg = 0.00
                        try:
                            vDesc = itens.find('.*/aux:vDesc',self.ns).text
                        except: 
                            vDesc = 0.00
                        try:
                            vOutro = itens.find('.*/aux:vOutro',self.ns).text
                        except: 
                            vOutro = 0.00
                        try:
                            nDI =itens.find('.*//aux:nDI',self.ns).text
                        except: 
                            nDI = None 
                        try:
                            dDI = itens.find('.*//aux:dDI',self.ns).text
                        except: 
                            dDI = None                                                                         
                        try:
                            xPed = itens.find('.*//aux:xPed',self.ns).text
                        except:
                            xPed = None    
                        try:
                            orig = itens.find('.*//aux:ICMS//aux:orig',self.ns).text
                        except:
                            orig = None
                        try:
                            CST_ICMS = itens.find('.*//aux:ICMS//aux:CST',self.ns).text
                        except:
                            CST_ICMS = None    
                        try:
                            vICMS = itens.find('.*//aux:ICMS//aux:vICMS',self.ns).text
                        except: 
                            vICMS = 0.00
                        try:
                            vICMSST = itens.find('.*//aux:ICMS//aux:vICMSST',self.ns).text
                        except: 
                            vICMSST = 0.00
                        try: 
                            vICMSDeson = itens.find('.*//aux:ICMS//aux:vICMSDeson',self.ns).text
                        except:
                            vICMSDeson = 0.00                               
                        try:
                            CST_IPI = itens.find('.*//aux:IPI//aux:CST',self.ns).text
                        except:
                            CST_IPI = None 
                        try:
                            vIPI = itens.find('.*//aux:IPI//aux:vIPI',self.ns).text
                        except: 
                            vIPI = 0.00  
                        try:
                            vBC_II = itens.find('.*//aux:II//aux:vBC',self.ns).text
                        except: 
                            vBC_II = 0.00
                        try:
                            vDespAdu = itens.find('.*//aux:II//aux:vDespAdu',self.ns).text
                        except: 
                            vDespAdu = 0.00
                        try:
                            vII = itens.find('.*//aux:II//aux:vII',self.ns).text
                        except: 
                            vII = 0.00
                        try:
                            CST_PIS = itens.find('.*//aux:PIS//aux:CST',self.ns).text
                        except: 
                            CST_PIS = None
                        try:
                            vBC_PIS = itens.find('.*//aux:PIS//aux:vBC',self.ns).text
                        except: 
                            vBC_PIS = 0.00
                        try:
                            pPIS = itens.find('.*//aux:PIS//aux:pPIS',self.ns).text
                        except: 
                            pPIS = 0.00  
                        try:
                            vPIS = itens.find('.*//aux:PIS//aux:vPIS',self.ns).text
                        except: 
                            vPIS = 0.00
                        try:
                            CST_COFINS = itens.find('.*//aux:COFINS//aux:CST',self.ns).text
                        except: 
                            CST_COFINS = None
                        try:
                            vBC_COFINS = itens.find('.*//aux:COFINS//aux:vBC',self.ns).text
                        except: 
                            vBC_COFINS = 0.00
                        try:
                            pCOFINS = itens.find('.*//aux:COFINS//aux:pCOFINS',self.ns).text
                        except: 
                            pCOFINS = 0.00  
                        try:
                            vCOFINS = itens.find('.*//aux:COFINS//aux:vCOFINS',self.ns).text
                        except: 
                            vCOFINS = 0.00   
                        try:
                            vFCPUFDest = itens.find('.*//aux:ICMSUFDest//aux:vFCPUFDest',self.ns).text
                        except: 
                            vFCPUFDest = 0.00
                        try:
                            vICMSUFDest = itens.find('.*//aux:ICMSUFDest//aux:vICMSUFDest',self.ns).text
                        except: 
                            vICMSUFDest = 0.00
                        try:
                            vICMSUFRemet = itens.find('.*//aux:ICMSUFDest//aux:vICMSUFRemet',self.ns).text
                        except: 
                            vICMSUFRemet = 0.00
                        try:
                            vISSQN = itens.find('.*//aux:ISSQN//aux:vISSQN',self.ns).text
                        except: 
                            vISSQN = 0.00  

                        leitura = {'Arq_ID':lidos,'Itm_ID':nItem,'chNFe':chNFe,'cUF':cUF,'natOP':natOp,'serie':serie,'nNF':nNF,'dhEmi':dhEmi,'dhSaiEnt':dhSaiEnt,
                                    'tpNF':tpNF,'refNFe':refNFe,'finNFe':finNFe,'Emit_CNPJ':Emit_CNPJ,'Emit_xNome':Emit_xNome,'Emit_xFant':Emit_xFant,
                                    'Emit_UF':Emit_UF,'Emit_cPais':Emit_cPais,'Emit_xPais':Emit_xPais,'Emit_IE':Emit_IE,'Dest_CNPJ':Dest_CNPJ,
                                    'Dest_CPF':Dest_CPF,'Dest_xNome':Dest_xNome,'Dest_xFant':Dest_xFant, 'Dest_UF':Dest_UF,'Dest_cPais':Dest_cPais,
                                    'Dest_xPais':Dest_xPais,'Dest_IE':Dest_IE,'nItem':nItem,'cProd':cProd,'xProd':xProd,'NCM':NCM,'CFOP':CFOP,'qCom':qCom,
                                    'uCom':uCom,'vProd':vProd,'vFrete':vFrete,'vSeg':vSeg,'vDesc':vDesc,'vOutro':vOutro,'nDI':nDI,'dDI':dDI,'xPed':xPed,
                                    'orig':orig,'CST_ICMS':CST_ICMS,'vICMS':vICMS,'vICMSST':vICMSST,'vICMSDeson':vICMSDeson,'CST_IPI':CST_IPI,'vIPI':vIPI,'vBC_II':vBC_II,
                                    'vDespAdu':vDespAdu,'vII':vII,'CST_PIS':CST_PIS,'vBC_PIS':vBC_PIS,'pPIS':pPIS,'vPIS':vPIS,'CST_COFINS':CST_COFINS,
                                    'vBC_COFINS':vBC_COFINS,'pCOFINS':pCOFINS,'vCOFINS':vCOFINS,'vFCPUFDest':vFCPUFDest,'vICMSUFDest':vICMSUFDest,
                                    'vICMSUFRemet':vICMSUFRemet,'vISSQN':vISSQN,'infCpl':infCpl,'infAdFisco':infAdFisco} 
                        
                        all_xml.append(leitura)                                        
                    
                    for dups in xroot.findall('aux:NFe/aux:infNFe/aux:cobr/aux:dup',self.ns):
                        try:
                            dVenc = dups.find('aux:dVenc',self.ns).text
                        except:
                            dVenc = None
                        try:    
                            vDup = dups.find('aux:vDup',self.ns).text
                        except:
                            vDup = None

                        vencimento = {'chNFe_venc':chNFe,'dhEmi':dhEmi,'dVenc':dVenc,'vDup':vDup}
                        venc_xml.append(vencimento)
                        

                        
                elif xtree.tag == '{http://www.portalfiscal.inf.br/nfe}procEventoNFe':
                    descevento_t = xtree.find('.*//aux:descEvento',self.ns)
                    if descevento_t is not None:
                        if xtree.find('.*//aux:descEvento',self.ns).text == 'Cancelamento':
                            canc_chNFe = xtree.find('.*//aux:chNFe',self.ns).text
                            canceladas = {'canc_chNFe':canc_chNFe}
                            canc_xml.append(canceladas)
                        else:
                            continue
                    else:
                        continue    
                else:
                    continue
            else:
                continue

    def chunker(self,seq, size):
        return (seq[pos:pos + size] for pos in range(0, len(seq), size))

    def export(self):
        global df_all_xml
        chunksize = 1000
        self.barprog['value']=0
        self.barprog.update()
        self.barprog['maximum'] = len(df_all_xml)
        for i,call_xml in enumerate(self.chunker(df_all_xml, chunksize)):
            self.statbar.set(str(i*chunksize)+' linhas inseridas')
            call_xml.to_sql(name=self.tnome, con=engine, if_exists='append', index=False,
                        dtype={'chNFe':sqlalchemy.types.VARCHAR(length=44),
                                    'cUF':sqlalchemy.types.VARCHAR(length=2),
                                    'natOP':sqlalchemy.types.VARCHAR(),
                                    'serie':sqlalchemy.types.INTEGER(),
                                    'nNF':sqlalchemy.types.INTEGER(),
                                    'dhEmi':sqlalchemy.types.Date(),
                                    'dhSaiEnt':sqlalchemy.types.Date(),
                                    'dVenc':sqlalchemy.types.Date(),
                                    'tpNF':sqlalchemy.types.INTEGER(),
                                    'refNFe':sqlalchemy.types.VARCHAR(length=44),
                                    'finNFe':sqlalchemy.types.INTEGER(),
                                    'Emit_CNPJ':sqlalchemy.types.VARCHAR(length=14),
                                    'Emit_xNome':sqlalchemy.types.VARCHAR(length=200),
                                    'Emit_xFant':sqlalchemy.types.VARCHAR(length=200),
                                    'Emit_UF':sqlalchemy.types.VARCHAR(length=2),
                                    'Emit_cPais':sqlalchemy.types.VARCHAR(length=4),
                                    'Emit_xPais':sqlalchemy.types.VARCHAR(length=100),
                                    'Emit_IE':sqlalchemy.types.VARCHAR(length=14),
                                    'Dest_CNPJ':sqlalchemy.types.VARCHAR(length=14),
                                    'Dest_CPF':sqlalchemy.types.VARCHAR(length=11),
                                    'Dest_xNome':sqlalchemy.types.VARCHAR(length=200),
                                    'Dest_xFant':sqlalchemy.types.VARCHAR(length=200),
                                    'Dest_UF':sqlalchemy.types.VARCHAR(length=2),
                                    'Dest_cPais':sqlalchemy.types.VARCHAR(length=4),
                                    'Dest_xPais':sqlalchemy.types.VARCHAR(length=100),
                                    'Dest_IE':sqlalchemy.types.VARCHAR(length=14),
                                    'cProd':sqlalchemy.types.VARCHAR(length=200),
                                    'xProd':sqlalchemy.types.VARCHAR(length=500),
                                    'NCM':sqlalchemy.types.VARCHAR(length=8),
                                    'CFOP':sqlalchemy.types.VARCHAR(length=4),
                                    'qCom':sqlalchemy.types.NUMERIC(precision=24, scale=12, asdecimal=True),
                                    'uCom':sqlalchemy.types.VARCHAR(length=6),
                                    'vProd':sqlalchemy.types.NUMERIC(precision=24, scale=12, asdecimal=True),
                                    'vFrete':sqlalchemy.types.NUMERIC(precision=24, scale=12, asdecimal=True),
                                    'vSeg':sqlalchemy.types.NUMERIC(precision=24, scale=12, asdecimal=True),
                                    'vDesc':sqlalchemy.types.NUMERIC(precision=24, scale=12, asdecimal=True),
                                    'vOutro':sqlalchemy.types.NUMERIC(precision=24, scale=12, asdecimal=True),
                                    'nDI':sqlalchemy.types.VARCHAR(length=12),
                                    'dDI':sqlalchemy.types.Date(),
                                    'xPed':sqlalchemy.types.VARCHAR(length=15),
                                    'orig':sqlalchemy.types.VARCHAR(length=1),
                                    'CST_ICMS':sqlalchemy.types.VARCHAR(length=2),
                                    'vICMS':sqlalchemy.types.NUMERIC(precision=24, scale=12, asdecimal=True),
                                    'vICMSST':sqlalchemy.types.NUMERIC(precision=24, scale=12, asdecimal=True),
                                    'vICMSDeson':sqlalchemy.types.NUMERIC(precision=24, scale=12, asdecimal=True),
                                    'CST_IPI':sqlalchemy.types.VARCHAR(length=2),
                                    'vIPI':sqlalchemy.types.NUMERIC(precision=24, scale=12, asdecimal=True),
                                    'vBC_II':sqlalchemy.types.NUMERIC(precision=24, scale=12, asdecimal=True),
                                    'vDespAdu_II':sqlalchemy.types.NUMERIC(precision=24, scale=12, asdecimal=True),
                                    'vII':sqlalchemy.types.NUMERIC(precision=24, scale=12, asdecimal=True),
                                    'CST_PIS':sqlalchemy.types.VARCHAR(length=2),
                                    'vBC_PIS':sqlalchemy.types.NUMERIC(precision=24, scale=12, asdecimal=True),
                                    'pPIS':sqlalchemy.types.NUMERIC(precision=24, scale=12, asdecimal=True),
                                    'vPIS':sqlalchemy.types.NUMERIC(precision=24, scale=12, asdecimal=True),
                                    'CST_COFINS':sqlalchemy.types.VARCHAR(length=2),
                                    'vBC_COFINS':sqlalchemy.types.NUMERIC(precision=24, scale=12, asdecimal=True),
                                    'pCOFINS':sqlalchemy.types.NUMERIC(precision=24, scale=12, asdecimal=True),
                                    'vCOFINS':sqlalchemy.types.NUMERIC(precision=24, scale=12, asdecimal=True),
                                    'vFCPUFDest':sqlalchemy.types.NUMERIC(precision=24, scale=12, asdecimal=True),
                                    'vICMSUFDest':sqlalchemy.types.NUMERIC(precision=24, scale=12, asdecimal=True),
                                    'vICMSUFRemet':sqlalchemy.types.NUMERIC(precision=24, scale=12, asdecimal=True),
                                    'vISSQN':sqlalchemy.types.NUMERIC(precision=24, scale=12, asdecimal=True),
                                    'infCpl':sqlalchemy.types.VARCHAR(length=5000),
                                    'infAdFisco':sqlalchemy.types.VARCHAR(length=5000)})
            self.barprog['value']= i*chunksize
            self.barprog.update()
        self.barprog['value']=len(df_all_xml)
        self.barprog.update()
        
#TK
root = tk.Tk()
janela = Leitor(root)
root.mainloop()

# =============================================================================
