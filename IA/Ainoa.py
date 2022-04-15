import pandas as pd
import numpy as np
import json
import obspy
import tensorflow as tf
import skfuzzy as fuzz
from skfuzzy import control as ctrl
import datetime
import time
import os 

srcdirectory= os.path.dirname(os.path.realpath(__file__))

def read_stream_obspy(allfilese):
    st = obspy.read(allfilese[0]);
    for fil in allfilese[1:]:
        st += obspy.read(fil);
    st = st.sort(['starttime'])
    st = st.merge(method=1,fill_value='latest',interpolation_samples=0)
    return st

def simu_dados(nsec,erro1=False,erro2=False):
    dict_o = {}
    keyv = ['R8CEBEHE', 'R8CEBEHN', 'R8CEBEHZ', 'R016AEHE', 
            'R016AEHN', 'R016AEHZ', 'RBAE5EHZ', 'RBAE5ENE', 
            'RBAE5ENN', 'RBAE5ENZ', 'RE647EHE', 'RE647EHN', 
            'RE647EHZ', 'RFB89EHE', 'RFB89EHZ', 'RFB89EHN']
    if erro1:
        keyv = keyv[:-3]
    else:
        pass
    for keyval in keyv:
        dict_o[keyval]=np.random.randint(-30000,30000, size=nsec*100).astype("float")
    if erro2:
        index = np.random.choice(nsec*100, 1, replace=False)
        valores = np.random.randint(-30000,30000, size=nsec*100).astype("float")
        valores[index] = np.nan
        dict_o[keyv[3]]=valores
    else:
        pass
    return dict_o

def hjorth(a):
    a = a.astype(np.float64)
    primeira_diff = np.diff(a)
    segunda_diff = np.diff(a,2)

    try:
        var_zero = np.mean(a ** 2)
    except:
        var_zero = np.nan
    try:
        var_d1 = np.mean(primeira_diff ** 2)
    except:
        var_d1 = np.nan
    try:
        var_d2 = np.mean(segunda_diff ** 2)
    except: 
        var_d2 = np.nan

    atividade = var_zero
    mobilidade = np.sqrt(var_d1 / var_zero)
    complexidade = np.sqrt(var_d2/var_d1) / mobilidade
    return np.array([atividade, mobilidade, complexidade])

def core_hjorth(d1):
    keyvs = d1[0]
    d1 = d1[1][:int(d1[1].shape[0]/100)*100]
    return pd.DataFrame([hjorth(a) for a in np.split(d1,d1.shape[0]/100)], columns = [keyvs+"-"+para for para in ["H1","H2","H3"]])

def scale_data(array,par_dfs):
    means, stds = par_dfs["Means"].to_numpy(),par_dfs["Variances"].to_numpy()**0.5
    return (array-means)/stds

def predict_AINOA(scal_inpu,srcdirectory):
    col_h = [[x for x in scal_inpu.columns.values if x[-2:]==y] for y in ["H1","H2","H3"]]
    todos_result=[]
    for ch in col_h:
        X_test = scal_inpu[ch]
        model = tf.keras.models.load_model(srcdirectory+'/Autoencoders/model-'+str(ch[0][-2:]))
        X_pred = model.predict(np.array(X_test))
        X_pred = pd.DataFrame(X_pred, 
                              columns=X_test.columns)
        X_pred.index = X_test.index

        scored = pd.DataFrame(index=X_test.index)
        scored['Loss_mae - '+ch[0].split("-")[1]] = np.mean(np.abs(X_pred-X_test), axis = 1)

        todos_result.append(scored)

    df_inputs_fuzzy = pd.concat(todos_result, axis=1).dropna()
    df_inputs_fuzzy.columns = ["H1","H2","H3"]
    return df_inputs_fuzzy

def moving_average(a, n=3):
    ret = np.cumsum(a, dtype=float)
    ret[n:] = ret[n:] - ret[:-n]
    return ret[n - 1:] / n

def Score_funct(xt,mu0,sigma0,perc_chang_mu,perc_chang_sigma):
    mu1, sigma1 = (1+perc_chang_mu)*mu0,(1+perc_chang_sigma)*sigma0
    delta,q = (mu1-mu0)/sigma0,sigma0/sigma1
    C1,C2,C3 = delta*(q**2),(1-q**2)/2,(delta**2)*(q**2)/2-np.log(q)
    Yt = (xt-mu0)/sigma0
    return C1*Yt+C2*(Yt**2)-C3

def Restart_Scores(df,srcdirectory):
    for colv in df.columns:
        to_json_vals_metric = {"Last Score":{"0":0.0}}
        with open(srcdirectory+"/CurrentScore/LastScore"+colv+".json", 'w') as f:
            json.dump(to_json_vals_metric, f)

def CumSum_CPD_N(colv,par_train,dfin,perc_chang_mu,perc_chang_sigma,score_init,srcdirectory):
    mu0,sigma0 = par_train.loc[colv]["Means"],par_train.loc[colv]["Stds"]
    sval = dfin[colv].to_numpy()
    scores_time=[score_init]
    for val in sval:
        nscore = Score_funct(val,mu0,sigma0,perc_chang_mu,perc_chang_sigma)
        scores_time.append(np.max([0,scores_time[-1]+nscore]))
    to_json_vals_metric = {"Last Score":{"0":scores_time[-1].flatten()[0]}}
    with open(srcdirectory+"/CurrentScore/LastScore"+colv+".json", 'w') as f:
        json.dump(to_json_vals_metric, f)
    return scores_time[1:]

##Note que na função acima, os estados do algoritmo de controle de erro são sempre sobrescritos 
## para o estado atual.

def CumSum_CPD_DF_N(df,par_train,perc_chang_mu,perc_chang_sigma,srcdirectory):
    up_cols = []
    for colv in df.columns:
        score_init = pd.read_json(srcdirectory+"/CurrentScore/LastScore"+colv+".json")["Last Score"][0]
        up_cols.append(CumSum_CPD_N(colv,par_train,df,perc_chang_mu,perc_chang_sigma,score_init,srcdirectory))
    return pd.DataFrame(up_cols,columns=df.index,index=df.columns).T

def Status_fuzzy(inpu_df):
    # Primeiramente, vamos definir os pontos de mudança para cada variável.

    t_H1 = -np.log(0.001)
    t_H2 = -np.log(0.001)
    t_H3 = -np.log(0.001)

    # Agora, vamos definir o ambiente do controlador Fuzzy responsável pela classificação.

    univ=np.arange(0, 10, 1/100)

    # Cria as variáveis do problema
    H1 = ctrl.Antecedent(univ, 'H1')
    H2 = ctrl.Antecedent(univ, 'H2')
    H3 = ctrl.Antecedent(univ, 'H3')
    status = ctrl.Consequent(np.arange(0, 4, 1/10), 'status')

    # Cria as funções de pertinência usando tipos variados
    H1['baixo'] = fuzz.sigmf(H1.universe, t_H1,-10)
    H1['alto'] = fuzz.sigmf(H1.universe, t_H1,10)

    H2['baixo'] = fuzz.sigmf(H2.universe, t_H2,-10)
    H2['alto'] = fuzz.sigmf(H2.universe, t_H2,10)

    H3['baixo'] = fuzz.sigmf(H3.universe, t_H3,-10)
    H3['alto'] = fuzz.sigmf(H3.universe, t_H3,10)

    status['verde'] = fuzz.sigmf(status.universe, 1,-50)
    status['amarelo'] = fuzz.psigmf(status.universe, 1, 50, 2, -50)
    status['laranja'] = fuzz.psigmf(status.universe, 2, 50, 3, -50)
    status['vermelho'] = fuzz.psigmf(status.universe, 3, 50, 4, -50)

    rule1 = ctrl.Rule(H1['baixo'] & H2['baixo'] & H3['baixo'], status['verde'])
    rule2 = ctrl.Rule((H1['alto'] & H2['baixo'] & H3['baixo']) | (H1['baixo'] & H2['alto'] & H3['baixo']) | (H1['baixo'] & H2['baixo'] & H3['alto']), status['amarelo'])
    rule3 = ctrl.Rule((H1['alto'] & H2['alto'] & H3['baixo']) | (H1['alto'] & H2['baixo'] & H3['alto']) | (H1['baixo'] & H2['alto'] & H3['alto']) , status['laranja'])
    rule4 = ctrl.Rule(H1['alto'] & H2['alto'] & H3['alto'], status['vermelho'])
    
    res=[]
    for x in inpu_df.index:
        inpu = inpu_df.loc[x].values
        status_ctrl = ctrl.ControlSystem([rule1, rule2, rule3,rule4])
        status_simulador = ctrl.ControlSystemSimulation(status_ctrl)
        try:
            status_simulador.input['H1'] = inpu[0]
            status_simulador.input['H2'] = inpu[1]
            status_simulador.input['H3'] = inpu[2]

            status_simulador.compute()

            vv = fuzz.interp_membership(status.universe, status['verde'].mf, status_simulador.output['status'])
            va = fuzz.interp_membership(status.universe, status['amarelo'].mf, status_simulador.output['status'])
            vl = fuzz.interp_membership(status.universe, status['laranja'].mf, status_simulador.output['status'])
            vvm = fuzz.interp_membership(status.universe, status['vermelho'].mf, status_simulador.output['status'])

            res.append(np.argmax([vv,va,vl,vvm]))
        except:
            res.append(4)
    outp_df=inpu_df
    outp_df["Classes"] = res
    return outp_df

#Define-se de quantos em quantos segundos se quer uma avaliação:

def check_status(inp_fuzzy,nsecs):
    dfn_filt = inp_fuzzy.groupby(np.arange(len(inp_fuzzy))//nsecs).mean()
    dfn_filt.index = inp_fuzzy.reset_index().groupby(np.arange(len(inp_fuzzy))//nsecs).first()["index"].values
    return Status_fuzzy(dfn_filt)

##Todo o Processo pode ser juntado em uma Classe chamada Barragem. Para tanto, nossa barragem do Paranoa
## será instanciada como Paranoa=Barragem(.). Não coloquei as funções aqui dentro, pra não poluir (não me importei
## se seriam acessadas por fora da Classe). Mas pode por se achar melhor.

class Barragem:
    def __init__(self,directory):
        self.sensores = ['R8CEBEHE', 'R8CEBEHN', 'R8CEBEHZ', 'R016AEHE', 
            'R016AEHN', 'R016AEHZ', 'RBAE5EHZ', 'RBAE5ENE', 
            'RBAE5ENN', 'RBAE5ENZ', 'RE647EHE', 'RE647EHN', 
            'RE647EHZ', 'RFB89EHE', 'RFB89EHZ', 'RFB89EHN']
        self.estado = 0
        self.metricaH1 = 0
        self.metricaH2 = 0
        self.metricaH3 = 0
        self.historico = []
        self.histtempo = []
        self.par_dfs = pd.read_json(directory+"/Scaler/scalerpar.json")
        self.cusum_dfs = pd.read_json(directory+"/CUSum/CUSumpar.json")
        self.src = directory
    def atualiza(self,dadosnovos,reseta=False):
        par_dfs = self.par_dfs
        cusum_dfs = self.cusum_dfs
        srcdirectory = self.src
        dicts_3 = dadosnovos        
        if sorted(list(dicts_3.keys())) == sorted(self.sensores):
            pass
        else:
            faltantes = [w for w in self.sensores if w not in dicts_3.keys()]
            raise ValueError('O(s) sensor(es) {foo} está(ão) offline, verificar em campo'.format(foo=repr(faltantes)))
        checknan = np.array([np.isnan(val) for val in dicts_3.values()])
        if checknan.any():
            inconsistentes = [list(dicts_3.keys())[w[0]] for w in np.argwhere(checknan)]
            raise ValueError('O(s) sensor(es) {foo} está(ão) com dados inconsistentes, verificar em campo'.format(foo=repr(inconsistentes)))
        sepdicts = [[keyvs,dicts_3[keyvs]] for keyvs in list(dicts_3.keys())]
        df_Hjorth_all = pd.concat([core_hjorth(d1) for d1 in sepdicts],axis=1)
        scal_inpu = scale_data(df_Hjorth_all,par_dfs)
        df_inputs_fuzzy = predict_AINOA(scal_inpu,srcdirectory)
        if reseta:
            Restart_Scores(df_inputs_fuzzy,srcdirectory)
            self.historico = []
            self.histtempo = []
        else:
            pass
        inp_fuzzy = CumSum_CPD_DF_N(df_inputs_fuzzy,cusum_dfs,0.0002,0.0,srcdirectory)
        sysstatus = check_status(inp_fuzzy,1)
        agora = datetime.datetime.now()
        self.metricaH1, self.metricaH2, self.metricaH3,self.estado = sysstatus.to_numpy()[-1]
        self.historico = self.historico+list(sysstatus["Classes"].to_numpy())
        self.histtempo = self.histtempo+list(np.arange(agora-datetime.timedelta(seconds=len(sysstatus)), agora,np.timedelta64(1,'s'), dtype='datetime64'))

##Para explicar melhor o que a Classe faz, considere a seguinte sequencia:
##Inicialmente, instaciamos apenas com o diretório onde estão a raiz do codigo
##Depois, precisamos que entrem os dados (dadosnovos) das 16 componentes medidas (4 sensores RShake3D e mais um RShake4D).
##Os dados são um dicionário em que cada chave é composta por "sensorid"+"componente", em que "sensorid" 
## é o nome do sensor e "componente" é a componente medida (EHE, EHZ, EHN etc). O valor atribuído à
## chave é um np.array com os valores registrados de amplitude. Por exemplo:
##    {'R8CEBEHE': array([-16576, -14946, -14166, ..., -15936, -19627, -19773]),
##     'R8CEBEHN': array([13093, 16284, 18977, ..., 20979, 19728, 16273]),
##     'R8CEBEHZ': array([16669, 16581, 14305, ..., 15594, 16404, 16500]),
##     'R016AEHE': array([-18026, -15435, -15142, ..., -15557, -18413, -18200]),
##     'R016AEHN': array([18062, 18376, 16294, ..., 14792, 16663, 19487]),
##     'R016AEHZ': array([15063, 17000, 17440, ..., 16048, 15602, 15854]),
##     'RBAE5EHZ': array([15450, 16339, 14842, ..., 16381, 17097, 16165]),
##     'RBAE5ENE': array([-227083, -228009, -226761, ..., -266014, -265651, -263011]),
##     'RBAE5ENN': array([-327968, -329352, -329619, ..., -295319, -294018, -293670]),
##     'RBAE5ENZ': array([3541828, 3541863, 3542949, ..., 3537435, 3536063, 3536530]),
##     'RE647EHE': array([-15240, -17471, -18096, ..., -16533, -18229, -15350]),
##     'RE647EHN': array([15449, 14627, 16859, ..., 15141, 19304, 18455]),
##     'RE647EHZ': array([15856, 15473, 16767, ..., 17275, 17646, 15690]),
##     'RFB89EHE': array([-15051, -16640, -16211, ..., -18292, -20653, -17632]),
##     'RFB89EHZ': array([16184, 16272, 17527, ..., 15539, 19139, 19058]),
##     'RFB89EHN': array([16512, 16605, 17161, ..., 17548, 23202, 17928])}

##Checamos então se todas as componentes dos dados estão presentes e levantamos um erro se não.
##Para simular os dados de entrada nesse formato, podemos usar a função simu_dados, que tem como
## entrada o número de segundos que se pretende simular e se os dados virão com erro (componentes faltando)
## Para simular 1 segundo de dados (ou seja, 1*100 entradas pois os dados são adquiridos a 100Hz) temos:

#dicts_3 = simu_dados(1)

##A função read_stream_obspy pode ser usada pra ler os arquivos mseed, se for esse o caminho
## de importação. Mas ai vai depender do deploy eu acho. 

##Agora, aplicamos a função que calcula os parâmetros de Hjorth para cada 100 dados colhidos:

#sepdicts = [[keyvs,dicts_3[keyvs]] for keyvs in list(dicts_3.keys())]
#df_Hjorth_all = pd.concat([core_hjorth(d1) for d1 in sepdicts],axis=1)

##Agora devemos aplicar o scaler nos dados de entrada. Para tanto, carregamos os dados do scaler calibrado e aplicamos:

#par_dfs = pd.read_json(srcdirectory+"\\Scaler\\scalerpar.json")

#scal_inpu = scale_data(df_Hjorth_all,par_dfs)

##Agora calculamos os erros de reconstrução a partir dos autoencoders calibrados, localizados em srcdirectory:

#df_inputs_fuzzy = predict_AINOA(scal_inpu,srcdirectory)

##É possível zerar as métricas de acompanhamento de erro usando a função:
    
#Restart_Scores(df_inputs_fuzzy,srcdirectory)

##Precisamos agora carregar os dados das séries de erros consideradas normais (calibrados):

#cusum_dfs = pd.read_json(srcdirectory+"\\CUSum\\CUSumpar.json")

#inp_fuzzy = CumSum_CPD_DF_N(df_inputs_fuzzy,cusum_dfs,0.0002,0.0,srcdirectory)
##Vamos prever então os estados do barramento a cada 1 segundo dos dados simulados (ou seja
## 1 previsão):
    
#statusfinal = check_status(inp_fuzzy,1)

##O resultado do código acima (statusfinal) um df da forma:
#          H1        H2        H3  Classes
#0   0.000774  2.844092  0.001290        0
    
##O estado do barramento está na coluna Classes, em que 0, 1, 2 e 3 são Verde, Amarelo, Laranja e Vermelho.

##Não sei como você prefere chamar as telas...
##No final das contas, basicamente vamos instanciar a nossa barragem e depois atualizar quantas vezes 
## forem necessárias. Por exemplo:

Paranoa = Barragem(srcdirectory)

print([Paranoa.estado,Paranoa.metricaH1,Paranoa.metricaH2,Paranoa.metricaH3])
print("\n\n\n#### sim dados:")
print(simu_dados(1))
##Atualizamos 1x a partir de um estado existente:
Paranoa.atualiza(simu_dados(1))

print([Paranoa.estado,Paranoa.metricaH1,Paranoa.metricaH2,Paranoa.metricaH3])

##Atualizamos novamente, também a partir de um estado existente:
Paranoa.atualiza(simu_dados(1))

print([Paranoa.estado,Paranoa.metricaH1,Paranoa.metricaH2,Paranoa.metricaH3])

##Agora atualizamos após resetar os estados do algoritmo de controle de erro:
Paranoa.atualiza(simu_dados(1),True)

print([Paranoa.estado,Paranoa.metricaH1,Paranoa.metricaH2,Paranoa.metricaH3])

time.sleep(1)

Paranoa.atualiza(simu_dados(1))

print([Paranoa.estado,Paranoa.metricaH1,Paranoa.metricaH2,Paranoa.metricaH3])

##E por ai vai...