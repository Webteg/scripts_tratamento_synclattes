import json
from query_grafo_mais_publicado2s import Mais_publicados
from elasticsearch import Elasticsearch
#from unicodedata import normalize
import io
import timeit


class Mais_publicados_coautores:

    
    es=Elasticsearch([{'host':'localhost','port':9200}])
    print(es)
    
    link=[]
    identificador=[]
    link_identificador=[]
    link_identificador_completo=[]
    json_agrupado=[]
    
    def principal(self):        
        mais_publicados = Mais_publicados()
        mp = mais_publicados.busca_grafo_mais_publicados_dept()

        res_autor = self.busca_agregacao_por_autor()
           
        #busca BUCKETS agregações em elastic
        for i in range(len(mp)):
            self.identificador.append({"id":i,"idlattes":mp[i]['idlattes']})
            
            self.buscar_coautores(mp[i],res_autor)
            self.atribuir_identificador(self.link,self.identificador)

        #agrupar jsons
        self.json_agrupado.append({"nodes":mp,"links":self.link_identificador_completo})

        with io.open('json_agrupado.json', 'w',encoding='utf-8') as outfile:       
            json.dump(self.json_agrupado[0], outfile,ensure_ascii=False)        
       
    def atribuir_identificador(self,link,identificador):
        #para SOURCE
        for i in range(len(link)):
            for j in range(len(identificador)):                
                if(link[i]['source'] == identificador[j]['idlattes']):
                    self.link_identificador.append({"source": identificador[j]['id'], "target": link[i]['target'],
                            "value":link[i]['value']})
                    
                    break
       
        #para TARGET
        for i in range(len(self.link_identificador)):
            for j in range(len(identificador)):
                if(self.link_identificador[i]['target'] == identificador[j]['idlattes']):
                    self.link_identificador_completo.append({"source": self.link_identificador[i]['source'],
                            "target": identificador[j]['id'],
                            "value":self.link_identificador[i]['value']})
                    break        
    def buscar_coautores(self, mp, res_autor):
        for j in range(len(res_autor['aggregations']['autor']['buckets'])):           
            if(mp['idlattes'] == res_autor['aggregations']['autor']['buckets'][j]['key']):                               
                for k in range(len(res_autor['aggregations']['autor']['buckets'][j]['subaggs']['buckets'])):
                    if(res_autor['aggregations']['autor']['buckets'][j]['subaggs']['buckets'][k]['key'] != mp['idlattes']):                       
                        self.link.append({"source": mp['idlattes'],
                                          "target": res_autor['aggregations']['autor']['buckets'][j]['subaggs']['buckets'][k]['key'],
                            "value": res_autor['aggregations']['autor']['buckets'][j]['subaggs']['buckets'][k]['doc_count']})
                       
                
                return
    def busca_agregacao_por_autor(self):
       
        res = self.es.search(index="ufscar", body={
        "_source": ["dc.contributor.author.authority","dc.contributor.author._nomecompleto"], 
        "size": 0,   
          "aggs": {
            "autor": {
              "terms": {
                "field": "dc.contributor.author.authority.keyword",
                 "size": 10000,
                "order": {
                  "_count": "desc"
                }
              },"aggs": {
                "subaggs": {
                  "terms": {
                    "field":"dc.contributor.author.authority.keyword" }
                  }
                }
              }
            }

          })

        return res

#inicio = timeit.default_timer()


mpc = Mais_publicados_coautores()
mpc.principal()

#fim = timeit.default_timer()
#print ('duracao: %f' % (fim - inicio))



'''
#BUSCA PARA CADA NOME AS INFORMAÇÕES SOBRE AUTHORITY - INFO_AUTOR SERÁ PASSADO O NOME DE CADA AUTOR (50 MAIS PUBLICADOS)
def procurar_idCNPQ(nomes):

    res_ids = busca_ids()
    info=[]
    info2=[]
    info_autor=[]
    
    for i in range(len(nomes)):        
        info_autor = busca_info_autor(nomes[i]['name'])

        function_loops(info_autor,nomes[i],res_ids,info2)   
    
    return info2

def function_loops(info_autor,nomes,res_ids,info2):
    index=-1
    for j in range(len(info_autor['hits']['hits'])):            
        for w in range(len(info_autor['hits']['hits'][j]['_source']['dc.contributor.author'])):                    
            try:
                if(nomes["name"] == info_autor['hits']['hits'][j]['_source']['dc.contributor.author'][w]['_nomecompleto']):                           
                            
                    index=index+1
                    valida = function_buscaIDS(nomes["name"],info_autor['hits']['hits'][j]['_source']['dc.contributor.author'][w]['authority'],
                                                       nomes['value'],
                                                       res_ids,info2)

                    if(valida):                                
                        return
                    else:
                        continue
            except KeyError:
                continue
    return

#BUSCA OS DEPARTAMENTOS E INCLUI NO JSON  
def function_buscaIDS(nome_autor,authority,valor,res_ids,info2): 
    existe=None
    for j in range(len(res_ids['hits']['hits'])):
        if(authority == res_ids['hits']['hits'][j]['_source']['idCNPq']):
            info2.append({"name":res_ids['hits']['hits'][j]['_source']['nome'],
                        "value":valor,
                        "idlattes":authority,
                        "area":res_ids['hits']['hits'][j]['_source']['departamento']})
            existe=True
            break
    return existe   

def montar_json_grafo():
    qtd_busca = 50
    res_autor = busca_agregacao_por_autor(qtd_busca,"dc.contributor.author._nomecompleto.keyword")   
   
    #busca BUCKETS agregações em elastic            
    for i in range(len(res_autor['aggregations']['autor']['buckets'])):
        nomes.append({"id":i,"name":res_autor['aggregations']['autor']['buckets'][i]['key'],
                      "value":res_autor['aggregations']['autor']['buckets'][i]['doc_count']})
       
        #json autor por coautor   
        for j in range(len(res_autor['aggregations']['autor']['buckets'][i]['subaggs']['buckets'])):
            if(res_autor['aggregations']['autor']['buckets'][i]['key'] != res_autor['aggregations']['autor']['buckets'][i]['subaggs']['buckets'][j]['key']):

                autor_coautor.append({"source":res_autor['aggregations']['autor']['buckets'][i]['key'],
                               "target":res_autor['aggregations']['autor']['buckets'][i]['subaggs']['buckets'][j]['key'],
                               "value":res_autor['aggregations']['autor']['buckets'][i]['subaggs']['buckets'][j]['doc_count']})

    #atribui os IDs para o TARGET                  
    for i in range(len(nomes)):        
        for j in range(len(autor_coautor)):                
            if( (autor_coautor[j]['target'] == nomes[i]["name"])):
                aux_autor_coautor.append({"source":autor_coautor[j]['source'],
                               "target":nomes[i]["id"],
                               "value":autor_coautor[j]['value']})
                break
    #atribui os IDs para o SOURCE 
    for i in range(len(nomes)):
        for j in range(len(aux_autor_coautor)):                
            if( (aux_autor_coautor[j]['source'] == nomes[i]["name"])) :                    
                aux2_autor_coautor.append({"source":nomes[i]["id"],
                               "target":aux_autor_coautor[j]["target"],
                               "value":aux_autor_coautor[j]['value']})              
                break
            
    #CHAMA FUNCAO PARA PROCURAR IDCNPQ   
    aux_nomes = procurar_idCNPQ(nomes)

    print(len(aux_nomes))

    #agrupar jsons
    json_agrupado.append({"nodes":aux_nomes,"links":aux2_autor_coautor})

    with io.open('json_agrupado.json', 'w',encoding='utf-8') as outfile:       
        json.dump(json_agrupado[0], outfile,ensure_ascii=False) 


def busca_info_autor(nome):
    res = es.search(index="ufscar", body={
        "_source": ["dc.contributor.author._nomecompleto","dc.contributor.author.authority"],
        "query": {
            "bool": {
                "must": [
                    {
                        "match": {
                            "dc.contributor.author._nomecompleto.keyword": nome
                        }
                    }
                ]
            }
        }
     })
    return res

'''

'''
def busca_ids():
       
    res = es.search(index="ufscar_ids", body={    
        "size": 10000,
       "query": {"match_all": {}}
    })

    return res
   
montar_json_grafo()

'''
        
      
    
