import json
from Mais_publicados import Mais_publicados
from elasticsearch import Elasticsearch
#from unicodedata import normalize
import io


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

        return self.json_agrupado[0]
        #with io.open('json_agrupado.json', 'w',encoding='utf-8') as outfile:       
            #json.dump(self.json_agrupado[0], outfile,ensure_ascii=False)        
       
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

          },request_timeout=30)

        return res
mpc = Mais_publicados_coautores()
mpc.principal()




        
      
    
