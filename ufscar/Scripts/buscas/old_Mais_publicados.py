import json
import io
from elasticsearch import Elasticsearch

class Mais_publicados:
    info=[]
    info_dept=[]
    # Connect to the elastic cluster
    es=Elasticsearch([{'host':'localhost','port':9200}])
    print(es)

    def busca_grafo_mais_publicados_dept(self):
        key_aggs = self.busca_agregacao(50)
        res_ids = self.busca_ids();
        for i in range(len(key_aggs)):        
            self.function_buscaIDS(key_aggs[i],res_ids)
        return self.info_dept 
        

    def busca_grafo_mais_publicados(self):
    
        key_aggs = self.busca_agregacao(50)
        res_ids = self.busca_ids();
   
        for i in range(len(key_aggs)):        
            self.function_buscaIDS(key_aggs[i],res_ids)
        
        return self.info
        #with io.open('query_grafo_mais_publicados.json', 'w',encoding='utf-8') as outfile:       
            #json.dump(self.info, outfile,ensure_ascii=False)       


    #Valida Ids E INCLUI NO JSON  
    def function_buscaIDS(self,key_aggs,res_ids):
   
        for j in range(len(res_ids)):
            if(key_aggs['key'] == res_ids[j]['_source']['idCNPq']):                       
                self.info.append({"key":res_ids[j]['_source']['nome'],
                                "doc_count":key_aggs['doc_count'],
                                "idlattes":res_ids[j]['_source']['idCNPq']})
                self.info_dept.append({"key":res_ids[j]['_source']['nome'],
                                "value":key_aggs['doc_count'],
                                "idlattes":res_ids[j]['_source']['idCNPq'],
                                "area":res_ids[j]['_source']['departamento']
                                       })
                return
        
    def busca_agregacao(self,numeroPublicacoes):
        res = self.es.search(index="ufscar", body={
        "size": 0,
          "aggs": {
            "group_by_name": {
              "terms": {
                "field": "dc.contributor.author.authority.keyword",
                "size": numeroPublicacoes,
                "order": {
                    "_count": "desc"
                }
              }
            }
        }
        })

        return res['aggregations']['group_by_name']['buckets']

    def busca_ids(self):
       
        res = self.es.search(index="ufscar_ids", body={    
            "size": 10000,
           "query": {"match_all": {}}
        })

        return res['hits']['hits']
    
mp = Mais_publicados()
mp.busca_grafo_mais_publicados()


      
      
    
