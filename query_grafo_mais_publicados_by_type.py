import json
from query_grafo_mais_publicado2s import Mais_publicados
from elasticsearch import Elasticsearch
import io
import timeit


class Mais_publicados_by_type:
    
    info=[]
    qtd_article=[]
    qtd_conference=[]
    result=[]
    es=Elasticsearch([{'host':'localhost','port':9200}])
    print(es)
    
    def mais_publicados(self):
        key_aggs = self.busca_agregacao(51)
        res_ids = self.busca_ids();
   
        for i in range(len(key_aggs)):        
            self.function_buscaIDS(key_aggs[i],res_ids)
        
        return self.info

    def principal(self):
        mp = self.mais_publicados()
      
        for i in range(len(mp)):
            art = self.quantidade_article(mp[i]['idlattes'])
            conf = self.quantidade_conference(mp[i]['idlattes'])
          
            self.result.append({"key":mp[i]['key'],
                                "artigos_eventos":conf[0]['doc_count'],
                                "artigos_periodicos":art[0]['doc_count'],
                                "idlattes": mp[i]['idlattes']})
        with io.open('query_grafo_article_conference.json', 'w',encoding='utf-8') as outfile:       
            json.dump(self.result, outfile,ensure_ascii=False)

     

    def quantidade_article(self,idlattes):
        qtd_article = self.busca_article_conference(idlattes,"article")
        return qtd_article

    def quantidade_conference(self,idlattes):
        qtd_conference = self.busca_article_conference(idlattes,"conferenceObject")
        return qtd_conference
    #Valida Ids E INCLUI NO JSON  
    def function_buscaIDS(self,key_aggs,res_ids):
        teste=1
       
        for j in range(len(res_ids)):
            if(key_aggs['key'] == res_ids[j]['_source']['idCNPq']):                       
                self.info.append({"key":res_ids[j]['_source']['nome'],                                
                                "idlattes":res_ids[j]['_source']['idCNPq']})
               
                teste=2
                return
        if(teste == 1):
            print('ID Não Localizado:',key_aggs['key'])

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
    
    def busca_article_conference(self,idlattes,tipo):
        res = self.es.search(index="ufscar", body={
         "size": 0,
         "query": {
          "bool": {"must": [
            {
                "match": {
                "dc.type.value.keyword": tipo
            }},
            { "match": { "dc.contributor.author.authority.keyword": idlattes   }}
            ]}
            },
                "aggs": {
                        "group_by_name": {
                        "terms": {
                        "field": "dc.contributor.author.authority.keyword",
                        "size": 1, 
                        "order": {
                                    "_count": "desc"
                                 }
                        }
                        }
                }
        })        
        return res['aggregations']['group_by_name']['buckets']   
    
   

mp = Mais_publicados_by_type()
mp.principal()
