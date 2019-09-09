import json
from query_grafo_mais_publicado2s import Mais_publicados
from elasticsearch import Elasticsearch
#from unicodedata import normalize
import io
import timeit


class Autor_ano_acumulado:

    es=Elasticsearch([{'host':'localhost','port':9200}])
    print(es)

    autor_ano=[]
    ac=0

    def principal(self):
        mais_publicados = Mais_publicados()
        mp = mais_publicados.busca_grafo_mais_publicados_dept()
        for i in range(len(mp)):
            self.mais_publicados_ano(mp[i]['idlattes'], mp[i]['key'])
       
        with io.open('autor_ano_acumulado.json', 'w',encoding='utf-8') as outfile:       
            json.dump(self.autor_ano, outfile,ensure_ascii=False)     


    def mais_publicados_ano(self,idlattes,nome):
        res_autor_ano = self.busca_autor_ano(idlattes)
        self.ac=0        
        for i in range(len(res_autor_ano['aggregations']['date']['buckets'])):
            
            for j in range(len(res_autor_ano['aggregations']['date']['buckets'][i]['subaggs']['buckets'])):
                if(idlattes == res_autor_ano['aggregations']['date']['buckets'][i]['subaggs']['buckets'][j]['key']):

                    self.ac = self.ac + res_autor_ano['aggregations']['date']['buckets'][i]['subaggs']['buckets'][j]['doc_count']
                    
                    self.autor_ano.append({"nome":nome,
                                           "ano":res_autor_ano['aggregations']['date']['buckets'][i]['key'],
                                           "quantidade":res_autor_ano['aggregations']['date']['buckets'][i]['subaggs']['buckets'][j]['doc_count'],
                                           "acumulado":self.ac})
                   
                    break
         
    def busca_autor_ano(self,idlattes):
        res = self.es.search(index="ufscar", body={
       "size": 0,
       
        "aggs": {
            "date": {
              "terms": {
                "field": "dc.date.issued.value.keyword",
                "size": 200, 
                "order": {
                  "_key": "asc"
                }
              },"aggs": {
                "subaggs": {
                  "terms": {
                    "field":"dc.contributor.author.authority.keyword"
                  }
                  }
                }
              }
            }
        })
        return res

  
inicio = timeit.default_timer()
ac = Autor_ano_acumulado()
ac.principal()
fim = timeit.default_timer()
print ('duracao: %f' % (fim - inicio))

    
