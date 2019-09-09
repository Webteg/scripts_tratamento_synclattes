import json
from flask import Flask, jsonify,json,Response
from query_grafo_mais_publicado2s import Mais_publicados
#from Mais_publicados_coautores import Mais_publicados_coautores
from Autor_ano_acumulado import Autor_ano_acumulado

app = Flask(__name__)
#problema com acentuação
app.config['JSON_AS_ASCII'] = False


mp = Mais_publicados()
#mpc = Mais_publicados_coautores()
ac = Autor_ano_acumulado()

#caminho para API JSON
'''
@app.route('/lattes/api/v1.0/mais_publicados', methods=['GET'])
def get_tasks():
    autor_publicacao = mp.busca_grafo_mais_publicados()    
    return jsonify({'tasks': autor_publicacao})

#02caminho para API JSON
@app.route('/lattes/api/v1.0/mais_publicados_coautores', methods=['GET'])
def get_tasks_2():
    autor_publicacao_coautor = mpc.principal()    
    return jsonify({'tasks': autor_publicacao_coautor})
'''
#03caminho para API JSON
@app.route('/lattes/api/v1.0/autor_ano_acumulado', methods=['GET'])
def get_tasks_3():
    ano_acumulado = ac.principal()    
    return jsonify({'tasks': ano_acumulado})

if __name__ == '__main__':
    app.run(debug=True)





