# TCC: Correlação de ataques de tubarão com maré e tempo

O objetivo desse repositório é manter os scripts python utilizados para coleta e processamento dos dados usados no meu trabalho de conclusão de curso (TCC).

## Como rodar

### Pré-requisitos

- Python 3.12.7
- Pip
- Git

### Instalação

1. Clone o repositório
2. Crie um ambiente virtual com `python -m venv venv` e ative-o com `source venv/bin/activate`
3. Instale as dependências com `pip install -r requirements.txt`
4. Copie o arquivo `src/.env.example` para `src/.env` e preencha com suas chaves de API
5. Rode o script `src/concurrent/main.py` para coletar os dados dos incidentes com tubarões e enriqueçer com dados meteorológicos
6. Rode o script `scripts/plot_shark_attacks.sh` para gerar os gráficos de incidentes com tubarões

## Estrutura do repositório

### `src`

Contém os scripts python utilizados para coleta e processamento dos dados, bem como geração dos gráficos.

### `data`

Contém os dados coletados do Global Shark Attack File (GSAF), que é a base de dados utilizada para os incidentes com tubarões.

### `out`

Contém os resultados do script python `src/concurrent/main.py`, que é o script responsável por coletar os dados dos incidentes com tubarões e enriquecer com dados meteorológicos.

### `plots`

Contém os gráficos gerados pelo script `src/plot_shark_attacks.py`. Esse script não precisa ser rodado manualmente, pois o script `scripts/plot_shark_attacks.sh` já faz isso.

### `scripts`

Contém scripts shell utilizados para automatizar a execução dos scripts python.

## Serviços de terceiros

### Google Maps Geocoding API

É necessário possuir uma chave de API para conseguir fazer as requisições de geocodificação, para isso é necessário criar uma conta na Google Cloud Platform, criar um projeto e ativar a chave de API para geocodificação pelo Google Maps.

### Google Timezone API

É necessário possuir uma chave de API para conseguir fazer as requisições de fuso horário, para isso é necessário criar uma conta na Google Cloud Platform, criar um projeto e ativar a chave de API para fuso horário pelo Google Maps.
É possível usar a mesma chave de API para geocodificação e fuso horário.

### OpenMeteo

É necessário possuir uma chave de API para conseguir fazer as requisições de dados meteorológicos, para isso é necessário criar uma conta na OpenMeteo e gerar uma chave de API. Apesar disso, o serviço é gratuito para uso não comercial e suporta até 600 requisições por minuto.
