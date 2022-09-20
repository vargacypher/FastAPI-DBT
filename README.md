<p align="center">
  <img src="project/dbt_packages/dbt_utils/etc/dbt-logo.png" alt="dbt logo" />
</p>


# DBT Basics

## YMLs

### project.yml


    Usado para configurações gerais do projeto como paths.

### sources.yml

    Descreve tabelas do banco que são utilizadas os ``models``


### schema.yml

    Usado para descrever views e testes.


## CLI

### Gerar Lineage/docs

``dbt docs generate --vars '{"database":"teste","profile":"general","dates":"false","host":"localhost","port":9000,"user":"default","password": ""}'``


### Abre docs no navegador

``dbt docs serve --vars '{"database":"teste","profile":"general","dates":"false","host":"localhost","port":9000,"user":"default","password": ""}'``


### Rodar Model específico


    "-s", "--select"


``dbt run -m models/staging/ingestion_validation --vars '{"database":"teste","profile":"general","dates":"false","host":"localhost","port":9000,"user":"default","password": ""}'``


### Insights

#### Gerar views nas marcas
`` dbt run -m models/marts/oto-insights/export-insights --vars '{"database":"teste","profile":"general","host":"localhost","port":9000,"user":"default","password": ""} '``



### OBS
`` dbt run -m models/marts/oto-insights/export-insights --vars '{"database":"teste","profile":"general"} ' `` 

O arquivo profile possui valores dafault para os parametros (User,Password,Port,Host) não sendo necessário passar esses parametros.
