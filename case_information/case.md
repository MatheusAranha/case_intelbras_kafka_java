Case Intelbras Backend Sênior
O objetivo desse case é avaliar conhecimentos de desenvolvimento backend, arquitetura orientada a eventos e capacidade de projetar uma solução escalável utilizando Java e Apache Kafka. Queremos enxergar como você organiza o código, trata falhas, toma decisões de arquitetura e comunica suas escolhas.











INTELBRAS  •  CASA INTELIGENTE

intelbras

Case Técnico

Processo seletivo — Backend Sênior Java
1. Contexto
A Intelbras possui um ecossistema de dispositivos IoT que gera eventos de diferentes naturezas, como alterações de propriedades, mudanças de status, detecção de movimento e outros eventos relacionados ao funcionamento dos dispositivos.
Neste case, você assume o desafio de construir uma solução backend para receber e processar esse tipo de evento. Será disponibilizado um arquivo CSV contendo 100 eventos baseados em eventos reais da plataforma, com os dados sensíveis devidamente anonimizados.
O objetivo não é reproduzir uma solução interna da Intelbras, e sim demonstrar como você estrutura código, projeta um fluxo orientado a eventos e prepara a aplicação para crescer de forma segura e organizada.
2. O Desafio
A partir do arquivo disponibilizado, desenvolva uma aplicação capaz de simular a entrada dos eventos e processá-los através de uma arquitetura orientada a eventos utilizando Apache Kafka.
    • Realizar a leitura e o disparo dos eventos presentes no CSV.
    • Publicar os eventos no Kafka e implementar o fluxo necessário para seu processamento.
    • Processar diferentes tipos de evento sem que a falha de uma mensagem interrompa as demais.
    • Estruturar a aplicação de forma organizada e adequada a um backend de produção.
    • Considerar que os 100 eventos são apenas uma amostra e que o cenário real poderá atingir milhões de eventos por dia.
O dataset do case será enviado junto com este enunciado. Os dados foram anonimizados e não devem ser interpretados como credenciais ou identificadores válidos de produção.
2.1 Requisitos Funcionais Mínimos
    • RF01 — Permitir a leitura e o disparo dos eventos disponibilizados no arquivo CSV.
    • RF02 — Publicar os eventos em Kafka para processamento assíncrono.
    • RF03 — Implementar consumidor(es) para processamento dos eventos.
    • RF04 — Tratar diferentes tipos de evento presentes no dataset.
    • RF05 — Registrar logs que permitam acompanhar o processamento dos eventos.
2.2 Requisitos Técnicos Mínimos
    • RT01 — Backend desenvolvido em Java.
    • RT02 — Utilização de Apache Kafka.
    • RT03 — Organização clara das responsabilidades e componentes da aplicação.
    • RT04 — Tratamento de erros sem interromper o processamento global.
    • RT05 — Estratégia para lidar com eventos duplicados ou reprocessados.
    • RT06 — Testes automatizados para os principais comportamentos.

3. Entregáveis
A entrega é dividida em três blocos, todos obrigatórios:
Bloco A — Implementação
    • Aplicação funcional desenvolvida em Java.
    • Integração com Apache Kafka para publicação e consumo dos eventos.
    • Código organizado, legível e com responsabilidades bem definidas.
    • Tratamento de falhas e cenários de reprocessamento.
    • Testes automatizados dos principais fluxos.
    • README com instruções para execução local.
Bloco B — Arquitetura
Apresente um diagrama simples da arquitetura proposta e descreva como a solução seria preparada para um cenário de produção com milhões de eventos por dia. Aborde:
    • Escalabilidade e estratégia de particionamento no Kafka.
    • Consumer Groups e paralelismo de processamento.
    • Ordenação dos eventos quando necessária.
    • Idempotência e tratamento de duplicidade.
    • Retry e tratamento de mensagens que não puderam ser processadas.
    • Observabilidade: logs, métricas e alertas.
    • Disponibilidade e tolerância a falhas.
Bloco C — Decisões Técnicas
Documente brevemente as principais decisões técnicas adotadas e seus trade-offs. O uso de Spring Boot, Docker/Docker Compose e demais bibliotecas ou ferramentas fica a critério do candidato.
Diferencial
Bônus
Versionamento do repositório do case no GitHub/GitLab
★★★
Execução local simplificada com Docker Compose
★★
Métricas e observabilidade da aplicação
★★
Testes de integração envolvendo Kafka
★★
4. Apresentação
Após a entrega, será agendada uma sessão com o time para apresentação da solução:
    • 20 min — Walkthrough da arquitetura e do fluxo de eventos.
    • 15 min — Walkthrough do código e principais decisões técnicas.
    • 15 min — Perguntas técnicas e discussão de cenários de escala/falha.

5. Critérios de Avaliação
Eixo
Peso
O que avaliamos
Arquitetura e Kafka
30%
Coerência do desenho, escalabilidade, particionamento, resiliência e decisões sobre mensageria.
Qualidade de Código
25%
Organização, legibilidade, separação de responsabilidades e tratamento de erros.
Conhecimento Técnico
20%
Domínio de Java, Kafka, concorrência, idempotência e justificativas técnicas.
Testes e Confiabilidade
15%
Qualidade dos testes e cobertura dos principais cenários de falha e processamento.
Comunicação
10%
Clareza na documentação, apresentação e explicação dos trade-offs.
6. Prazo e Entrega
    • Prazo: 7 dias corridos a partir do recebimento do case.
    • Como entregar: envie o link do repositório e a documentação solicitada no formato indicado pelo RH.
7. Importante
    • Esteja preparado para explicar e justificar cada decisão tomada durante a implementação.
    • Não existe uma única arquitetura considerada correta; as decisões e seus trade-offs fazem parte da avaliação.
    • O dataset disponibilizado é uma amostra funcional e não representa um teste de carga.
    • Não publique dados adicionais da Intelbras ou informações obtidas fora do material disponibilizado para o case.
Boa sorte! Mais do que concluir todos os itens, queremos entender como você pensa, estrutura e sustenta suas decisões técnicas.
