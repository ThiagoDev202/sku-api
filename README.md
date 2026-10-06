# Integração de lotes de SKUs

Serviço API-only que recebe mensagens de lotes, consulta preço e estoque, consolida respostas e envia um callback final. A implementação usa Node.js 24, TypeScript, Fastify, BullMQ e Redis.

## Escopo e execução

O serviço expõe `POST /check` e `POST /process`. A integração com a plataforma usa `POST /register`, `POST /burst/:cid`, `GET /enrich/:sku` e `POST /callback`. O registro e cada burst são operações explícitas; reiniciar os containers não cria outro lote.

Pré-requisitos: Docker Compose e uma URL HTTPS pública que encaminhe para a porta da API. Crie o arquivo local de ambiente e configure o endereço do túnel e o nome do registro:

```sh
cp .env.example .env
```

Inicie os processos:

```sh
docker compose up -d --build
docker compose ps
```

Registre o webhook uma vez e solicite lotes quando necessário:

```sh
docker compose exec api npm run register
docker compose exec api npm run burst
```

Se um lote ficar bloqueado por falha terminal, corrija a causa externa e solicite explicitamente o replay da sequência falha:

```sh
docker compose exec -T api npm run reprocess-failure -- ID_DA_EXECUCAO SEQ_FALHA
```

O comando só aceita sequências falhas de um lote `BLOCKED_FOR_POLICY`; usa o SKU original e a mesma chave `(run_id, seq)`. Chamadas repetidas do comando para o mesmo replay compartilham um ticket/job ID estável. Redeliveries de `/process` não reprocessam falhas. Após o sucesso de todas as sequências, o finalizador envia o callback completo uma única vez. O comando não consome a DLQ automaticamente.

O comando `burst` cria um relatório novo na plataforma. Execute-o apenas quando quiser iniciar outro lote. A API escuta na porta configurada em `PORT`, que por padrão é `4000`.

O volume Redis permanece após `docker compose down`. `docker compose down -v` também apaga os dados persistidos do desafio.

## Arquitetura

- A API valida DTOs com JSON Schema fechado. `/process` só responde 2xx depois do aceite BullMQ e nunca aguarda o enriquecimento. Se Redis não confirmar o enqueue em 350 ms, a API responde 503 sem confirmar a mensagem.
- A identidade `(run_id, seq)` gera um ID BullMQ determinístico e um marcador Redis sem TTL. Jobs concluídos e falhos são retidos para que uma redelivery tardia não crie outro enriquecimento.
- Workers compartilham concorrência global três na fila. Resultados fora de ordem são gravados uma vez no Redis e ordenados por `seq` antes do callback.
- Respostas 429 respeitam `Retry-After`; 500 e rede têm até três tentativas totais. 404 e 401 são permanentes. Falhas terminais vão à `enrichment-dlq`, que não tem consumidor automático.
- O finalizador reserva `CALLBACK_ATTEMPTED` antes do POST. Um 2xx marca `SENT`; uma resposta não-2xx ou resultado de transporte ambíguo marca `UNKNOWN` e não é repetido às cegas.
- Não há callback de progresso nem callback parcial. Qualquer falha terminal bloqueia o lote inteiro; sucessos e falhas permanecem armazenados para diagnóstico. Para tentar novamente, um operador deve acionar explicitamente `reprocess-failure` com o `run_id` e `seq` falho. O comando usa o SKU original, preserva `(run_id, seq)` e não é acionado por redelivery.

## Configuração

| Variável | Uso |
|---|---|
| `PORT` | Porta HTTP da API; padrão `4000`. |
| `PLATFORM_BASE_URL` | URL HTTPS da plataforma avaliadora; vem preenchida em `.env.example`. |
| `PUBLIC_WEBHOOK_URL` | URL HTTPS pública do túnel que encaminha para a API. |
| `REGISTRATION_NAME` | Nome enviado no registro do webhook. |
| `REDIS_URL` | Redis compartilhado por API, workers e CLI; no Compose usa `redis://redis:6379`. |
| `REDIS_MAXMEMORY` | Limite inicial do Redis; padrão `256mb`. |

Redis usa volume nomeado, AOF com `appendfsync everysec` e `maxmemory-policy noeviction`. A política pode perder até aproximadamente um segundo de escritas numa queda abrupta. `noeviction` preserva marcadores de idempotência; sob pressão, gravações falham em vez de remover estado silenciosamente.

## Persistência, cache e o lote de 20.000 SKUs

O estado operacional fica no Redis compartilhado, não na memória de um processo. API e worker são processos separados e precisam observar os mesmos marcadores, resultados e transições atômicas. SQLite ou outra base não acrescentaria uma necessidade coberta pelo desafio: BullMQ já exige Redis, e não há requisito de histórico consultável independente.

Também não há cache L1/L2 de preços e estoques. Um cache em memória diverge entre processos e reinícios; Redis já guarda o resultado por `(run_id, seq)` para impedir a repetição do trabalho. Para 20 SKUs, outra camada de cache não traz benefício proporcional à complexidade.

O desafio não define por quanto tempo a plataforma pode reenviar mensagens. Por isso, marcadores e resultados não expiram automaticamente. Essa escolha mantém idempotência para redeliveries tardias, mas faz o uso de disco e memória crescer a cada lote até uma limpeza explícita. O limite de 256 MiB é um ponto inicial para o cenário local de 20 itens, não uma capacidade declarada para 20.000; medir o footprint real e ajustar o limite antes de aumentar o volume.

Com a latência indicada pelo desafio de 400–800 ms por `/enrich` e três chamadas em voo, 20.000 SKUs levariam aproximadamente **44–89 minutos** em condições ideais, antes de retries e overhead. Esse valor é uma estimativa aritmética, não uma medição. O limite externo de três chamadas mantém-se mesmo adicionando workers.

## Testes e validação

O agente guardião escreveu os testes unitários antes da implementação e foi o único a executá-los. Testes automatizados de API e integração foram executados depois da aprovação unitária. Este serviço não possui interface visual, então Playwright não é necessário.

```sh
docker compose exec -T api npm run test:unit
docker compose exec -T api npm run test:integration
docker compose exec -T api npm run typecheck
docker compose exec -T api npm run build
docker compose config --quiet
```

O agente guardião foi o único a escrever e executar os testes unitários. A última execução unitária registrada, em 05/10/2026 após implementar replay manual explícito, aprovou **41/41 testes**. Em 06/10/2026, a suíte de integração foi reexecutada no Compose com Node.js 24 e Redis/BullMQ reais: **5 arquivos e 10/10 testes aprovados**. A compilação, o typecheck e `docker compose config --quiet` também passaram no container Node.js 24. A integração cobre dois workers concorrentes, retenção de falhas na DLQ, captura limitada do corpo da resposta de callback e recuperação com callback completo. Um smoke test anterior reiniciou Redis e API e confirmou que o job e seu marcador permaneceram únicos.

### Relatório da medição pelo túnel

- Em 06/10/2026 foi executado um burst adicional, `run_id` `us2otq1njyxbxamzliy23xij`, com 20 SKUs. O corpo de 803 bytes da resposta HTTP 2xx do callback foi capturado localmente, sem truncamento. O resumo oficial retornado informa score 100, 20 ACKs medidos, `ack.pass=true`, P50 **501 ms**, P95 e pior ACK **565 ms**, zero ACKs acima de 600 ms e zero falhas. A medição oficial confirma o SLA nesta execução, com margem de 35 ms.
- O resumo oficial não contém média aritmética nem amostras individuais; P50 é mediana e não pode ser apresentado como média. Portanto, a média oficial exata continua indisponível. O relatório detalhado desta verificação está em [RELATORIO-DE-EXECUCAO-2026-10-06-AVALIADOR.md](RELATORIO-DE-EXECUCAO-2026-10-06-AVALIADOR.md).
- Os PDFs pedem explicar o que mudaria para 20.000 SKUs, não executar um teste de carga nesse volume. A reflexão de escala está acima; 20.000 SKUs não foram testados e o projeto não declara capacidade medida para esse volume.
