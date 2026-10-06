# Relatório de execução — resumo do avaliador

**Data:** 06/10/2026

**Run ID:** `us2otq1njyxbxamzliy23xij`

**Origem:** burst executado pelo serviço local, com o endpoint ngrok registrado

**Resultado:** aprovado pelo avaliador

## Resultado oficial

Após a conclusão do lote de 20 SKUs, o avaliador aceitou o callback e devolveu um corpo JSON de 803 bytes. O serviço o capturou no Redis local, associado ao `run_id`, sem truncamento. O resumo informa score 100 e os seguintes dados de ACK:

| Métrica | Resultado |
|---|---:|
| ACKs medidos | 20 |
| P50 | 501 ms |
| P95 | 565 ms |
| Pior ACK | 565 ms |
| ACKs acima do limite de 600 ms | 0 |
| Falhas | 0 |
| `ack.pass` | `true` |
| Score | 100 |

Assim, o limite de 600 ms foi confirmado pelo relógio oficial para este burst: o pior ACK ficou 35 ms abaixo do SLA.

## Métrica que o retorno não fornece

O corpo da resposta não contém média aritmética nem tempos individuais. P50 é a mediana e não deve ser rotulada como média. Por isso, a média oficial exata não pode ser calculada com os dados recebidos e permanece pendente. O JSON inclui ainda `attempt`, `idempotency`, `retry`, `concurrency`, `result` e `duration_ms`; os valores detalhados não são reproduzidos aqui além do necessário para registrar o SLA.

O contrato consultado não documenta uma rota de leitura de relatório. A resposta real do callback forneceu o resumo acima, mas não a média. Não foi enviado callback adicional para tentar obter outro formato.

## Verificação técnica da captura

A resposta do callback passou a ser lida como texto, limitada a 65.536 bytes e persistida depois que o callback foi marcado `SENT`. A captura é apenas evidência local. Se a gravação falhar após o callback 2xx, o estado enviado é mantido e o callback não é repetido. A suíte de integração verifica persistência do corpo capturado.

Validações executadas após a alteração: build Docker, typecheck, `docker compose config --quiet` e suíte de integração com Redis/BullMQ reais (5 arquivos, 10 testes aprovados). A suíte unitária não foi executada nesta etapa.

## Escopo de 20.000 SKUs

Os PDFs pedem uma explicação do que mudaria para 20.000 SKUs; não exigem teste de carga para esse volume. Nenhum teste de 20.000 SKUs foi executado e este relatório não declara capacidade validada nesse volume. A estimativa e os pontos de dimensionamento permanecem na seção correspondente do README.
