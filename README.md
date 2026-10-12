# Solver-handler

Serviço que entrega **produtos** (resultado de um Solver, por exemplo um Token) por **chave**. A ideia é ter produtos já gerados num estoque externo para reduzir a espera de quem pede; quando não há estoque, o produto é resolvido na hora.

Roda na AWS em várias tasks atrás de um balanceador, então o estado compartilhado fica no Redis.

> Ainda em construção. As peças já existem, mas o `main()` está vazio, então nada sobe ao rodar.

## Conceitos

- **Chave (`Request.Key`)**: texto livre que identifica os solvers, definido por quem usa o sistema.
- **Labels (`Request.Labels`)**: atributos livres (`client`, `pool`, `region`...) que outras partes, como a proxy, podem usar.
- **Solver**: gera um produto para uma chave. Uma chave pode ter vários solvers, cada um com um peso (0 desativa).
- **Produto**: o que é entregue, ou seja, Id, chave, Solver que gerou, horário, tempo de geração, o valor e os `Params` usados para gerá-lo. Um produto do estoque traz a proxy com que foi gerado, e o cliente deve usar essa proxy.
- **Prazo (`Request.Timeout`)**: opcional, em segundos. Limita o prazo do solve (ver [Prazo e erros do `Solve`](#prazo-e-erros-do-solve)).

## Fluxo do `GetProduct(key)`

1. Valida a chave.
2. Incrementa o contador global da chave no Redis (`{chave}:requests`). Se falhar, o incremento é guardado e somado na próxima requisição.
3. Busca o produto no estoque externo (`SessionManager.Get`).
4. Se não conseguir, escolhe um Solver e resolve na hora.

### Escolha do Solver

Definida por `SolverConfig.adaptiveChoice`:

- **Peso fixo**: sorteio proporcional ao peso (os pesos não precisam somar 100).
- **Adaptativa**: usa as métricas de cada Solver (qualidade do produto, taxa de sucesso, velocidade e carga atual) multiplicadas pelo peso. Cada Solver fica entre 5% e 90% do tráfego. Sem métricas, segue os pesos.

### Prazo e erros do `Solve`

`Solve(ctx, params)` devolve o produto e um `Info` com o erro e o código HTTP da falha (0 quando não há código).

**Prazo:** o `ctx` do solve vence em `média + 4 × desvio` da latência de sucesso do solver, limitado entre 2s e 30s. Média e desvio são ponderados com o mesmo decaimento das outras métricas (meia-vida de 30 min). Sem amostras, o prazo é 30s. No `GetProduct`, o `Request.Timeout` do cliente reduz esse prazo, mas não o estende. No estoque, o prazo deriva do `ctx` do loop, então desligar o loop cancela os solves em andamento.

**Erros:** o código em `Info` decide quem leva a culpa (`external/httperrors.go`):

| Código | Culpa | Efeito |
|---|---|---|
| 407 | proxy | a proxy do estoque é descartada e reportada como ruim; não conta para o solver |
| 0, 429, 502, 503, 504 | nem do solver nem da proxy | não conta para o solver; a proxy é reportada como boa |
| outros | solver | conta como falha nas métricas locais e no Redis (saúde e disjuntor) |

**Report da proxy (estoque):** toda geração reporta a proxy usada, em segundo plano para não segurar o loop: falha só com erro de proxy, sucesso em qualquer outro caso.

### Taxa de requisições

`RequestRate(ctx, key)` lê o contador global e devolve requisições por segundo desde a medição anterior. A primeira chamada só registra a medição e devolve 0.

## Pacotes

| Pacote | O que faz |
|---|---|
| `.` (main) | `BucketHandler`, seleção de solvers, estatísticas, produto |
| `external` | Cliente HTTP/JSON para sistemas externos (`External`), `SessionManager` e lista de erros comuns |
| `tcpapi` | Servidor TCP genérico com confirmação de entrega |
| `redisconn` | Conexão com o Redis |

### `external`

- `BackOffice[T]`: `Get` e `Post` em JSON, com Token Bearer opcional. Todos compartilham um único cliente HTTP ajustado para muitas requisições em paralelo.
- `SessionManager[T]`: `Save(ctx, key, valor)` faz POST e `Get(ctx, key)` faz GET no path da chave.
- `Routes`: mapa de chave para path, carregado de um JSON. Assim dá para adicionar chaves sem precisar de deploy:

```json
{
  "alfa|cli": "/sessoes/alfa",
  "beta|cli": "/sessoes/beta"
}
```

### `tcpapi`

Uma conexão carrega várias requisições ao mesmo tempo, cada uma processada em paralelo. Uma mensagem JSON por vez:

```
cliente -> {"Id": 1, "data": <requisição>}
servidor -> {"Id": 1, "data": <resposta>}   ou   {"Id": 1, "error": "..."}
cliente -> {"Id": 1, "ack": true}            (só depois de "data")
```

Resposta sem `ack` dentro do `AckTimeout` (5s por padrão), ou cuja conexão caiu, vai para `Undelivered` para voltar ao estoque. A entrega é "pelo menos uma vez": se o `ack` se perder, o item pode ser entregue de novo.

## Rodando

Requer Go 1.27 e, para o contador global, um Redis acessível no endereço da variável `REDIS_ADDR`. Para subir um localmente:

```sh
cp .env.example .env
cp docker-compose.example.yml docker-compose.yml
docker compose up -d
set -a; . ./.env; set +a   # exporta REDIS_ADDR para o processo
```

Para compilar e testar:

```sh
go build ./...
go vet ./...
go test -race ./...
```

## Pendências conhecidas

- `main()` vazio: falta iniciar o `BucketHandler`, o `SessionManager` e o servidor `tcpapi`.
- **Melhoria — cliente que desiste:** o `GetProduct` não recebe `context`, então o prazo do cliente só vale pelo `Request.Timeout`; se a conexão cair, o solve segue até o fim e o produto é descartado. Passar o `ctx` da requisição e, se o cliente desistir, mandar o produto em andamento para o estoque.
- **Melhoria — classificar o resto dos erros do `Solve`:** hoje só se separa erro de proxy, erro do solver e erro que não é do solver (ver [Prazo e erros do `Solve`](#prazo-e-erros-do-solve)). Faltam: temporário (tenta outro solver), limite do destino (reduz o ritmo da chave inteira) e parâmetro inválido do cliente (não repete nem pune). Os solvers costumam ter códigos próprios, fora do padrão HTTP; avaliar se o mapa de `external/httperrors.go` basta ou se o `Info` deve trazer a classe do erro explícita.
- **A pensar:** erro sem código (`errCode` 0) nunca pune o solver. Um solver que falha sempre sem código não perde tráfego nem fica doente.
- **Melhoria — pontuação de proxy:** a proxy do estoque é trocada na primeira falha de proxy. Manter taxa de sucesso por proxy e trocar só quando cair, e lembrar quais proxies funcionam melhor com cada solver.
- **A pensar — segurança da proxy no estoque:** o produto guarda os `Params` com a URL inteira da proxy (`http://user:pass@host:porta`), em texto puro no estoque externo. Avaliar guardar só uma referência à proxy ou cifrar as credenciais.
- Qualquer falha do estoque externo cai em "resolver na hora"; ainda não se distingue "sem estoque" de "sistema fora".
- As métricas da escolha adaptativa (`SolverStats`) ainda são por task, não globais. Próxima etapa depois do estoque distribuído: levar para o Redis em `{chave}:solver:<nome>`.
- **A pensar:** `redisconn` usa `redis.Client` (nó único). Com ElastiCache em modo cluster, trocar por `redis.UniversalClient`/`ClusterClient`, que segue `MOVED`/`ASK`.
- **A pensar:** requisição duplicada. Se o solver passar do seu p95, dispara um segundo em paralelo e fica com o primeiro que terminar; o outro vai para o estoque. Avaliar se o ganho no p99 compensa o custo (cerca de 5% de gerações a mais) e o que fazer com o produto extra em chaves sem estoque.
- **A pensar:** nova tentativa no `GetProduct` dentro de um orçamento de tempo. Hoje tenta um solver só e devolve erro se falhar. A ideia é tentar o próximo solver da lista sem passar do orçamento total (melhor devolver erro do que demorar): em sequência com corte por tentativa, ou em paralelo com atraso (dispara o próximo quando o atual falha ou passa do p95 e fica com o primeiro que terminar). Antes, medir o desperdício: o produto que perde a corrida pode ir para o estoque, mas só é consumido se houver demanda; em chaves com pouca demanda ele vence no pool sem uso.
- **A pensar:** limite de concorrência fixo (solver comprado, API com limite em contrato).
  - A geração na hora passa do limite: `startNow` usa o primeiro solver mesmo com todos cheios, e também quando o Redis falha. Quando não há vaga, avaliar esperar dentro do orçamento de tempo ou devolver erro.
  - O limite é contado por chave (`busyKey(key, solver)`): uma mesma conta usada em várias chaves passa do contrato. Avaliar contar pelo recurso real, compartilhado entre as chaves.
  - Limite automático (cresce com a latência perto da baseline, cai à metade com latência alta ou falhas) só para solvers de capacidade variável; nos de contrato o limite fica fixo e, no máximo, desce quando a API degrada.
