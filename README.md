# Solver-handler

Serviço que entrega **produtos** (resultado de um Solver, por exemplo um Token) por **chave**. A ideia é ter produtos já gerados num estoque externo para reduzir a espera de quem pede; quando não há estoque, o produto é resolvido na hora.

Roda na AWS em várias tasks atrás de um balanceador, então o estado compartilhado fica no Redis.

> Ainda em construção. As peças já existem, mas o `main()` está vazio, então nada sobe ao rodar.

## Conceitos

- **Chave (`keys.Key`)**: `{Name, Client}`, escrita como `nome|cliente`. `Name` é livre, definido por quem usa o sistema.
- **Solver**: gera um produto para uma chave. Uma chave pode ter vários solvers, cada um com um peso (0 desativa).
- **Produto**: o que é entregue, ou seja, Id, chave, Solver que gerou, horário, tempo de geração e o valor.

## Fluxo do `GetProduct(key)`

1. Valida a chave.
2. Incrementa o contador global da chave no Redis (`requests:nome|cliente`). Se falhar, o incremento é guardado e somado na próxima requisição.
3. Busca o produto no estoque externo (`SessionManager.Get`).
4. Se não conseguir, escolhe um Solver e resolve na hora.

### Escolha do Solver

Definida por `SolverConfig.adaptiveChoice`:

- **Peso fixo**: sorteio proporcional ao peso (os pesos não precisam somar 100).
- **Adaptativa**: usa as métricas de cada Solver (qualidade do produto, taxa de sucesso, velocidade e carga atual) multiplicadas pelo peso. Cada Solver fica entre 5% e 90% do tráfego. Sem métricas, segue os pesos.

### Taxa de requisições

`RequestRate(ctx, key)` lê o contador global e devolve requisições por segundo desde a medição anterior. A primeira chamada só registra a medição e devolve 0.

## Pacotes

| Pacote | O que faz |
|---|---|
| `.` (main) | `BucketHandler`, seleção de solvers, estatísticas, produto |
| `keys` | Tipo `Key` |
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
- Os campos de `Product` não são exportados, então ele ainda não pode ser lido do JSON do estoque externo.
- Qualquer falha do estoque externo cai em "resolver na hora"; ainda não se distingue "sem estoque" de "sistema fora".
- As rotas são fixas após criar o `SessionManager`; trocar em produção precisa de substituição atômica.
- As métricas da escolha adaptativa (`SolverStats`) ainda são por task, não globais. Próxima etapa depois do estoque distribuído: levar para o Redis em `{name|client}:solver:<nome>`.
- Limite de concorrência por solver: nada chama `SetLimit` ainda. Falta aplicar `Config.SolverLimit` do backoffice em cada solver ao carregar a config; até lá, todos ficam sem teto (limite 0).
- **A pensar:** distribuir a geração do estoque entre os pods. Hoje o primeiro pod a ticar em cada segundo reserva toda a falta e gera tudo sozinho.
- **A pensar:** `redisconn` usa `redis.Client` (nó único). Com ElastiCache em modo cluster, trocar por `redis.UniversalClient`/`ClusterClient`, que segue `MOVED`/`ASK`.
- **Estoque x dados do cliente (considerar no refill):** quando o solve depende do que o cliente envia em `Params` (por exemplo, a proxy), um produto gerado antes, para o estoque, não usou esses dados. Saídas possíveis: essas chaves não usam estoque e sempre resolvem na hora, ou a proxy (ou o pool dela) entra na chave, separando o estoque por proxy.
