Ambiente: Java 21.0.13 | Linux aarch64 | CPUs visíveis=10 | heap máx=3072 MB  
Volume: 1,000,000 mensagens | 5 repetições medidas + 2 de aquecimento por configuração

| Threads | Tempo médio (ms) | Desvio (ms) | Mínimo (ms) | Speedup | Eficiência | Resultado = sequencial |
|---:|---:|---:|---:|---:|---:|:---:|
| 1 | 6512.1 | 154.3 | 6404.8 | 1.00x | 100.0% | sim |
| 2 | 3804.7 | 126.1 | 3708.1 | 1.71x | 85.6% | sim |
| 4 | 2575.0 | 99.8 | 2482.3 | 2.53x | 63.2% | sim |
| 6 | 2440.4 | 313.0 | 2173.9 | 2.67x | 44.5% | sim |
| 8 | 2019.9 | 100.4 | 1906.7 | 3.22x | 40.3% | sim |
| 10 | 2214.6 | 185.9 | 1972.0 | 2.94x | 29.4% | sim |
