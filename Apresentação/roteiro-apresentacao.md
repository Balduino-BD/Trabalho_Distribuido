# Sistemas Distribuídos de Tempo Real

Roteiro dos 10 slides. Os tempos indicados somam 22 minutos e incluem a leitura dos diagramas, as pausas e as perguntas à audiência.

## Slide 1 — Sistemas Distribuídos de Tempo Real

**Tempo previsto** · 1 min 30 s

### Explicação resumida

Um sistema distribuído de tempo real precisa produzir a resposta correta dentro do prazo, mesmo quando a medição, o processamento e a atuação acontecem em componentes diferentes. O diagrama da capa resume o percurso de um sinal entre sensor, rede e atuador.

### Fala sugerida

> Imaginem uma máquina industrial em que um sensor detecta que um eixo saiu da posição prevista. Um controlador interpreta a leitura e envia pela rede um comando de correção. Agora imaginem que o comando só chegue depois que o eixo já passou pelo ponto em que deveria corrigir a trajetória. O cálculo estava certo, mas a resposta chegou tarde para aquele ciclo.
>
> A pergunta que vai nos acompanhar é esta: como saber, antes de colocar a máquina em operação, se o atuador vai responder dentro do prazo? Vamos seguir esse comando desde a medição até a atuação e ver onde a CPU e a rede entram nessa garantia.

## Slide 2 — Resultado correto, no prazo

**Tempo previsto** · 2 min 30 s

### Explicação resumida

A corretude depende do resultado e do instante em que ele é entregue. Em um requisito *hard*, perder o prazo viola uma exigência obrigatória. Em um requisito *firm*, o resultado atrasado perde a utilidade e pode ser descartado. Em um requisito *soft*, a entrega tardia ainda tem valor, mas pode reduzir a qualidade do serviço.

### Fala sugerida

> Uma resposta que chega em dois milissegundos já é tardia se o prazo era de um milissegundo. Para entender o que esse atraso significa, olhem a linha tracejada nos três gráficos. Ela marca o prazo.
>
> À esquerda, em *hard real-time*, ultrapassar a linha viola o requisito. Pensem no limite de tempo para acionar uma proteção. No meio, em *firm real-time*, a utilidade cai a zero. Uma amostra de posição pode ser descartada quando o ciclo seguinte já começou. À direita, em *soft real-time*, a utilidade diminui aos poucos. Uma atualização de vídeo atrasada ainda pode ser aproveitada, embora a reprodução perca qualidade.
>
> Voltando à máquina, o que o controlador deveria fazer com uma medição que chegou depois do próximo ciclo? A resposta depende da função que usa aquela medição.

## Slide 3 — Prazo ponta a ponta

**Tempo previsto** · 3 min 30 s

### Explicação resumida

O prazo ponta a ponta cobre o percurso completo da medição até a resposta física. Cada etapa consome parte do orçamento de latência. O WCET é o tempo de execução no pior caso sob as hipóteses da análise. O tempo de resposta também inclui esperas. *Jitter* é a variação temporal, como a diferença entre os intervalos de chegada de mensagens sucessivas.

### Fala sugerida

> Vamos ler a figura da esquerda para a direita. O sensor coleta uma amostra. A CPU A processa a leitura, a rede carrega o comando, a CPU B trata a mensagem recebida e o atuador responde. Em cada CPU, a tarefa pode esperar para executar. Na rede, o comando pode esperar acesso ao enlace.
>
> Vamos supor que a resposta física precise ocorrer em dez milissegundos. Para ilustrar, reservamos um para amostragem, dois para a CPU A, três para a rede, dois para a CPU B e dois para o atuador. Se a rede passar de três para quatro milissegundos e as outras etapas consumirem todo o tempo reservado, chegamos a onze. O prazo estoura mesmo que as duas CPUs cumpram seus limites.
>
> O WCET ajuda a estimar a parcela de execução. Para chegar ao tempo de resposta, também precisamos contabilizar a espera. O *jitter* aparece quando o intervalo entre eventos varia, por exemplo quando mensagens que deveriam chegar regularmente chegam com espaçamentos diferentes. Quando a rede usa janelas programadas em vários nós, a sincronização dos relógios também entra no projeto.

## Slide 4 — Escalonamento na CPU

**Tempo previsto** · 3 min 30 s

### Explicação resumida

O escalonador decide qual tarefa pronta ocupa a CPU. O RMS (*Rate Monotonic Scheduling*) dá prioridade fixa à tarefa de menor período. O EDF (*Earliest Deadline First*) escolhe a tarefa cujo prazo absoluto está mais próximo. As duas linhas do slide mostram a ordem inicial de execução para o mesmo par de tarefas.

### Fala sugerida

> Imaginem as tarefas A e B prontas no instante zero. A precisa de dois milissegundos de CPU, repete a cada cinco e tem prazo de cinco. B precisa de um milissegundo, repete a cada oito e tem prazo de três. Antes de olhar as barras, qual começa primeiro em cada política?
>
> Na primeira linha, o RMS escolhe A porque seu período é menor. A termina no instante dois e B termina no três, exatamente no prazo. Na segunda linha, o EDF começa com B porque o prazo dela é o mais próximo. B termina no instante um e A, no três. As duas políticas cumprem os prazos nesse primeiro trecho. Para avaliar a sequência completa, precisaríamos considerar também as próximas liberações.
>
> A fila de tarefas prontas explica uma parte do atraso. Se a tarefa escolhida precisar de um recurso que outra tarefa mantém ocupado, ela ainda poderá ficar bloqueada. Essa espera precisa caber no orçamento da CPU, porque qualquer atraso nessa etapa reduz a margem disponível para a rede.

Referência dos algoritmos clássicos. [Liu e Layland (1973)](https://web.cs.wpi.edu/~cs502/f07/Papers/Liu%20%26%20Layland%2C%20Real-time%20Scheduling.pdf)

## Slide 5 — Resposta a falhas dentro do prazo

**Tempo previsto** · 2 min 30 s

### Explicação resumida

Em uma função *fail-safe*, o sistema precisa alcançar um estado seguro após detectar uma falha. Em uma função *fail-operational*, ele precisa continuar prestando o serviço exigido sob as falhas previstas, por exemplo com redundância. A garantia temporal abrange a detecção, a decisão e a atuação.

### Fala sugerida

> Vamos voltar à máquina do começo. O que acontece se um componente parar de responder durante o movimento do eixo? Em uma função *fail-safe*, a ação prevista pode ser levar a máquina a uma parada controlada. O tempo que ela leva para chegar a esse estado precisa caber no limite definido para a situação.
>
> Se a função tiver de continuar disponível, entra o caso *fail-operational*. Uma segunda unidade pode assumir o controle, por exemplo. Nessa arquitetura, o orçamento inclui o tempo para perceber a falha, transferir o controle e retomar a resposta. Uma pausa muito longa durante a troca pode violar o requisito mesmo quando a unidade de reserva está funcionando.
>
> Para cada função, precisamos definir as falhas que estamos considerando, a resposta esperada e o prazo para executá-la.

## Slide 6 — Acesso ao enlace em tempo real

**Tempo previsto** · 1 min 30 s

### Explicação resumida

Quando vários quadros compartilham um enlace, o acesso ao meio acrescenta espera. No exemplo por evento, o CAN arbitra entre os quadros prontos quando o barramento fica livre. No exemplo por tempo, filas programadas em TSN 802.1Qbv ganham oportunidades de transmissão em janelas coordenadas por relógios sincronizados.

### Fala sugerida

> Um comando já está pronto na CPU. Quando ele consegue entrar na rede? No lado esquerdo, a disputa acontece quando há quadros prontos e o barramento fica livre. No CAN, a arbitragem seleciona o quadro de maior prioridade entre os concorrentes.
>
> No lado direito, o acesso das filas programadas segue um calendário. No TSN, os switches usam relógios sincronizados para abrir os portões dessas filas em horários planejados. A diferença nos leva aos dois estudos. Primeiro, vamos ver como a arbitragem e a ocupação do barramento podem atrasar uma mensagem no CAN. Depois, como se planejam as janelas ao longo de uma rota no TSN.

## Slide 7 — CAN e o pior tempo de resposta

**Tempo previsto** · 2 min

### Explicação resumida

No CAN, uma transmissão em andamento segue até o fim. O exemplo de Davis et al. mostra que a interferência de mensagens de maior prioridade pode recair sobre uma instância posterior. Na figura do slide, a primeira instância da mensagem C responde em **3,0 ms** e a segunda em **3,5 ms**, para um prazo relativo de **3,25 ms**. A análise revisada considera as instâncias relevantes de um período ocupado para identificar o pior caso.

### Fala sugerida

> Estas duas barras vêm de um exemplo de Davis e colegas, publicado em 2007. Cada uma mede o tempo desde a liberação da sua instância da mensagem C até o fim da transmissão. A linha tracejada marca o mesmo prazo para as duas, de 3,25 milissegundos.
>
> A primeira instância responde em três milissegundos. Enquanto ela transmite, quadros de maior prioridade chegam e aguardam. Quando o barramento fica livre, esses quadros podem ganhar a arbitragem antes da próxima instância de C. No exemplo, a segunda instância leva 3,5 milissegundos e passa da linha tracejada.
>
> Se a análise parar na primeira barra, o prazo parece garantido. O artigo identifica essa falha e revisa a análise para examinar outras instâncias que ainda podem sofrer interferência naquele período ocupado.

Referência do contraexemplo. [Davis et al. (2007), figura 3](https://doi.org/10.1007/s11241-007-9012-7)

## Slide 8 — TSN e as janelas de transmissão

**Tempo previsto** · 2 min

### Explicação resumida

No IEEE 802.1Qbv, portões programados definem quando cada fila de saída fica elegível para transmitir. Ao longo de uma rota, as janelas precisam levar em conta o tempo para o quadro chegar ao próximo switch. Craciunas et al. formulam restrições para coordenar essas janelas e tratar interferências causadas pela ordem de chegada em filas compartilhadas.

### Fala sugerida

> Agora acompanhem a mesma escala de tempo nas duas faixas. Cada faixa representa uma fila de saída, uma no switch A e outra no switch B. O azul marca os períodos em que a fila pode transmitir. Quando o portão fecha, os quadros ainda na fila aguardam outra abertura.
>
> Imaginem um quadro que sai pela primeira janela do switch A. Ele precisa chegar ao switch B a tempo de aproveitar a janela prevista lá. Se chegar depois que ela fecha, pode ter de esperar a próxima. Em uma fila compartilhada, a ordem de chegada dos quadros também influencia quem sai primeiro.
>
> No artigo de 2016, Craciunas e colegas calculam antecipadamente as janelas dos switches ao longo da rota. O estudo também propõe restrições de isolamento para controlar a interferência entre quadros nas filas. A pergunta prática é se o quadro consegue atravessar todos os saltos e chegar dentro do prazo.

Referência do escalonamento de janelas. [Craciunas et al. (2016), seções 4–5](https://doi.org/10.1145/2997465.2997470)

## Slide 9 — Aplicações de tempo real

**Tempo previsto** · 1 min 30 s

### Explicação resumida

Os três fluxos mostram como uma entrada leva a uma ação que precisa ocorrer no prazo. Na aviação, o *fly-by-wire* transforma o comando do piloto em atuação nas superfícies de controle. No automóvel, o ABS ajusta a pressão de frenagem quando detecta tendência de travamento. Na indústria, o controle de movimento usa a posição medida para corrigir a trajetória do eixo.

### Fala sugerida

> Leiam cada linha da esquerda para a direita. Na aviação, o computador de voo interpreta o comando do piloto e envia sinais aos atuadores das superfícies de controle. No automóvel, o ABS observa a velocidade das rodas e ajusta a pressão do freio quando identifica tendência de travamento. Na indústria, voltamos ao nosso exemplo inicial: o controlador mede a posição do eixo e comanda uma correção.
>
> Nos três casos, podemos perguntar quanto tempo pode passar entre receber a entrada e produzir a ação. Esse limite depende da função e da dinâmica do equipamento. Quando o caminho passa por uma rede, o tempo de comunicação faz parte do mesmo orçamento.

Fontes dos exemplos. [Airbus, controle de voo *fly-by-wire*](https://www.airbus.com/en/newsroom/stories/2023-02-safety-innovation-7-flight-envelope-protection), [Bosch Mobility, ABS](https://www.bosch-mobility.com/en/solutions/braking-systems/antilock-braking-system/), [Siemens, controle de movimento](https://cache.industry.siemens.com/dl/files/969/109803969/att_1322315/v1/SIMATIC_AllAboutMotionControl_V1.2_2025-03_en.pdf)

## Slide 10 — A garantia é ponta a ponta

**Tempo previsto** · 1 min 30 s

### Explicação resumida

A verificação começa com um prazo bem definido. Em seguida, é preciso justificar os limites de execução, bloqueio e comunicação e verificar se a cadeia completa cabe no prazo. O exemplo CAN mostra por que testar uma única instância pode ser insuficiente. O exemplo TSN mostra por que as janelas precisam ser coordenadas ao longo da rota.

### Fala sugerida

> Como sabemos, então, se o atuador da abertura vai responder dentro do prazo? Primeiro, definimos quando começa a contagem e qual evento encerra o prazo: o início da atuação ou o fim do movimento, conforme a função. Depois, verificamos quanto a CPU pode gastar com execução e espera e quanto a rede pode acrescentar.
>
> No CAN, vimos que a próxima instância pode sofrer um atraso maior. No TSN, vimos que uma janela perdida em um switch afeta o restante da rota. Em ambos os casos, o resultado da análise de rede precisa entrar na verificação da cadeia inteira. É assim que justificamos, antes de operar o sistema, que o comando correto chega a tempo de produzir a resposta esperada.
