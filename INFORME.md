# TP Nivelador

## Arquitectura 

El servidor escucha conexiones TCP hasta llegar al quórum configurado para poder iniciar el sorteo, por cada conexión con un cliente se crea un thread del tipo “Connection”, cada Connection recibe y maneja los mensajes de un único cliente, estos registros se almacenan en el archivo “bets.csv” (el mismo para todas las conexiones). 

El thread principal del servidor se encarga de recibir las conexiones e iniciar los threads “Connection” una vez que se inició un número de conexiones igual al quórum se espera a que todos los threads iniciados reciban todos los registros de sus clientes y una vez que se reciben, cada thread se registra en el diccionario “finished_transactions”, cuando todas las conexiones se registraron en el diccionario (al cual se accede mediante una threading.Condition) el thread principal inicia el sorteo, accede al archivo “bets.csv” y completa un diccionario con los ganadores de cada agencia, finalizada la lectura, se completa el campo “winners” de la clase Connection y se notifica al thread con su evento “winners_ready”, luego cada thread envía los ganadores al cliente asignado y finaliza la conexión y la ejecución  del thread.

Por otro lado el cliente inicia la conexión TCP con el servidor y envía el contenido del archivo input configurado como variable de entorno, una vez enviado el contenido de todo el archivo se espera a que el servidor envíe a los ganadores de los registros que envio y los almacena en el archivo output.csv.

## Protocolo

En la consigna debemos completar la implementación de safe_socket para el cliente y el servidor para evitar short-read/short-write para esto es necesario especificar la cantidad de bytes a leer del socket, teniendo esto en cuenta defini este protocolo para intercambiar mensajes.

Para el protocolo decidí que cada mensaje tenga un header fijo de 5 bytes, en el primer byte se especifica el tipo del mensaje y en los otros 4 bytes se especifica el largo, en bytes, de el payload. De esta manera el servidor siempre intenta leer primero una cantidad fija de 5 bytes y luego el largo que se especifica en este header.

Los tipos de mensaje son: Greeting(1), Bet(2), End(3), Winners(4), ACK(5).

Para iniciar la conexión el cliente envía un mensaje Greeting en el cual el servidor especifica cuál es su numero de agencia, luego el cliente acumula apuestas en batches y envió varias apuestas en un único mensaje del tipo Bet y no vuelve a enviar otro mensaje hasta recibir el mensaje de tipo ACK del servidor. Cuando el thread Connection recibe el mensaje de tipo End da por finalizada la transacción de información sobre apuestas de parte del cliente y notifica que ya finalizó esta fase con el diccionario “finished_transactions” al cual accede con la condición “condition”. Cuando se hace el sorteo y el thread principal termina de leer el contenido de “bets.csv” cada thread envía el mensaje del tipo Winners al cliente especificando en su payload cuales fueron los ganadores de su agencia.

## Algunos Comentarios de la solución:

Creí que era necesario que sea uno solo el archivo en el que se registran las apuestas de todas las agencias, por lo que fue necesario agregar la coordinación con los threads Connection para enviar los ganadores a cada cliente desde el thread principal del server. Una opción un poco más eficiente hubiera sido tener un archivo independiente específico para cada cliente, pero interprete que para el caso que plantea la consigna se debería mantener un registro único de todas las apuestas sin distinguir entre agencias (para mantener un registro centralizado para la organización y que el sorteo se haga una sola vez y de manera centralizada).
En consecuencia, en esta implementación cada thread del lado del servidor debe ser capaz de acceder al mismo archivo “bets.csv” y para no depender exclusivamente en los mecanismos de coordinación de acceso a archivos provistas por el SO agregue un lock “lottery_lock” que utilizo para coordinar el acceso al archivo entre los diferentes threads.
Además como comentario adicional del funcionamiento del servidor, decidí que el sorteo se haga cuando la cantidad de clientes conectados correctamente al servidor sea igual al quórum especificado. Si hay 7 clientes y el quórum es 3, los primeros 3 se procesan en el primer sorteo, los 3 siguientes en otro sorteo y el último queda esperando a que al menos otros 2 clientes se conecten para que el servidor ejecute el tercer sorteo.