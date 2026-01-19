# Remote File System in Rust

Un'implementazione di un client File System remoto basato su FUSE in Rust, con un backend RESTful in Node.js. Questo progetto permette di montare una directory remota localmente, offrendo un accesso trasparente ai file come se fossero presenti sul disco fisico.

## Descrizione Generale
Il sistema permette di interagire con un filesystem ospitato su un server remoto tramite le normali operazioni di sistema (es. `ls`, `cp`, `rm`, `cat`). Il client Rust traduce queste chiamate di sistema in richieste HTTP verso un server API, gestendo in modo efficiente la latenza tramite caching e lo streaming per i file di grandi dimensioni.

## Architettura
Il progetto adotta un'architettura **Client-Server** disaccoppiata:
- **Client (Rust):** Utilizza la libreria `fuser` per implementare il filesystem in userspace. Agisce come un bridge tra il kernel OS e le API REST.
- **Server (Node.js):** Un server stateless che espone il filesystem locale tramite endpoint REST.
- **Comunicazione:** Protocollo HTTP con payload JSON per i metadati e flussi binari (streaming) per il contenuto dei file.

---

## Requisiti Funzionali
In conformità con le specifiche del documento "Project_API_2025.pdf":

- **[Pienamente Conforme] Mount Locale:** Supporto al montaggio su un mount point locale definito dall'utente.
- **[Pienamente Conforme] Esplorazione:** Visualizzazione di file e directory tramite `ls` (mappato su `GET /list`).
- **[Pienamente Conforme] Operazioni CRUD:** - Lettura file (`GET /files`).
    - Scrittura e upload (`PUT /files`).
    - Creazione cartelle (`POST /mkdir`).
    - Eliminazione (`DELETE /files`).
- **[Pienamente Conforme] Attributi:** Gestione di dimensione, timestamp e permessi base recuperati dal server.
- **[Pienamente Conforme] Background Daemon:** Il client viene eseguito come processo continuo per servire le richieste FUSE.

## Requisiti Non Funzionali
- **Performance:** Minimizzazione dei round-trip di rete grazie alla cache locale dei metadati.
- **Robustezza:** Gestione degli errori di rete e timeout delle richieste HTTP.
- **Scalabilità:** Gestione di file grandi senza saturazione della memoria RAM locale.

---

## Supporto Multipiattaforma
Il client è progettato con un'astrazione modulare per garantire la massima compatibilità:
- **Linux:** Supporto nativo completo tramite `fuser` e `libfuse`.
- **macOS:** Supporto **best-effort** tramite macFUSE (richiede installazione manuale dei driver macFUSE).
- **Windows:** Supporto **best-effort** tramite WinFSP o Dokany (astrazione presente in `fuse_windows.rs`).

---

## Scelte Progettuali
- **Rust per la Sicurezza:** L'uso di Rust garantisce la gestione sicura della memoria e la prevenzione di data race in contesti multi-threaded (fondamentale per FUSE).
- **Integrazione FUSE:** Scelto per evitare lo sviluppo di driver a livello kernel, aumentando la stabilità del sistema.
- **Server Stateless:** Il server non mantiene sessioni, facilitando il ripristino in caso di crash e la scalabilità orizzontale.
- **I/O Asincrono:** Utilizzo di `tokio` e `reqwest` per gestire le chiamate di rete senza bloccare i thread di sistema.

---

## API REST del Server
Il server espone i seguenti endpoint:

| Metodo | Percorso | Funzione |
| :--- | :--- | :--- |
| `GET` | `/list/<path>` | Ritorna JSON con la lista dei file/cartelle nel percorso. |
| `GET` | `/files/<path>` | Download del contenuto del file (supporta streaming). |
| `PUT` | `/files/<path>` | Upload del contenuto (sovrascrittura o creazione). |
| `POST` | `/mkdir/<path>` | Crea una nuova directory. |
| `DELETE` | `/files/<path>` | Rimuove file o directory ricorsivamente. |
| `GET` | `/stats/<path>` | Recupera metadati (mtime, atime, size, mode). |

---

## Cache Locale
Il client implementa una strategia di caching per mitigare la latenza di rete:
- **Strategia:** **TTL (Time-To-Live)** per i metadati delle directory e degli attributi dei file.
- **Vantaggi:** Le operazioni frequenti come `ls` o il controllo dei permessi non richiedono chiamate di rete ogni volta.
- **Limiti:** Rischio di "stale data" (dati non aggiornati) se il server viene modificato da un altro client. Il TTL è configurabile per bilanciare consistenza e velocità.

---

## Supporto File Grandi e Streaming
Il sistema è ottimizzato per file superiori a **100MB**:
- **Streaming Read/Write:** Il client Rust utilizza gli stream di `reqwest` per trasmettere i dati direttamente tra il kernel e il server, mantenendo un footprint di memoria costante (non carica l'intero file in RAM).
- **Gestione Latenza:** I buffer di lettura sono ottimizzati per caricare chunk di dati in parallelo dove possibile.

---

## Installazione

### Pre-requisiti
- **Rust** (versione 1.75 o superiore)
- **Node.js** (v18+) e **npm**
- **FUSE:** `libfuse` (Linux), `macFUSE` (macOS) o `WinFSP` (Windows).
- 

### Setup Repository
```bash
git clone [https://github.com/emanuelepietrocometti/project-api-2025.git](https://github.com/emanuelepietrocometti/project-api-2025.git)
cd project-api-2025
```

# Comandi di esecuzione

## Backend (server)
Il server Node.js deve essere avviato prima del client per permettere la connessione iniziale.

```bash
cd backend
npm install
npm start
```
Per fermare il server in modo pulito:
```bash
npm stop
```
Esecuzione in modalità debug:
```bash
npm run dev    #Per avviare in modalità di debug
ctrl + C       #Per fermare l'esecuzione in modalità di debug
```

## Frontend (client)
Il client Rust gestisce automaticamente il mountpoint predefinito nel percorso `~/mnt/remote-fs`.
Avvio con IP e Background (Daemon): Il sistema supporta l'esecuzione in background tramite il flag `--deamon`:
```bash
cargo run --release -- <IP> deamon
```
Per fermare l'esecuzione:
```bash
cargo run --release -- stop
```
In alternativa, se si preferisce compilare prima di eseguire, è possibile utilizzare il seguente comando:
```bash
cargo build --release
```
Successivamente è possibile eseguire invocando il nome dell'eseguibile e passando le varie opzioni:
```bash
./target/release/frontend <IP> deamon   #Per avviare l'esecuzione
./target/release/frontedn stop          #Per stoppare l'esecuzione
```
Esecuzione in modalità di debug:
```bash
cargo build                         #Per compilare l'eseguibile
cargo run -- <IP> deamon            #Per eseguire il codice in modalità demone
cargo run                           #Per eseguire il codice in modalità sviluppo con i log visibile
cargo run -- stop                   #Per stoppare l'esecuzion se abbiamo avviato l'esecuzione in modalità demone
ctrl + C                            #Per stoppare l'esecuzione in modalità log visibili
```

## Dipendenze / Librerie

### Frontend (Rust)
Le dipendenze del client sono suddivise per gestire la logica asincrona, la comunicazione di rete e l'integrazione nativa con i diversi sistemi operativi.

**Core & Network:**
* **tokio**: Runtime asincrono (v1.48.0) fondamentale per gestire thread e I/O non bloccante.
* **reqwest**: Client HTTP (v0.12.24) utilizzato per le chiamate RESTful verso il backend.
* **rust_socketio**: Client per la comunicazione bidirezionale in tempo reale.
* **serde / serde_json**: Framework per la serializzazione e deserializzazione dei dati JSON.

**Filesystem & OS (Unix):**
* **fuser**: Implementazione del protocollo FUSE per Linux e macOS.
   - **Linux**: fuser (v0.16.0)
   - **macOS**: fuser (v0.15.1)
* **daemonize**: Gestione del processo come demone in background su sistemi Unix.

**Supporto Windows:**
* **winfsp**: Binding di alto livello per WinFSP (Windows File System Proxy) per emulare il comportamento FUSE su Windows.
* **winfsp-sys**: Binding di basso livello (FFI) per l'interfacciamento diretto con la libreria nativa WinFSP.
* **windows-service**: Utilizzato per registrare e gestire il client come servizio di sistema Windows.
* **winapi**: Accesso alle API Win32 legacy (con feature `wincon` per la gestione della console).
* **windows-sys**: Binding moderni e performanti per le API di sistema Windows (Foundation e Security).

### Backend (Node.js)
* **express**: Framework per la creazione delle API REST.
* **sqlite3**: Database per la persistenza dei metadati.
* **socket.io**: Gestione degli eventi in tempo reale.
* **morgan**: Logger per il monitoraggio delle richieste HTTP.
