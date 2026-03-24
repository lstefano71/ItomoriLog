issues:
1) I don't recognise the workflow. I was expecting a welcome screen where I could select older sessions or simply drag and drop files and start a new session with default title and parameters. The title of a session should be editable when the session is already created and even after the first batch of files ingested
2) once a file or directory is dropped on the GUI, or once a sessions is started manually, I should still be able to add more files and directories (directories by name not just by content!) with drag and drop or with controls on the view. I should also be able to remove files or directories added by accident
3) once files and directory are accumulating, they should be sniffed to begin the selection of automatic rules for subsequent ingestion. As they are, the various guesses should be shown with a quality marker, and the user should still be free to modify the guesses. I think this is called "wizard" now, but I couldn't get it to start. 
3.1) all the guessing should be done in the background to keep the gui responsive
4) once list and rules are agreed on, the user can start the ingestion. The GUI should show the ingestion process with plenty of details. for instance: inline progress bars per file based on bytes consumed/total and so on.
5) the speed of ingestion is horrible at the moment: 15k records in more than 2 minutes for a very simple file.  there must be some serious misunderstanding because once the initial sniffing is done (and that should happen BEFORE the ingestion start) the ingestion could really proceed at breakneck speed.
6) it would be great if the browser GUI could be active since the first batch is ingested, while the panel with the progress is still reacheable. in theory one may want to add more files/directories to the list and those eshould also go through the sniffing, guessing, with user confirmation if the guess is with low confidence. all highly concurrent
7) even after the ingestion, the guesses can be overridden and the relevant files reingested according to the workflow documented in the design documents
8) the docs mention the possibility of driving the program via a CLI. was that implemented? what else was not implemented from the doc "session per db"?
9) was the command palette implemented? if it was: is the command palette keyboard enabled, search-as-you-type enabled?

Make sure to update the documentation as described in the main prescriptions when the code is changed.

P.S.: a few lines from the 15k lines log file that took minutes to be ingested:
==============================
2022-05-12 16:25:14Z SRV Current timezone: W. Europe Daylight Time (Bias from UTC: 120 minutes)
2022-05-12 16:25:14Z SRV Read configuration file: Z:\ConfSRV\serverCS.txt
2022-05-12 16:25:14Z SRV trap_srv_errors: 2
2022-05-12 16:25:14Z SRV [MASTER] Connecting to 127.0.0.1:17531 ...
2022-05-12 16:25:14Z SRV [MASTER] Connected
2022-05-12 16:25:14Z SRV [MASTER] Ready
2022-05-12 16:25:14Z SRV Starting Server: 1, ANFS
2022-05-12 16:25:14Z SRV Process ID: 14216
2022-05-12 16:25:14Z SRV Starting service: CMP
2022-05-12 16:25:14Z SRV Initializing service: CMP in thread: 23
2022-05-12 16:25:14Z CMPS Logging level: 1
2022-05-12 16:25:14Z CMPS Listener for: CMP created on port: 29195, rc: 0  SCMP 
2022-05-12 16:25:14Z SRV Status: CMP Started Successfully
2022-05-12 16:25:14Z SRV Starting service: ANFS
2022-05-12 16:25:14Z SRV Initializing service: ANFS in thread: 23
2022-05-12 16:25:14Z ASV Logging level: 1
2022-05-12 16:25:14Z ASV Opening: Z:\sofiawd\SOFDTANG.FLS
==============================

