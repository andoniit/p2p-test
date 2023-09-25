JC = javac
JR = java
JFLAGS  = -g -Wall 
 
default: all

all:
	$(JC) *.java

clean: 
	$(RM) *.class

is_server:
	$(JR) IndexingServer

is_peer:
	$(JR) Peer

is_test:
	$(JR) Test