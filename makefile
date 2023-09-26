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

.PHONY: default all clean is_server is_peer is_test